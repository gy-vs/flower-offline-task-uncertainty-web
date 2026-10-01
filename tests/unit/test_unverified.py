import json
import os
import tempfile
import time
import unittest
from unittest.mock import Mock
from urllib.parse import urlencode

from celery.events import Event
from tornado.testing import AsyncTestCase

from flower.events import Events, EventsState
from tests.unit import AsyncHTTPTestCase


def feed(state, *events):
    for i, event in enumerate(events):
        event.setdefault('clock', i)
        event.setdefault('local_received', time.time())
        state.event(event)


def started_task(state, uuid, worker, name='billing.charge'):
    feed(state,
         Event('task-received', uuid=uuid, name=name, args='(1,)',
               kwargs='{}', retries=0, eta=None, hostname=worker),
         Event('task-started', uuid=uuid, hostname=worker))


class UnverifiedTasksTests(unittest.TestCase):
    def setUp(self):
        self.state = EventsState()
        feed(self.state, Event('worker-online', hostname='worker1'),
             Event('worker-online', hostname='worker2'))

    def test_started_task_is_flagged_when_its_worker_goes_offline(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('worker-offline', hostname='worker1'))

        self.assertTrue(self.state.is_unverified('charge-1'))
        self.assertEqual('worker1', self.state.unverified_tasks['charge-1'])
        # the celery-reported state is left untouched
        self.assertEqual('STARTED', self.state.tasks['charge-1'].state)

    def test_only_started_tasks_of_the_offline_worker_are_flagged(self):
        started_task(self.state, 'charge-1', 'worker1')
        started_task(self.state, 'charge-2', 'worker2')
        feed(self.state,
             Event('task-received', uuid='charge-3', name='billing.charge',
                   args='(1,)', kwargs='{}', retries=0, eta=None,
                   hostname='worker1'))
        feed(self.state, Event('worker-offline', hostname='worker1'))

        self.assertTrue(self.state.is_unverified('charge-1'))
        self.assertFalse(self.state.is_unverified('charge-2'))
        # received but never started: nothing to verify
        self.assertFalse(self.state.is_unverified('charge-3'))

    def test_finished_failed_and_revoked_tasks_are_not_flagged(self):
        for uuid, event_type in (('charge-1', 'task-succeeded'),
                                 ('charge-2', 'task-failed'),
                                 ('charge-3', 'task-revoked')):
            started_task(self.state, uuid, 'worker1')
            feed(self.state, Event(event_type, uuid=uuid, hostname='worker1'))
        feed(self.state, Event('worker-offline', hostname='worker1'))

        self.assertEqual({}, self.state.unverified_tasks)

    def test_worker_coming_back_online_does_not_clear_the_flag(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('worker-offline', hostname='worker1'))
        feed(self.state, Event('worker-online', hostname='worker1'),
             Event('worker-heartbeat', hostname='worker1', active=1))

        self.assertTrue(self.state.is_unverified('charge-1'))

    def test_outcome_events_clear_the_flag(self):
        for event_type in ('task-succeeded', 'task-failed', 'task-retried',
                           'task-revoked', 'task-rejected'):
            with self.subTest(event_type=event_type):
                state = EventsState()
                feed(state, Event('worker-online', hostname='worker1'))
                started_task(state, 'charge-1', 'worker1')
                feed(state, Event('worker-offline', hostname='worker1'))
                self.assertTrue(state.is_unverified('charge-1'))

                feed(state, Event(event_type, uuid='charge-1',
                                  hostname='worker1'))
                self.assertFalse(state.is_unverified('charge-1'))

    def test_task_started_on_another_worker_clears_the_flag(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('worker-offline', hostname='worker1'))
        self.assertTrue(self.state.is_unverified('charge-1'))

        feed(self.state, Event('task-started', uuid='charge-1',
                               hostname='worker2'))

        self.assertFalse(self.state.is_unverified('charge-1'))
        self.assertEqual('STARTED', self.state.tasks['charge-1'].state)
        self.assertEqual('worker2',
                         self.state.tasks['charge-1'].worker.hostname)

    def test_task_received_alone_does_not_clear_the_flag(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('worker-offline', hostname='worker1'))

        feed(self.state,
             Event('task-received', uuid='charge-1', name='billing.charge',
                   args='(1,)', kwargs='{}', retries=1, eta=None,
                   hostname='worker2'))
        self.assertTrue(self.state.is_unverified('charge-1'))

        feed(self.state, Event('task-started', uuid='charge-1',
                               hostname='worker2'))
        self.assertFalse(self.state.is_unverified('charge-1'))

    def test_late_terminal_event_arriving_after_offline_clears_the_flag(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('worker-offline', hostname='worker1'))
        self.assertTrue(self.state.is_unverified('charge-1'))

        # the task actually succeeded before the worker went offline,
        # the event just arrived late
        feed(self.state, Event('task-succeeded', uuid='charge-1',
                               result='ok', hostname='worker1'))

        self.assertFalse(self.state.is_unverified('charge-1'))
        self.assertEqual('SUCCESS', self.state.tasks['charge-1'].state)

    def test_late_offline_event_does_not_flag_a_finished_task(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('task-succeeded', uuid='charge-1',
                               result='ok', hostname='worker1'))

        # the worker-offline event was emitted earlier but arrives now
        feed(self.state, Event('worker-offline', hostname='worker1'))

        self.assertFalse(self.state.is_unverified('charge-1'))
        self.assertEqual('SUCCESS', self.state.tasks['charge-1'].state)

    def test_flagged_task_can_be_flagged_again_after_restarting(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('worker-offline', hostname='worker1'))
        feed(self.state, Event('task-started', uuid='charge-1',
                               hostname='worker2'))
        self.assertFalse(self.state.is_unverified('charge-1'))

        feed(self.state, Event('worker-offline', hostname='worker2'))
        self.assertTrue(self.state.is_unverified('charge-1'))
        self.assertEqual('worker2', self.state.unverified_tasks['charge-1'])

    def test_eviction_drops_the_flag(self):
        state = EventsState(max_tasks_in_memory=2)
        feed(state, Event('worker-online', hostname='worker1'))
        started_task(state, 'charge-1', 'worker1')
        started_task(state, 'charge-2', 'worker1')
        feed(state, Event('worker-offline', hostname='worker1'))
        self.assertEqual({'charge-1', 'charge-2'}, set(state.unverified_tasks))

        started_task(state, 'charge-3', 'worker1')

        self.assertNotIn('charge-1', state.tasks)
        self.assertFalse(state.is_unverified('charge-1'))
        self.assertTrue(state.is_unverified('charge-2'))

    def test_clear_tasks_prunes_flags(self):
        started_task(self.state, 'charge-1', 'worker1')
        feed(self.state, Event('worker-offline', hostname='worker1'))

        # started tasks survive a ready-only cleanup, flags included
        self.state.clear_tasks(ready=True)
        self.assertTrue(self.state.is_unverified('charge-1'))

        self.state.clear_tasks(ready=False)
        self.assertEqual({}, self.state.unverified_tasks)


class UnverifiedPersistenceTests(AsyncTestCase):
    def events(self, db, max_tasks_in_memory=10):
        return Events(Mock(), self.io_loop, db=db, persistent=True,
                      enable_events=False,
                      max_tasks_in_memory=max_tasks_in_memory)

    def test_flags_survive_save_and_restore(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = os.path.join(tmpdir, 'flower')
            events = self.events(db)
            feed(events.state, Event('worker-online', hostname='worker1'))
            started_task(events.state, 'charge-1', 'worker1')
            started_task(events.state, 'charge-2', 'worker1')
            feed(events.state, Event('task-succeeded', uuid='charge-2',
                                     result='ok', hostname='worker1'))
            feed(events.state, Event('worker-offline', hostname='worker1'))
            events.save_state()

            restored = self.events(db)

            self.assertTrue(restored.state.is_unverified('charge-1'))
            self.assertEqual(
                'worker1', restored.state.unverified_tasks['charge-1'])
            self.assertFalse(restored.state.is_unverified('charge-2'))
            self.assertEqual('STARTED', restored.state.tasks['charge-1'].state)

            # and the restored state keeps tracking new events
            feed(restored.state, Event('task-succeeded', uuid='charge-1',
                                       result='ok', hostname='worker1'))
            self.assertFalse(restored.state.is_unverified('charge-1'))

    def test_restore_with_smaller_limit_prunes_flags_of_evicted_tasks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = os.path.join(tmpdir, 'flower')
            events = self.events(db, max_tasks_in_memory=4)
            feed(events.state, Event('worker-online', hostname='worker1'))
            for i in range(4):
                started_task(events.state, f'charge-{i}', 'worker1')
            feed(events.state, Event('worker-offline', hostname='worker1'))
            self.assertEqual(4, len(events.state.unverified_tasks))
            events.save_state()

            restored = self.events(db, max_tasks_in_memory=2)

            self.assertEqual(2, len(restored.state.tasks))
            self.assertEqual(
                set(restored.state.tasks), set(restored.state.unverified_tasks))

    def test_state_saved_by_an_old_version_loads_without_flags(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = os.path.join(tmpdir, 'flower')
            events = self.events(db)
            started_task(events.state, 'charge-1', 'worker1')

            # simulate a pre-feature save whose pickle only carries the
            # plain celery State reduce, without any unverified marks
            from celery.events.state import State

            class OldFormatState:
                def __reduce__(self):
                    return State.__reduce__(events.state)

            import shelve
            with shelve.open(db, flag='n') as shelf:
                shelf['events'] = OldFormatState()

            restored = self.events(db)

            self.assertEqual({}, restored.state.unverified_tasks)
            self.assertFalse(restored.state.is_unverified('charge-1'))
            self.assertIn('charge-1', restored.state.tasks)


class BillingScenarioTest(AsyncHTTPTestCase):
    # The full verification plan: which events raise and lift the mark

    def setUp(self):
        super().setUp()
        self.state = EventsState()
        self.clock = 0

    def feed(self, *events):
        for event in events:
            event['clock'] = self.clock
            event['local_received'] = time.time()
            self.clock += 1
            self.state.event(event)
        self._app.events.state = self.state

    def datatable(self, **extra):
        params = {'draw': 1, 'start': 0, 'length': 15, 'search[value]': '',
                  'order[0][column]': 0, 'columns[0][data]': 'name',
                  'order[0][dir]': 'asc'}
        params.update(extra)
        r = self.get('/tasks/datatable?' + urlencode(params))
        self.assertEqual(200, r.code)
        return json.loads(r.body.decode('utf-8'))

    def rows(self, **extra):
        return {row['uuid']: row for row in self.datatable(**extra)['data']}

    def test_charge_scenario(self):
        self.feed(
            Event('worker-online', hostname='worker1'),
            Event('task-received', uuid='charge-1', name='billing.charge',
                  args='(1,)', kwargs='{}', retries=0, eta=None,
                  hostname='worker1'),
            Event('task-started', uuid='charge-1', hostname='worker1'),
            Event('task-received', uuid='charge-2', name='billing.charge',
                  args='(2,)', kwargs='{}', retries=0, eta=None,
                  hostname='worker1'),
            Event('task-started', uuid='charge-2', hostname='worker1'),
            Event('task-succeeded', uuid='charge-2', result='ok', runtime=1.0,
                  hostname='worker1'),
        )
        self.feed(Event('worker-offline', hostname='worker1'))

        rows = self.rows()
        self.assertEqual('STARTED', rows['charge-1']['state'])
        self.assertTrue(rows['charge-1']['unverified'])
        self.assertEqual('SUCCESS', rows['charge-2']['state'])
        self.assertFalse(rows['charge-2']['unverified'])

        # state:STARTED still works on the celery-reported state, and the
        # unverified filter finds the same row server-side
        self.assertEqual(['charge-1'], list(self.rows(
            **{'search[value]': 'state:STARTED'})))
        self.assertEqual(['charge-1'], list(self.rows(unverified='true')))

        body = self.get('/task/charge-1').body.decode('utf-8')
        self.assertIn('task-state-unverified', body)
        self.assertNotIn('task-terminate', body)

        # worker1 coming back online proves nothing: the hint stays
        self.feed(Event('worker-online', hostname='worker1'))
        self.assertTrue(self.rows()['charge-1']['unverified'])
        self.assertNotIn('task-terminate',
                         self.get('/task/charge-1').body.decode('utf-8'))

        # the actual completion event clears it
        self.feed(Event('task-succeeded', uuid='charge-1', result='ok',
                        runtime=1.0, hostname='worker1'))
        rows = self.rows()
        self.assertEqual('SUCCESS', rows['charge-1']['state'])
        self.assertFalse(rows['charge-1']['unverified'])
        self.assertEqual([], self.datatable(unverified='true')['data'])

        # a task restarted on another worker clears it at the new start
        self.feed(
            Event('task-received', uuid='charge-3', name='billing.charge',
                  args='(3,)', kwargs='{}', retries=0, eta=None,
                  hostname='worker1'),
            Event('task-started', uuid='charge-3', hostname='worker1'),
            Event('worker-offline', hostname='worker1'),
        )
        self.assertEqual(['charge-3'], list(self.rows(unverified='true')))
        self.feed(Event('worker-online', hostname='worker2'),
                  Event('task-started', uuid='charge-3', hostname='worker2'))
        rows = self.rows()
        self.assertEqual('STARTED', rows['charge-3']['state'])
        self.assertFalse(rows['charge-3']['unverified'])
        self.assertEqual('worker2', rows['charge-3']['worker'])
        self.assertIn('task-terminate',
                      self.get('/task/charge-3').body.decode('utf-8'))
