import pickle
import time
import unittest

from celery import states
from celery.events import Event

from flower.events import EventsState
from tests.unit.utils import (
    task_failed_events,
    task_started_events,
    task_succeeded_events,
)


def _send(state, event, clock):
    event['clock'] = clock
    event['local_received'] = time.time()
    state.event(event)
    return clock + 1


def _event(event_type, hostname, clock, **kwargs):
    event = Event(event_type, hostname=hostname, **kwargs)
    event['clock'] = clock
    event['local_received'] = time.time()
    return event


def _received_event(task_id, worker):
    return Event('task-received', uuid=task_id, name='t', args=(), kwargs={},
                 retries=0, eta=None, hostname=worker)


class UnverifiedStateTestCase(unittest.TestCase):
    def state_with_started_task(self, worker='worker1', task_id='charge-1'):
        state = EventsState()
        clock = 1
        clock = _send(state, Event('worker-online', hostname=worker), clock)
        for event in task_started_events(worker, id=task_id):
            clock = _send(state, event, clock)
        return state, clock

    def unverified_state(self):
        state, clock = self.state_with_started_task()
        clock = _send(state, Event('worker-offline', hostname='worker1'), clock)
        return state, clock


class WorkerOfflineTests(UnverifiedStateTestCase):
    def test_started_task_on_offline_worker_is_unverified(self):
        state, clock = self.state_with_started_task()
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        self.assertTrue(state.is_unverified('charge-1'))
        self.assertEqual(states.STARTED, state.tasks['charge-1'].state)
        self.assertFalse(state.workers['worker1'].alive)

    def test_reported_state_is_unchanged(self):
        state, clock = self.state_with_started_task()
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        self.assertEqual(
            {'charge-1'},
            state.search_engine.matching_ids('state:STARTED'))
        self.assertEqual(
            {'charge-1'},
            state.search_engine.matching_ids('unverified:true'))

    def test_succeeded_task_is_not_unverified(self):
        state = EventsState()
        clock = 1
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        for event in task_succeeded_events(worker='worker1', id='charge-2'):
            clock = _send(state, event, clock)
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        self.assertFalse(state.is_unverified('charge-2'))
        self.assertEqual(states.SUCCESS, state.tasks['charge-2'].state)

    def test_failed_task_is_not_unverified(self):
        state = EventsState()
        clock = 1
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        for event in task_failed_events(worker='worker1', id='charge-f'):
            clock = _send(state, event, clock)
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        self.assertFalse(state.is_unverified('charge-f'))

    def test_received_only_task_is_not_unverified(self):
        state = EventsState()
        clock = 1
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        clock = _send(state, _received_event('charge-r', 'worker1'), clock)
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        self.assertFalse(state.is_unverified('charge-r'))

    def test_retried_task_is_not_unverified(self):
        state, clock = self.state_with_started_task()
        clock = _send(
            state,
            Event('task-retried', uuid='charge-1', exception='boom',
                  hostname='worker1'),
            clock)
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        self.assertEqual(states.RETRY, state.tasks['charge-1'].state)
        self.assertFalse(state.is_unverified('charge-1'))

    def test_offline_of_other_worker_does_not_flag_task(self):
        state, _ = self.state_with_started_task(worker='worker1')
        _send(state, Event('worker-online', hostname='worker2'), 100)
        _send(state, Event('worker-offline', hostname='worker2'), 101)

        self.assertFalse(state.is_unverified('charge-1'))

    def test_offline_for_unknown_worker_is_ignored(self):
        state, _ = self.state_with_started_task()
        _send(state, Event('worker-offline', hostname='ghost'), 100)

        self.assertFalse(state.is_unverified('charge-1'))

    def test_only_started_task_is_flagged_on_shared_worker(self):
        # The sequence from the bug report: charge-1 STARTED, charge-2
        # SUCCESS, then worker-offline -- only charge-1 is unverified.
        state = EventsState()
        clock = 1
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        for event in task_started_events('worker1', id='charge-1'):
            clock = _send(state, event, clock)
        for event in task_succeeded_events(worker='worker1', id='charge-2'):
            clock = _send(state, event, clock)
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        self.assertTrue(state.is_unverified('charge-1'))
        self.assertFalse(state.is_unverified('charge-2'))
        self.assertEqual(
            {'charge-1'},
            state.search_engine.matching_ids('unverified:true'))


class UnverifiedClearingTests(UnverifiedStateTestCase):
    def test_worker_reonline_does_not_clear(self):
        state, clock = self.unverified_state()
        _send(state, Event('worker-online', hostname='worker1'), clock)

        self.assertTrue(state.is_unverified('charge-1'))

    def test_later_success_clears(self):
        state, clock = self.unverified_state()
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        _send(state, Event('task-succeeded', uuid='charge-1', result='ok',
                           hostname='worker1'), clock)

        self.assertFalse(state.is_unverified('charge-1'))
        self.assertEqual(states.SUCCESS, state.tasks['charge-1'].state)
        self.assertFalse(
            state.search_engine.matching_ids('unverified:true'))

    def test_later_failure_clears(self):
        state, clock = self.unverified_state()
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        _send(state, Event('task-failed', uuid='charge-1', exception='boom',
                           traceback='tb', hostname='worker1'), clock)

        self.assertFalse(state.is_unverified('charge-1'))
        self.assertEqual(states.FAILURE, state.tasks['charge-1'].state)

    def test_restart_on_another_worker_clears(self):
        state, clock = self.unverified_state()
        clock = _send(state, Event('worker-online', hostname='worker2'), clock)
        _send(state, Event('task-started', uuid='charge-1',
                           hostname='worker2'), clock)

        self.assertFalse(state.is_unverified('charge-1'))
        self.assertEqual(
            'worker2', state.tasks['charge-1'].worker.hostname)
        self.assertEqual(states.STARTED, state.tasks['charge-1'].state)
        self.assertTrue(state.workers['worker2'].alive)

    def test_new_execution_does_not_reinherit_old_offline(self):
        # offline -> later success must stay cleared through another offline
        state, clock = self.unverified_state()
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        clock = _send(state, Event('task-succeeded', uuid='charge-1',
                                   result='ok', hostname='worker1'), clock)
        self.assertFalse(state.is_unverified('charge-1'))

        _send(state, Event('worker-offline', hostname='worker1'), clock)
        self.assertFalse(state.is_unverified('charge-1'))


class OutOfOrderEventTests(UnverifiedStateTestCase):
    def test_terminal_event_before_late_offline_is_not_flagged(self):
        state = EventsState()
        clock = 1
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        for event in task_succeeded_events(worker='worker1', id='charge-1'):
            clock = _send(state, event, clock)
        # delayed offline carrying an older clock than the worker has seen
        state.event(_event('worker-offline', 'worker1', clock=2))

        self.assertEqual(states.SUCCESS, state.tasks['charge-1'].state)
        self.assertFalse(state.is_unverified('charge-1'))

    def test_late_offline_before_any_terminal_event_is_applied(self):
        # Normal disorder: task STARTED, offline delayed, nothing proves the
        # execution moved on, so the flag must still appear.
        state = EventsState()
        state.event(_event('worker-online', 'worker1', clock=1))
        state.event(Event(
            'task-received', uuid='charge-1', name='t', args=(), kwargs={},
            retries=0, eta=None, hostname='worker1', clock=2,
            local_received=time.time()))
        state.event(_event('task-started', 'worker1', clock=3,
                           uuid='charge-1'))
        # offline delayed until the worker was observed alive again, but
        # carries the clock of the original disappearance
        state.event(_event('worker-online', 'worker1', clock=5))
        state.event(_event('worker-offline', 'worker1', clock=4))

        # task still belongs to the old offline generation and never got a
        # newer task event; flag applies
        self.assertTrue(state.is_unverified('charge-1'))

    def test_late_old_clock_started_keeps_flag(self):
        state, clock = self.state_with_started_task()  # started at clock 3
        _send(state, Event('worker-offline', hostname='worker1'), clock)
        self.assertTrue(state.is_unverified('charge-1'))

        # a duplicate task-started carrying the pre-offline clock arrives late
        state.event(_event('task-started', 'worker1', clock=3,
                           uuid='charge-1'))
        self.assertTrue(state.is_unverified('charge-1'))

        # a genuinely newer execution clears it
        state.event(_event('task-started', 'worker1', clock=5,
                           uuid='charge-1'))
        self.assertFalse(state.is_unverified('charge-1'))

    def test_offline_older_than_task_event_is_not_flagged(self):
        state = EventsState()
        state.event(_event('worker-online', 'worker1', clock=1))
        state.event(Event(
            'task-received', uuid='charge-1', name='t', args=(), kwargs={},
            retries=0, eta=None, hostname='worker1', clock=2,
            local_received=time.time()))
        state.event(_event('task-started', 'worker1', clock=5,
                           uuid='charge-1'))
        # offline event delayed, clock predates the started event
        state.event(_event('worker-offline', 'worker1', clock=4))

        self.assertFalse(state.is_unverified('charge-1'))


class UnverifiedEvictionAndPersistenceTests(UnverifiedStateTestCase):
    def test_evicted_task_drops_side_record_and_posting(self):
        state = EventsState(max_tasks_in_memory=2)
        clock = 1
        clock = _send(state, Event('worker-online', hostname='worker1'), clock)
        clock = _send(state, _received_event('a', 'worker1'), clock)
        clock = _send(
            state, Event('task-started', uuid='a', hostname='worker1'), clock)
        clock = _send(
            state, Event('worker-offline', hostname='worker1'), clock)
        self.assertTrue(state.is_unverified('a'))

        clock = _send(state, Event('worker-online', hostname='worker2'), clock)
        clock = _send(state, _received_event('b', 'worker2'), clock)
        _send(state, _received_event('c', 'worker2'), clock)

        self.assertEqual({'b', 'c'}, set(state.tasks))
        self.assertNotIn('a', state.unverified_tasks)
        self.assertEqual(set(),
                         state.search_engine.matching_ids('unverified:true'))

    def test_pickle_round_trip_preserves_unverified(self):
        state, clock = self.state_with_started_task()
        _send(state, Event('worker-offline', hostname='worker1'), clock)

        restored = pickle.loads(pickle.dumps(state))

        self.assertTrue(restored.is_unverified('charge-1'))
        self.assertEqual(
            {'charge-1'},
            restored.search_engine.matching_ids('unverified:true'))
        self.assertEqual(states.STARTED,
                         restored.tasks['charge-1'].state)

    def test_pickle_round_trip_of_clean_state_has_empty_set(self):
        state, _ = self.state_with_started_task()

        restored = pickle.loads(pickle.dumps(state))

        self.assertEqual({}, restored.unverified_tasks)
        self.assertEqual(
            set(), restored.search_engine.matching_ids('unverified:true'))


if __name__ == '__main__':
    unittest.main()
