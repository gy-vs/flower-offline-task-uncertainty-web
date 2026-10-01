import json
import re
import time
from urllib.parse import urlencode

from celery.events import Event

from flower.events import EventsState
from flower.views.tasks import visible_task_columns
from tests.unit import AsyncHTTPTestCase
from tests.unit.utils import task_failed_events, task_succeeded_events


class TaskTest(AsyncHTTPTestCase):
    def test_unknown_task(self):
        r = self.get('/task/unknown')
        self.assertEqual(404, r.code)
        self.assertTrue('Unknown task' in str(r.body))

    def test_unknown_task_error_preserves_percent(self):
        r = self.get('/task/foo%25bar')
        self.assertEqual(404, r.code)
        self.assertIn('foo%bar', r.body.decode('utf-8'))
        self.assertNotIn('foo%%bar', r.body.decode('utf-8'))


class TaskControlsTest(AsyncHTTPTestCase):
    def render_task(self, *task_events):
        state = EventsState()
        state.get_or_create_worker('worker1')
        events = [Event('worker-online', hostname='worker1'), *task_events]
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state
        return self.get('/task/123')

    @staticmethod
    def received_event():
        return Event('task-received', uuid='123', name='task1', args='(2, 2)',
                     kwargs="{'foo': 'bar'}", retries=0, eta=None,
                     hostname='worker1')

    @staticmethod
    def started_event():
        return Event('task-started', uuid='123', hostname='worker1')

    def test_task_without_a_name_renders(self):
        # Flower saw this task finish but never saw it received, so it has no name
        r = self.render_task(Event('task-started', uuid='123', hostname='worker1'))
        self.assertEqual(200, r.code)
        body = r.body.decode('utf-8')
        self.assertIn('<title>Task 123 · Flower</title>', body)
        self.assertIn('<span class="value-missing">&mdash;</span>', body)

    def test_task_name_links_to_tasks_with_that_name(self):
        r = self.render_task(self.received_event(), self.started_event())
        self.assertEqual(200, r.code)
        self.assertIn('<a href="/tasks?name=task1">task1</a>', str(r.body))

    def test_task_page_title_has_name_and_short_id(self):
        r = self.render_task(self.received_event(), self.started_event())
        self.assertIn('<title>task1 123 · Flower</title>', r.body.decode('utf-8'))

    def test_started_task_has_terminate_button(self):
        r = self.render_task(self.received_event(), self.started_event())
        self.assertEqual(200, r.code)
        self.assertIn('task-terminate', str(r.body))

    def test_started_task_has_no_terminate_button_in_read_only(self):
        with self.mock_option('read_only', True):
            r = self.render_task(self.received_event(), self.started_event())
        self.assertEqual(200, r.code)
        self.assertNotIn('task-terminate', str(r.body))

    def test_received_task_has_revoke_button(self):
        r = self.render_task(self.received_event())
        self.assertEqual(200, r.code)
        self.assertIn('task-revoke', str(r.body))

    def test_received_task_has_no_revoke_button_in_read_only(self):
        with self.mock_option('read_only', True):
            r = self.render_task(self.received_event())
        self.assertEqual(200, r.code)
        self.assertNotIn('task-revoke', str(r.body))


class UnverifiedTaskViewTest(AsyncHTTPTestCase):
    def render_task(self, *events):
        state = EventsState()
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state
        return self.get('/task/123')

    @staticmethod
    def task_events(*types, worker='worker1'):
        events = []
        for event_type in types:
            if event_type == 'task-received':
                events.append(Event(
                    event_type, uuid='123', name='billing.charge', args='(2, 2)',
                    kwargs="{'foo': 'bar'}", retries=0, eta=None,
                    hostname=worker))
            else:
                events.append(Event(event_type, uuid='123', hostname=worker))
        return events

    def test_unverified_task_keeps_started_state_and_shows_hint(self):
        r = self.render_task(
            Event('worker-online', hostname='worker1'),
            *self.task_events('task-received', 'task-started'),
            Event('worker-offline', hostname='worker1'))

        self.assertEqual(200, r.code)
        body = r.body.decode('utf-8')
        self.assertIn('task-state-started', body)
        self.assertIn('task-state-unverified', body)
        self.assertIn('result is unverified', body)

    def test_unverified_task_has_no_terminate_button(self):
        r = self.render_task(
            Event('worker-online', hostname='worker1'),
            *self.task_events('task-received', 'task-started'),
            Event('worker-offline', hostname='worker1'))

        self.assertEqual(200, r.code)
        self.assertNotIn('task-terminate', str(r.body))

    def test_started_task_on_online_worker_keeps_terminate_button(self):
        r = self.render_task(
            Event('worker-online', hostname='worker1'),
            *self.task_events('task-received', 'task-started'))

        self.assertEqual(200, r.code)
        self.assertIn('task-terminate', str(r.body))
        self.assertNotIn('task-state-unverified', str(r.body))

    def test_worker_coming_back_online_keeps_the_hint(self):
        r = self.render_task(
            Event('worker-online', hostname='worker1'),
            *self.task_events('task-received', 'task-started'),
            Event('worker-offline', hostname='worker1'),
            Event('worker-online', hostname='worker1'))

        self.assertEqual(200, r.code)
        self.assertIn('task-state-unverified', str(r.body))
        self.assertNotIn('task-terminate', str(r.body))

    def test_terminate_button_returns_when_task_starts_elsewhere(self):
        r = self.render_task(
            Event('worker-online', hostname='worker1'),
            *self.task_events('task-received', 'task-started'),
            Event('worker-offline', hostname='worker1'),
            Event('task-started', uuid='123', hostname='worker2'))

        self.assertEqual(200, r.code)
        body = r.body.decode('utf-8')
        self.assertIn('task-terminate', body)
        self.assertNotIn('task-state-unverified', body)

    def test_succeeded_task_shows_no_hint(self):
        r = self.render_task(
            Event('worker-online', hostname='worker1'),
            *self.task_events('task-received', 'task-started'),
            Event('worker-offline', hostname='worker1'),
            Event('task-succeeded', uuid='123', result='4', runtime=0.1,
                  hostname='worker1'))

        self.assertEqual(200, r.code)
        body = r.body.decode('utf-8')
        self.assertIn('text-bg-success', body)
        self.assertNotIn('task-state-unverified', body)

    def test_unverified_task_has_no_terminate_button_in_read_only(self):
        with self.mock_option('read_only', True):
            r = self.render_task(
                Event('worker-online', hostname='worker1'),
                *self.task_events('task-received', 'task-started'),
                Event('worker-offline', hostname='worker1'))
        self.assertEqual(200, r.code)
        body = r.body.decode('utf-8')
        self.assertNotIn('task-terminate', body)
        self.assertIn('task-state-unverified', body)


class UnverifiedTasksTableTest(AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        state = EventsState()
        events = [Event('worker-online', hostname='worker1'),
                  Event('worker-online', hostname='worker2')]
        events += task_succeeded_events(worker='worker2', name='task1', id='ok-1')
        events += task_succeeded_events(worker='worker1', name='task1', id='ok-2')
        events += [Event('task-received', uuid='lost-1', name='billing.charge',
                         args='(2, 2)', kwargs='{}', retries=0, eta=None,
                         hostname='worker1'),
                   Event('task-started', uuid='lost-1', hostname='worker1'),
                   Event('task-received', uuid='pending-1', name='billing.charge',
                         args='(3, 3)', kwargs='{}', retries=0, eta=None,
                         hostname='worker1'),
                   Event('worker-offline', hostname='worker1')]
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state

    def datatable(self, **extra):
        params = {'draw': 1, 'start': 0, 'length': 10,
                  'search[value]': '',
                  'order[0][column]': 0, 'columns[0][data]': 'name',
                  'order[0][dir]': 'asc'}
        params.update(extra)
        r = self.get('/tasks/datatable?' + urlencode(params))
        self.assertEqual(200, r.code)
        return json.loads(r.body.decode('utf-8'))

    def test_rows_carry_the_unverified_flag(self):
        table = self.datatable()

        self.assertEqual(4, table['recordsTotal'])
        flags = {row['uuid']: row['unverified'] for row in table['data']}
        self.assertEqual(
            {'ok-1': False, 'ok-2': False, 'lost-1': True, 'pending-1': False},
            flags)
        # the celery-reported state is untouched
        states = {row['uuid']: row['state'] for row in table['data']}
        self.assertEqual('STARTED', states['lost-1'])
        self.assertEqual('RECEIVED', states['pending-1'])

    def test_unverified_filter_is_applied_server_side(self):
        table = self.datatable(unverified='true')

        self.assertEqual(4, table['recordsTotal'])
        self.assertEqual(1, table['recordsFiltered'])
        self.assertEqual(['lost-1'], [row['uuid'] for row in table['data']])

    def test_unverified_filter_paginates_before_slicing(self):
        # the flagged task sorts last; a client-side-only filter of the
        # first page would miss it
        table = self.datatable(unverified='true', **{
            'order[0][dir]': 'asc', 'columns[0][data]': 'uuid',
            'start': 0, 'length': 2})

        self.assertEqual(1, table['recordsFiltered'])
        self.assertEqual(['lost-1'], [row['uuid'] for row in table['data']])

    def test_state_search_still_matches_unverified_tasks(self):
        table = self.datatable(**{'search[value]': 'state:STARTED'})

        self.assertEqual(['lost-1'], [row['uuid'] for row in table['data']])

    def test_unverified_filter_combines_with_search(self):
        table = self.datatable(unverified='true',
                               **{'search[value]': 'state:STARTED'})
        self.assertEqual(['lost-1'], [row['uuid'] for row in table['data']])

        table = self.datatable(unverified='true',
                               **{'search[value]': 'state:SUCCESS'})
        self.assertEqual([], table['data'])

    def test_tasks_page_offers_the_unverified_filter(self):
        r = self.get('/tasks')
        self.assertEqual(200, r.code)
        self.assertIn('id="task-unverified-filter"', str(r.body))



class TasksTest(AsyncHTTPTestCase):
    def test_no_task(self):
        r = self.get('/tasks')
        self.assertEqual(200, r.code)
        self.assertTrue('UUID' in str(r.body))
        self.assertIn('<title>Tasks · Flower</title>', r.body.decode('utf-8'))
        self.assertNotIn('<tr id=', str(r.body))
        self.assertIn('id="task-search-error"', str(r.body))

    def test_invalid_search_returns_inline_error(self):
        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = 'ab'
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'name'
        params['order[0][dir]'] = 'asc'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode('utf-8'))
        self.assertEqual(200, r.code)
        self.assertEqual([], table['data'])
        self.assertEqual(0, table['recordsTotal'])
        self.assertEqual(0, table['recordsFiltered'])
        self.assertEqual(
            'Substring search terms must contain at least 3 characters '
            'at position 0.',
            table['searchError'])

    def test_succeeded_task(self):
        state = EventsState()
        state.get_or_create_worker('worker1')
        events = [Event('worker-online', hostname='worker1')]
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='123')
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state

        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = ''
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'name'
        params['order[0][dir]'] = 'asc'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode("utf-8"))
        self.assertEqual(200, r.code)
        self.assertEqual(1, table['recordsTotal'])
        self.assertEqual(1, table['recordsFiltered'])
        tasks = table['data']
        self.assertEqual(1, len(tasks))
        self.assertEqual('SUCCESS', tasks[0]['state'])
        self.assertEqual('task1', tasks[0]['name'])
        self.assertEqual('123', tasks[0]['uuid'])
        self.assertEqual('worker1', tasks[0]['worker'])

    def datatable_rows(self, search):
        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = search
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'name'
        params['order[0][dir]'] = 'asc'
        r = self.get('/tasks/datatable?' + urlencode(params))
        self.assertEqual(200, r.code)
        return json.loads(r.body.decode('utf-8'))

    def test_search_with_quoted_phrase(self):
        state = EventsState()
        for uuid, args in (('1', ['hello world']), ('2', ['hello there'])):
            state.event(Event(
                'task-received', uuid=uuid, name='task1', args=args, kwargs={},
                retries=0, eta=None, hostname='worker1', clock=int(uuid),
                local_received=time.time()))
        self._app.events.state = state

        table = self.datatable_rows('args:"hello world"')
        self.assertEqual(['1'], [task['uuid'] for task in table['data']])

    def test_search_with_special_characters(self):
        state = EventsState()
        for uuid, args in (('1', ['<order>']), ('2', ['order'])):
            state.event(Event(
                'task-received', uuid=uuid, name='task1', args=args, kwargs={},
                retries=0, eta=None, hostname='worker1', clock=int(uuid),
                local_received=time.time()))
        self._app.events.state = state

        table = self.datatable_rows('args:<order>')
        self.assertEqual(['1'], [task['uuid'] for task in table['data']])

    def test_search_task_with_list_args(self):
        state = EventsState()
        event = Event(
            'task-received', uuid='123', name='task1',
            args=['needle', 2], kwargs={}, retries=0, eta=None,
            hostname='worker1', clock=1, local_received=time.time())
        state.event(event)
        self._app.events.state = state

        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = 'needle'
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'name'
        params['order[0][dir]'] = 'asc'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode('utf-8'))
        self.assertEqual(200, r.code)
        self.assertEqual(1, table['recordsTotal'])
        self.assertEqual(1, table['recordsFiltered'])
        self.assertEqual('123', table['data'][0]['uuid'])

    def test_failed_task(self):
        state = EventsState()
        state.get_or_create_worker('worker1')
        events = [Event('worker-online', hostname='worker1')]
        events += task_failed_events(worker='worker1', name='task1',
                                     id='123')
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state

        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = ''
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'name'
        params['order[0][dir]'] = 'asc'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode("utf-8"))
        self.assertEqual(200, r.code)
        self.assertEqual(1, table['recordsTotal'])
        self.assertEqual(1, table['recordsFiltered'])
        tasks = table['data']
        self.assertEqual(1, len(tasks))
        self.assertEqual('FAILURE', tasks[0]['state'])
        self.assertEqual('task1', tasks[0]['name'])
        self.assertEqual('123', tasks[0]['uuid'])
        self.assertEqual('worker1', tasks[0]['worker'])

    def test_sort_runtime(self):
        state = EventsState()
        state.get_or_create_worker('worker1')
        events = [Event('worker-online', hostname='worker1')]
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='2', runtime=10.0)
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='4', runtime=10000000.0)
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='3', runtime=20.0)
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='1', runtime=2.0)
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state

        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = ''
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'runtime'
        params['order[0][dir]'] = 'asc'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode("utf-8"))
        self.assertEqual(200, r.code)
        self.assertEqual(4, table['recordsTotal'])
        self.assertEqual(4, table['recordsFiltered'])
        tasks = table['data']
        self.assertEqual(4, len(tasks))

        self.assertEqual('SUCCESS', tasks[0]['state'])
        self.assertEqual('task1', tasks[0]['name'])
        self.assertEqual('1', tasks[0]['uuid'])
        self.assertEqual('worker1', tasks[0]['worker'])
        self.assertEqual(2.0, tasks[0]['runtime'])

        self.assertEqual('SUCCESS', tasks[1]['state'])
        self.assertEqual('task1', tasks[1]['name'])
        self.assertEqual('2', tasks[1]['uuid'])
        self.assertEqual('worker1', tasks[1]['worker'])
        self.assertEqual(10.0, tasks[1]['runtime'])

        self.assertEqual('SUCCESS', tasks[3]['state'])
        self.assertEqual('task1', tasks[3]['name'])
        self.assertEqual('4', tasks[3]['uuid'])
        self.assertEqual('worker1', tasks[3]['worker'])
        self.assertEqual(10000000.0, tasks[3]['runtime'])

    def test_sort_incomparable(self):
        state = EventsState()
        state.get_or_create_worker('worker1')
        events = [Event('worker-online', hostname='worker1')]
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='123')
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='456', runtime=None)
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state

        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = ''
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'runtime'
        params['order[0][dir]'] = 'asc'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode("utf-8"))
        self.assertEqual(200, r.code)
        self.assertEqual(2, table['recordsTotal'])
        self.assertEqual(2, table['recordsFiltered'])
        tasks = table['data']
        self.assertEqual(2, len(tasks))

        self.assertEqual('SUCCESS', tasks[0]['state'])
        self.assertEqual('task1', tasks[0]['name'])
        self.assertEqual('456', tasks[0]['uuid'])
        self.assertEqual('worker1', tasks[0]['worker'])
        self.assertIsNone(tasks[0]['runtime'])

        self.assertEqual('SUCCESS', tasks[1]['state'])
        self.assertEqual('task1', tasks[1]['name'])
        self.assertEqual('123', tasks[1]['uuid'])
        self.assertEqual('worker1', tasks[1]['worker'])

    def test_pagination(self):
        state = EventsState()
        state.get_or_create_worker('worker1')
        events = [Event('worker-online', hostname='worker1')]
        events += task_succeeded_events(worker='worker1', name='task1',
                                        id='123')
        events += task_succeeded_events(worker='worker1', name='task2',
                                        id='456')
        for i, e in enumerate(events):
            e['clock'] = i
            e['local_received'] = time.time()
            state.event(e)
        self._app.events.state = state

        params = {'draw': 1, 'start': 0, 'length': 10}
        params['search[value]'] = ''
        params['order[0][column]'] = 0
        params['columns[0][data]'] = 'name'
        params['order[0][dir]'] = 'asc'
        params['start'] = '0'
        params['length'] = '1'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode("utf-8"))
        self.assertEqual(200, r.code)
        self.assertEqual(2, table['recordsTotal'])
        self.assertEqual(2, table['recordsFiltered'])
        tasks = table['data']
        self.assertEqual(1, len(tasks))

        self.assertEqual('SUCCESS', tasks[0]['state'])
        self.assertEqual('task1', tasks[0]['name'])
        self.assertEqual('123', tasks[0]['uuid'])
        self.assertEqual('worker1', tasks[0]['worker'])

        params['start'] = '1'
        params['length'] = '1'

        r = self.get('/tasks/datatable?' + '&'.join(
            '{}={}'.format(*x) for x in params.items()))

        table = json.loads(r.body.decode("utf-8"))
        self.assertEqual(200, r.code)
        self.assertEqual(2, table['recordsTotal'])
        self.assertEqual(2, table['recordsFiltered'])
        tasks = table['data']
        self.assertEqual(1, len(tasks))

        self.assertEqual('SUCCESS', tasks[0]['state'])
        self.assertEqual('task2', tasks[0]['name'])
        self.assertEqual('456', tasks[0]['uuid'])
        self.assertEqual('worker1', tasks[0]['worker'])


class TaskColumnsTest(AsyncHTTPTestCase):
    def header_columns(self):
        r = self.get('/tasks')
        self.assertEqual(200, r.code)
        return re.findall(r'<th data-column="(\w+)"', r.body.decode('utf-8'))

    def test_listed_columns_keep_the_given_order(self):
        self.assertEqual([('worker', 'Worker'), ('name', 'Name'), ('state', 'State')],
                         visible_task_columns('worker,name,state'))

    def test_unknown_columns_are_dropped(self):
        self.assertEqual([('uuid', 'UUID'), ('name', 'Name')],
                         visible_task_columns(' uuid, bogus,name'))

    def test_header_renders_the_selected_columns_in_order(self):
        with self.mock_option('tasks_columns', 'worker,name,state'):
            self.assertEqual(['worker', 'name', 'state'], self.header_columns())

    def test_default_header(self):
        self.assertEqual(['name', 'uuid', 'state', 'received', 'runtime', 'worker'],
                         self.header_columns())
