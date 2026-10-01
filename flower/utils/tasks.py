import datetime
import time


# pylint: disable=too-many-branches,too-many-locals,too-many-arguments
def iter_tasks(events, limit=None, offset=0, type=None, worker=None, state=None,
               sort_by=None, received_start=None, received_end=None,
               search=None, unverified=False):
    descending = False
    if sort_by is not None:
        assert sort_by.lstrip('-') in SORT_KEYS
        descending = sort_by.startswith('-')
        sort_by = sort_by.lstrip('-')

    page = search_tasks(
        events, limit=limit, offset=offset, type=type, worker=worker,
        state=state, sort_by=sort_by, descending=descending,
        received_start=received_start, received_end=received_end,
        search=search, unverified=unverified)
    task_map = getattr(events.state.tasks, 'data', events.state.tasks)
    for task_id in page.task_ids:
        task = task_map.get(task_id)
        if task is not None:
            yield task_id, task


def search_tasks(events, limit=None, offset=0, type=None, worker=None,
                 state=None, sort_by=None, descending=False,
                 received_start=None, received_end=None, search=None,
                 unverified=False):
    unverified_ids = None
    if unverified:
        unverified_ids = getattr(events.state, 'unverified_tasks', None) or {}
    return events.state.search_engine.search(
        events.state.tasks,
        search or '',
        task_type=type,
        worker=worker,
        state=state,
        unverified_ids=unverified_ids,
        received_start=_convert_datetime(received_start),
        received_end=_convert_datetime(received_end),
        sort_by=sort_by,
        descending=descending,
        offset=offset,
        limit=limit)


def _convert_datetime(value):
    if not value:
        return None
    return time.mktime(
        datetime.datetime.strptime(value, '%Y-%m-%d %H:%M').timetuple())


SORT_KEYS = frozenset({'name', 'state', 'received', 'started'})


def get_task_by_id(events, task_id):
    return events.state.tasks.get(task_id)


def is_unverified(events, task_id):
    "True if the task's worker went offline mid-execution and no task event accounted for it since"
    unverified_tasks = getattr(events.state, 'unverified_tasks', None)
    return bool(unverified_tasks) and task_id in unverified_tasks


def as_dict(task):
    return task.as_dict()
