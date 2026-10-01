import collections
import glob
import logging
import os
import shelve
import threading
import time
from collections import Counter
from functools import partial

from celery import states
from celery.events import EventReceiver
from celery.events.state import State
from celery.events.state import _serialize_Task_WeakSet_Mapping
from kombu.exceptions import OperationalError
from prometheus_client import Counter as PrometheusCounter
from prometheus_client import Gauge, Histogram
from tornado.ioloop import PeriodicCallback

from .options import options
from .utils.search import TaskSearchEngine

logger = logging.getLogger(__name__)
PROMETHEUS_METRICS = None


def get_prometheus_metrics():
    global PROMETHEUS_METRICS  # pylint: disable=global-statement
    if PROMETHEUS_METRICS is None:
        PROMETHEUS_METRICS = PrometheusMetrics()

    return PROMETHEUS_METRICS


class PrometheusMetrics:
    def __init__(self):
        self.events = PrometheusCounter('flower_events_total', "Number of events", ['worker', 'type', 'task'])

        self.runtime = Histogram(
            'flower_task_runtime_seconds',
            "Task runtime",
            ['worker', 'task'],
            buckets=options.task_runtime_metric_buckets
        )
        self.prefetch_time = Gauge(
            'flower_task_prefetch_time_seconds',
            "The time the task spent waiting at the celery worker to be executed.",
            ['worker', 'task']
        )
        self.number_of_prefetched_tasks = Gauge(
            'flower_worker_prefetched_tasks',
            'Number of tasks of given type prefetched at a worker',
            ['worker', 'task']
        )
        self.worker_online = Gauge('flower_worker_online', "Worker online status", ['worker'])
        self.worker_number_of_currently_executing_tasks = Gauge(
            'flower_worker_number_of_currently_executing_tasks',
            "Number of tasks currently executing at a worker",
            ['worker']
        )

    def observe_task(self, worker, event, task):
        event_type = event['type']
        name = event.get('name') or task.name or ''
        self.events.labels(worker, event_type, name).inc()
        if event.get('runtime'):
            self.runtime.labels(worker, name).observe(event['runtime'])

        # Prefetch metrics only make sense for tasks without a scheduled time
        if task.eta or not task.received:
            return
        if event_type == 'task-received':
            self.number_of_prefetched_tasks.labels(worker, name).inc()
        elif event_type == 'task-started' and task.started:
            self.prefetch_time.labels(worker, name).set(task.started - task.received)
            self.number_of_prefetched_tasks.labels(worker, name).dec()
        elif event_type in ('task-succeeded', 'task-failed') and task.started:
            self.prefetch_time.labels(worker, name).set(0)
        elif event_type in ('task-revoked', 'task-rejected') and not task.started:
            self.number_of_prefetched_tasks.labels(worker, name).dec()

    def observe_worker(self, worker, event):
        online = {'worker-online': 1, 'worker-heartbeat': 1, 'worker-offline': 0}
        if event['type'] in online:
            self.worker_online.labels(worker).set(online[event['type']])
        if event['type'] == 'worker-heartbeat' and event.get('active') is not None:
            self.worker_number_of_currently_executing_tasks.labels(worker).set(event['active'])

    def remove_workers(self, worker_names):
        metrics = (
            self.events,
            self.runtime,
            self.prefetch_time,
            self.number_of_prefetched_tasks,
            self.worker_online,
            self.worker_number_of_currently_executing_tasks,
        )
        removed_workers = set()
        for metric in metrics:
            labels_to_remove = [
                labels for labels in metric._metrics  # pylint: disable=protected-access
                if labels and labels[0] in worker_names
            ]
            for labels in labels_to_remove:
                removed_workers.add(labels[0])
                metric.remove(*labels)
        return len(removed_workers)


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)
        self.metrics = get_prometheus_metrics()
        # task id -> clock of the worker-offline event that left the task
        # without a known worker. Only STARTED tasks can appear here.
        self.unverified_tasks = {}
        self.search_engine = TaskSearchEngine(is_unverified=self.is_unverified)
        self.search_engine.rebuild(self.tasks.items())

    def is_unverified(self, task_id):
        return task_id in self.unverified_tasks

    def __reduce__(self):
        # Celery's State.__reduce__ only restores the fields its constructor
        # knows about, silently dropping subclass attributes. Carry the
        # unverified set in the pickle state; counter is persisted separately
        # by Events.save_state and the search engine is rebuilt on unpickling.
        return (
            self.__class__,
            (
                self.event_callback, self.workers, self.tasks, None,
                self.max_workers_in_memory, self.max_tasks_in_memory,
                self.on_node_join, self.on_node_leave,
                _serialize_Task_WeakSet_Mapping(self.tasks_by_type),
                _serialize_Task_WeakSet_Mapping(self.tasks_by_worker),
            ),
            {'unverified_tasks': dict(self.unverified_tasks)},
        )

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.search_engine = TaskSearchEngine(is_unverified=self.is_unverified)
        self.search_engine.rebuild(self.tasks.items())

    def _clear_tasks(self, ready=True):
        super()._clear_tasks(ready)
        # _clear_tasks only removes ready tasks; unverified tasks are never
        # ready, but drop records for anything that left the task table.
        self.unverified_tasks = {
            task_id: clock
            for task_id, clock in self.unverified_tasks.items()
            if task_id in self.tasks
        }
        self.search_engine.rebuild(self.tasks.items())

    def _eviction_candidate(self, task_id):
        # Celery discards the least recently used task when a new one
        # arrives at the limit, so remember which one may go
        limit = self.tasks.limit
        if not limit or task_id in self.tasks or len(self.tasks) < limit:
            return None
        return next(iter(self.tasks), None)

    def _index_task(self, task, evicted):
        if evicted is not None and evicted not in self.tasks:
            self.search_engine.remove(evicted)
            self.unverified_tasks.pop(evicted, None)
        self.search_engine.upsert(task)

    def _refresh_unverified(self, task_id):
        # Any newer task event proves the task is no longer tied to the
        # process whose worker sent the offline event. An offline event
        # arriving late (older clock than the task event) does not clear it.
        offline_clock = self.unverified_tasks.get(task_id)
        if offline_clock is not None:
            task = self.tasks.get(task_id)
            if task is None or task.state != states.STARTED or \
                    (task.clock or 0) > offline_clock:
                del self.unverified_tasks[task_id]

    def _mark_worker_offline(self, worker, event):
        # Only tasks bound to this exact worker object belong to the offline
        # generation; a newer live worker with the same hostname keeps a
        # different object and is never scanned here.
        event_clock = event.get('clock')
        for task_id, task in list(self.tasks.items()):
            if task_id in self.unverified_tasks:
                continue
            if task.state != states.STARTED or task.worker is not worker:
                continue
            # An offline event older than the task's last event cannot
            # describe the execution the task event reported.
            if event_clock is not None and task.clock \
                    and task.clock > event_clock:
                continue
            self.unverified_tasks[task_id] = event_clock
            self.search_engine.upsert(task)

    def event(self, event):
        event_type, worker_name = event['type'], event['hostname']
        is_task = event_type.startswith('task-')
        evicted = self._eviction_candidate(event.get('uuid')) if is_task else None

        # An offline event pops and may create workers; keep the one the
        # event describes so its tasks can be matched by object identity.
        offline_worker = self.workers.get(worker_name) \
            if event_type == 'worker-offline' else None

        super().event(event)
        self.counter[worker_name][event_type] += 1

        if is_task:
            task = self.tasks[event['uuid']]
            self._refresh_unverified(event['uuid'])
            self._index_task(task, evicted)
            self.metrics.observe_task(worker_name, event, task)
        else:
            if event_type == 'worker-offline' and offline_worker is not None:
                self._mark_worker_offline(offline_worker, event)
            self.metrics.observe_worker(worker_name, event)


class Events(threading.Thread):
    events_enable_interval = 5000

    # pylint: disable=too-many-arguments
    def __init__(self, capp, io_loop, db=None, persistent=False,
                 enable_events=True, state_save_interval=0,
                 *, max_tasks_in_memory, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop
        self.capp = capp

        self.db = db
        self.persistent = persistent
        self.enable_events = enable_events
        self.state = None
        self.state_save_timer = None
        self.state_save_interval = state_save_interval

        if self.persistent:
            self.state = self.load_state()
            if self.state:
                # A restored state keeps the limit it was saved with
                self.state.max_tasks_in_memory = self.state.tasks.limit = max_tasks_in_memory
                self.state.tasks.update()
                self.state.unverified_tasks = {
                    task_id: clock
                    for task_id, clock
                    in getattr(self.state, 'unverified_tasks', {}).items()
                    if task_id in self.state.tasks
                    and self.state.tasks[task_id].state == states.STARTED
                }
                # The search engine holds live inverted indexes and is rebuilt
                # from the restored task table.
                self.state.search_engine.rebuild(self.state.tasks.items())

            if state_save_interval:
                self.state_save_timer = PeriodicCallback(self.save_state,
                                                         state_save_interval)

        if not self.state:
            self.state = EventsState(max_tasks_in_memory=max_tasks_in_memory, **kwargs)

        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

    def start(self):
        threading.Thread.start(self)
        if self.enable_events:
            logger.debug("Starting enable events timer...")
            self.timer.start()

        if self.state_save_timer:
            logger.debug("Starting state save timer...")
            self.state_save_timer.start()

    def stop(self):
        if self.enable_events:
            logger.debug("Stopping enable events timer...")
            self.timer.stop()

        if self.state_save_timer:
            logger.debug("Stopping state save timer...")
            self.state_save_timer.stop()

        if self.persistent:
            self.save_state()

    def run(self):
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                with self.capp.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_event},
                                         app=self.capp)
                    try_interval = 1
                    logger.debug("Capturing events...")
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit):
                import _thread
                _thread.interrupt_main()
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def load_state(self):
        logger.debug("Loading state from '%s'...", self.db)
        try:
            with shelve.open(self.db) as state:
                if not state:
                    return None
                events = state['events']
                events.counter.update(state.get('counter', {}))
                return events
        except Exception as e:
            logger.error("Failed to load state from '%s', moving it aside "
                         "and starting fresh: %s", self.db, e)
            # dbm backends may add suffixes like .db or .dat to the actual files
            for suffix in ('', '.db', '.dat', '.dir', '.bak'):
                name = self.db + suffix
                if os.path.exists(name):
                    os.replace(name, f'{name}.corrupt')
            return None

    def save_state(self):
        logger.debug("Saving state to '%s'...", self.db)
        started = time.monotonic()
        tmp = f'{self.db}.tmp'
        with shelve.open(tmp, flag='n') as state:
            state['events'] = self.state
            state['counter'] = dict(self.state.counter)
        # dbm backends may add suffixes like .db or .dat to the actual files
        for name in glob.glob(glob.escape(tmp) + '*'):
            os.replace(name, self.db + name[len(tmp):])

        elapsed = time.monotonic() - started
        interval_seconds = self.state_save_interval / 1000
        if self.state_save_timer and elapsed > interval_seconds / 10:
            logger.warning(
                "Saving state took %.1fs, consider increasing "
                "--state-save-interval or decreasing --max-tasks", elapsed)

    async def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        try:
            await self.io_loop.run_in_executor(
                None, self.capp.control.enable_events)
        except OperationalError as exc:
            logger.warning("Failed to enable events: %s", exc)

    def on_event(self, event):
        # Call EventsState.event in ioloop thread to avoid synchronization
        self.io_loop.add_callback(partial(self.state.event, event))
