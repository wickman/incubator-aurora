#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import contextlib
import functools
import os
import shutil
import threading
import time
import unittest
from collections import namedtuple
from itertools import product

import mock
from thrift.TSerialization import serialize as thrift_serialize
from twitter.common import log
from twitter.common.concurrent import deadline, Timeout
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_rmtree
from twitter.common.quantity import Amount, Time
from twitter.common.testing.clock import ThreadedClock

from apache.aurora.executor.common.interface import mesos_pb2
from apache.aurora.executor.gc_executor import RootedTask, ThermosGCExecutor
from apache.thermos.common.path import TaskPath
from apache.thermos.config.schema import SimpleTask
from apache.thermos.core.runner import TaskRunner
from apache.thermos.monitoring.detector import FixedPathDetector

from gen.apache.aurora.api.constants import LIVE_STATES, TERMINAL_STATES
from gen.apache.aurora.api.ttypes import ScheduleStatus
from gen.apache.aurora.comm.ttypes import AdjustRetainedTasks
from gen.apache.thermos.ttypes import ProcessState, TaskState

FAKE_ROOT = 'fake_root'


def make_task(task_id):
  return RootedTask(FAKE_ROOT, task_id)


ACTIVE_TASKS = (make_task('sleep60-lost'),)


FINISHED_TASKS = {
  make_task('failure'): ProcessState.SUCCESS,
  make_task('failure_limit'): ProcessState.FAILED,
  make_task('hello_world'): ProcessState.SUCCESS,
  make_task('ordering'): ProcessState.SUCCESS,
  make_task('ports'): ProcessState.SUCCESS,
  make_task('sleep60'): ProcessState.KILLED
}

# TODO(wickman) These should be constant sets in the Thermos thrift
THERMOS_LIVES = (TaskState.ACTIVE, TaskState.CLEANING, TaskState.FINALIZING)
THERMOS_TERMINALS = (TaskState.SUCCESS, TaskState.FAILED, TaskState.KILLED, TaskState.LOST)
STARTING_STATES = (ScheduleStatus.STARTING, ScheduleStatus.ASSIGNED)
TASK_ID = 'gc_executor_task_id'
EVENT_WAIT_TIMEOUT_SECS = 10


if 'THERMOS_DEBUG' in os.environ:
  from twitter.common.log.options import LogOptions
  LogOptions.set_disk_log_level('NONE')
  LogOptions.set_stderr_log_level('DEBUG')
  log.init('test_gc_executor')


def thread_yield():
  time.sleep(0.1)


def setup_tree(td, lose=False):
  safe_rmtree(td)

  # TODO(wickman) These should be referred as resources= in the python_target instead.
  shutil.copytree('src/test/resources/org/apache/thermos/root', td)

  if lose:
    lost_age = time.time() - (
      2 * ThinTestThermosGCExecutor.MAX_CHECKPOINT_TIME_DRIFT.as_(Time.SECONDS))
    utime = (lost_age, lost_age)
  else:
    utime = None

  # touch everything
  for root, dirs, files in os.walk(td):
    for fn in files:
      os.utime(os.path.join(root, fn), utime)


StatusUpdate = namedtuple('StatusUpdate', 'state task_id')


class ProxyDriver(object):
  def __init__(self):
    self.stopped = threading.Event()
    self.updates = []

  def stop(self):
    log.debug('ProxyDriver.stop')
    self.stopped.set()

  def sendStatusUpdate(self, update):  # noqa
    log.debug('ProxyDriver.sendStatusUpdate %r' % update)
    self.updates.append(StatusUpdate(update.state, update.task_id.value))


def serialize_art(art, task_id=TASK_ID):
  td = mesos_pb2.TaskInfo()
  td.slave_id.value = 'ignore_me'
  td.task_id.value = task_id
  td.data = thrift_serialize(art)
  return td


class FakeExecutorDetector(object):
  class ExecutorScanf(object):
    def __init__(self, task_id):
      self.executor_id = 'thermos-' + task_id
      self.run = 'some_run_number'

  def __init__(self, *task_ids):
    self.__scanfs = [self.ExecutorScanf(task_id) for task_id in task_ids]

  def __iter__(self):
    return iter(self.__scanfs)


class ThinTestThermosGCExecutor(ThermosGCExecutor):
  POLL_WAIT = Amount(5, Time.MILLISECONDS)
  MINIMUM_KILL_AGE = Amount(5, Time.SECONDS)

  def __init__(self, checkpoint_root, active_executors=[]):
    self._active_executors = active_executors
    self._kills = set()
    self._losses = set()
    self._gcs = set()
    ThermosGCExecutor.__init__(
        self,
        FixedPathDetector(checkpoint_root),
        clock=ThreadedClock(time.time()),
        executor_detector=lambda: list)

  @property
  def gcs(self):
    return self._gcs

  def _gc(self, task):
    self._gcs.add(task)

  def _terminate_task(self, task, kill=True):
    if kill:
      self._kills.add(task)
    else:
      self._losses.add(task)
    return True

  @property
  def linked_executors(self):
    return self._active_executors


class ThickTestThermosGCExecutor(ThinTestThermosGCExecutor):
  def __init__(self, active_tasks, finished_tasks, active_executors=[], corrupt_tasks=[]):
    self._active_tasks = active_tasks
    self._finished_tasks = finished_tasks
    self._corrupt_tasks = corrupt_tasks
    self._maybe_terminate = set()
    ThinTestThermosGCExecutor.__init__(self, FAKE_ROOT, active_executors)

  @property
  def results(self):
    return self._kills, self._losses, self._gcs, self._maybe_terminate

  @property
  def len_results(self):
    return len(self._kills), len(self._losses), len(self._gcs), len(self._maybe_terminate)

  def partition_tasks(self):
    return set(self._active_tasks.keys()), set(self._finished_tasks.keys())

  def maybe_terminate_unknown_task(self, task):
    self._maybe_terminate.add(task)

  def get_states(self, task):
    if task not in self._corrupt_tasks:
      if task in self._active_tasks:
        return [(self._clock.time(), self._active_tasks[task])]
      elif task in self._finished_tasks:
        return [(self._clock.time(), self._finished_tasks[task])]
    return []

  def should_gc_task(self, task):
    if task in self._corrupt_tasks:
      return set([task])
    return set()


def make_pair(*args, **kw):
  return ThickTestThermosGCExecutor(*args, **kw), ProxyDriver()


def llen(*iterables):
  return tuple(len(iterable) for iterable in iterables)


def test_state_reconciliation_no_ops():
  # active vs. active
  for st0, st1 in product(THERMOS_LIVES, LIVE_STATES):
    tgc, driver = make_pair({make_task('foo'): st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)

  # terminal vs. terminal
  for st0, st1 in product(THERMOS_TERMINALS, TERMINAL_STATES):
    tgc, driver = make_pair({}, {make_task('foo'): st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)

  # active vs. starting
  for st0, st1 in product(THERMOS_LIVES, STARTING_STATES):
    tgc, driver = make_pair({make_task('foo'): st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)

  # nexist vs. starting
  for st1 in STARTING_STATES:
    tgc, driver = make_pair({}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)


def test_state_reconciliation_active_terminal():
  for st0, st1 in product(THERMOS_LIVES, TERMINAL_STATES):
    tgc, driver = make_pair({make_task('foo'): st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 1)
    assert llen(lgc, rgc, updates) == (0, 0, 0)


def test_state_reconciliation_active_nexist():
  for st0 in THERMOS_LIVES:
    tgc, driver = make_pair({make_task('foo'): st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {})
    assert tgc.len_results == (0, 0, 0, 1)
    assert llen(lgc, rgc, updates) == (0, 0, 0)


def test_state_reconciliation_terminal_active():
  for st0, st1 in product(THERMOS_TERMINALS, LIVE_STATES):
    tgc, driver = make_pair({}, {make_task('foo'): st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 1)


def test_state_reconciliation_corrupt_tasks():
  for st0, st1 in product(THERMOS_TERMINALS, LIVE_STATES):
    tgc, driver = make_pair({}, {make_task('foo'): st0}, corrupt_tasks=[make_task('foo')])
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (1, 0, 0)


def test_state_reconciliation_terminal_nexist():
  for st0, st1 in product(THERMOS_TERMINALS, LIVE_STATES):
    tgc, driver = make_pair({}, {make_task('foo'): st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (1, 0, 0)
    assert lgc == set([make_task('foo')])


def test_state_reconciliation_nexist_active():
  for st1 in LIVE_STATES:
    tgc, driver = make_pair({}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 1)


def test_state_reconciliation_nexist_terminal():
  for st1 in TERMINAL_STATES:
    tgc, driver = make_pair({}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 1, 0)
    assert rgc == set(['foo'])


def test_real_get_states():
  with temporary_dir() as td:
    setup_tree(td)
    executor = ThinTestThermosGCExecutor(td)
    for task in FINISHED_TASKS:
      real_task = RootedTask(td, task.task_id)
      states = executor.get_states(real_task)
      assert isinstance(states, list) and len(states) > 0
      assert executor.get_sandbox(real_task) is not None


def wait_until_not(thing, timeout=EVENT_WAIT_TIMEOUT_SECS):
  """wait until something is booleany False"""
  def wait():
    while thing():
      time.sleep(0.1)
  try:
    deadline(wait, timeout=timeout, daemon=True)
    return True
  except Timeout:
    return False


def run_gc_with(active_executors, retained_tasks, lose=False):
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    setup_tree(td, lose=lose)
    executor = ThinTestThermosGCExecutor(td, active_executors=active_executors)
    try:
      executor.registered(proxy_driver, None, None, None)
      executor.start()
      art = AdjustRetainedTasks(retainedTasks=retained_tasks)
      executor.launchTask(proxy_driver, serialize_art(art, TASK_ID))
      assert wait_until_not(lambda: executor._gc_task_queue)
      assert wait_until_not(lambda: executor._task_id)
      assert len(executor._gc_task_queue) == 0
      assert not executor._task_id
    finally:
      executor.shutdown(proxy_driver)
  assert len(proxy_driver.updates) >= 1
  if not lose:  # if the task is lost it will be cleaned out of band (by clean_orphans),
                # so we don't care when the GC task actually finishes
    assert proxy_driver.updates[-1][0] == mesos_pb2.TASK_FINISHED
    assert proxy_driver.updates[-1][1] == TASK_ID
  return executor, proxy_driver


def test_gc_with_loss():
  executor, proxy_driver = run_gc_with(
      active_executors=set(task.task_id for task in ACTIVE_TASKS),
      retained_tasks={},
      lose=True)
  assert len(executor._kills) == len(ACTIVE_TASKS)
  assert len(executor.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.updates) >= 1
  assert StatusUpdate(mesos_pb2.TASK_LOST, ACTIVE_TASKS[0].task_id) in proxy_driver.updates


def test_gc_with_starting_task():
  executor, proxy_driver = run_gc_with(
    active_executors=set(task.task_id for task in ACTIVE_TASKS),
    retained_tasks={ACTIVE_TASKS[0].task_id: ScheduleStatus.STARTING})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS)


def test_gc_without_task_missing():
  executor, proxy_driver = run_gc_with(
      active_executors=set(task.task_id for task in ACTIVE_TASKS),
      retained_tasks={},
      lose=False)
  assert len(executor._kills) == len(ACTIVE_TASKS)
  assert len(executor.gcs) == len(FINISHED_TASKS)


def test_gc_without_loss():
  executor, proxy_driver = run_gc_with(
      active_executors=set(task.task_id for task in ACTIVE_TASKS),
      retained_tasks={ACTIVE_TASKS[0].task_id: ScheduleStatus.RUNNING})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS)


def test_gc_withheld():
  executor, proxy_driver = run_gc_with(
      active_executors=set([ACTIVE_TASKS[0].task_id, 'failure']),
      retained_tasks={ACTIVE_TASKS[0].task_id: ScheduleStatus.RUNNING,
                      'failure': ScheduleStatus.FAILED})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS) - 1


def test_gc_withheld_and_executor_missing():
  executor, proxy_driver = run_gc_with(
      active_executors=set(task.task_id for task in ACTIVE_TASKS),
      retained_tasks={ACTIVE_TASKS[0].task_id: ScheduleStatus.RUNNING,
                      'failure': ScheduleStatus.FAILED})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS)


def build_blocking_gc_executor(td, proxy_driver):
  class LongGCThinTestThermosGCExecutor(ThinTestThermosGCExecutor):
    def _run_gc(self, task, retain_tasks, retain_start):
      # just block until we shutdown
      log.debug('%r : _run_gc called (%r, %r, %r)' % (
          self, task, retain_tasks, retain_start))
      self._start_time = retain_start
      self._task_id = task.task_id.value
      self._stop_event.wait()
      self._start_time = None
      self._task_id = None
  executor = LongGCThinTestThermosGCExecutor(td)
  executor.registered(proxy_driver, None, None, None)
  executor.start()
  return executor


def test_gc_killtask_noop():
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = ThinTestThermosGCExecutor(td)
    try:
      executor.registered(proxy_driver, None, None, None)
      executor.start()
      executor.killTask(proxy_driver, TASK_ID)
      assert not proxy_driver.stopped.is_set()
      assert len(proxy_driver.updates) == 0
    finally:
      executor.shutdown(proxy_driver)


def test_gc_killtask_current():
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = build_blocking_gc_executor(td, proxy_driver)

    try:
      executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks()))
      assert wait_until_not(lambda: len(executor._gc_task_queue))
      assert len(executor._gc_task_queue) == 0
      assert executor._task_id == TASK_ID
      executor.killTask(proxy_driver, TASK_ID)
      assert executor._task_id == TASK_ID
      assert len(executor._gc_task_queue) == 0
      assert not proxy_driver.stopped.is_set()
      assert len(proxy_driver.updates) == 0
    finally:
      executor.shutdown(proxy_driver)


def test_gc_killtask_queued():
  TASK2_ID = "task2"
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = build_blocking_gc_executor(td, proxy_driver)
    try:
      executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks()))
      thread_yield()
      executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks(), task_id=TASK2_ID))
      thread_yield()
      assert len(executor._gc_task_queue) == 1
      executor.killTask(proxy_driver, TASK2_ID)
      thread_yield()
      assert len(executor._gc_task_queue) == 0
      assert not proxy_driver.stopped.is_set()
      assert len(proxy_driver.updates) == 0
    finally:
      executor.shutdown(proxy_driver)


def test_gc_multiple_launchtasks():
  TASK2, TASK3 = "task2", "task3"
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = build_blocking_gc_executor(td, proxy_driver)

    try:
      executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks()))
      thread_yield()
      executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks(), task_id=TASK2))
      thread_yield()
      assert len(executor._gc_task_queue) == 1
      executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks(), task_id=TASK3))
      thread_yield()
      assert len(executor._gc_task_queue) == 1
      assert not proxy_driver.stopped.is_set()
      assert len(proxy_driver.updates) >= 1
      assert StatusUpdate(mesos_pb2.TASK_FINISHED, TASK2) in proxy_driver.updates
    finally:
      executor.shutdown(proxy_driver)


def test_gc_shutdown():
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = ThinTestThermosGCExecutor(td)
    executor.registered(proxy_driver, None, None, None)
    executor.start()
    executor.shutdown(proxy_driver)
    executor._stop_event.wait(timeout=EVENT_WAIT_TIMEOUT_SECS)
    assert executor._stop_event.is_set()
  proxy_driver.stopped.wait(timeout=EVENT_WAIT_TIMEOUT_SECS)
  assert proxy_driver.stopped.is_set()
  assert len(proxy_driver.updates) == 0


def test_ignores_launch_task_when_shutting_down():
  """Newly launched tasks should be rejected if shutdown was already called."""
  TASK_ID = "task"
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = build_blocking_gc_executor(td, proxy_driver)
    executor.shutdown(proxy_driver)
    executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks(), task_id=TASK_ID))
    assert (mesos_pb2.TASK_FAILED, TASK_ID) == proxy_driver.updates[-1]


def make_gc_executor_with_timeouts(maximum_executor_lifetime=Amount(1, Time.DAYS)):
  class TimeoutGCExecutor(ThinTestThermosGCExecutor):
    MAXIMUM_EXECUTOR_LIFETIME = maximum_executor_lifetime
  return TimeoutGCExecutor


@contextlib.contextmanager
def run_gc_with_timeout(**kw):
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor_class = make_gc_executor_with_timeouts(**kw)
    executor = executor_class(td)
    executor.registered(proxy_driver, None, None, None)
    executor.start()
    yield (proxy_driver, executor)
    executor.shutdown(proxy_driver)


def test_gc_lifetime():
  with run_gc_with_timeout(maximum_executor_lifetime=Amount(500, Time.MILLISECONDS)) as (
      proxy_driver, executor):
    executor._clock.tick(1)
    proxy_driver.stopped.wait(timeout=EVENT_WAIT_TIMEOUT_SECS)
    assert proxy_driver.stopped.is_set()
    assert not executor._stop_event.is_set()


DIRECTORY_SANDBOX = 'apache.aurora.executor.gc_executor.DirectorySandbox'


class TestRealGC(unittest.TestCase):
  """
    Test functions against the actual garbage_collect() functionality of the GC executor
  """

  def setUp(self):
    self.HELLO_WORLD = SimpleTask(name="foo", command="echo hello world")

  def setup_task(self, task, root, finished=False, corrupt=False):
    """Set up the checkpoint stream for the given task in the given checkpoint root, optionally
    finished and/or with a corrupt stream"""
    class FastTaskRunner(TaskRunner):
      COORDINATOR_INTERVAL_SLEEP = Amount(1, Time.MILLISECONDS)
    tr = FastTaskRunner(
        task=task,
        checkpoint_root=root,
        sandbox=os.path.join(root, 'sandbox', task.name().get()),
        clock=ThreadedClock(time.time()))
    with tr.control():
      # initialize checkpoint stream
      pass
    if finished:
      tr.kill()
    if corrupt:
      ckpt_file = TaskPath(root=root, tr=tr.task_id).getpath('runner_checkpoint')
      with open(ckpt_file, 'w') as f:
        f.write("definitely not a valid checkpoint stream")
    return tr.task_id

  def run_gc(self, root, task_id, retain=False):
    """Run the garbage collection process against the given task_id in the given checkpoint root"""
    class FakeTaskKiller(object):
      def __init__(self, task_id, checkpoint_root):
        pass

      def kill(self):
        pass

      def lose(self):
        pass

    class FakeTaskGarbageCollector(object):
      def __init__(self, root, task_id):
        pass

      def erase_logs(self):
        pass

      def erase_metadata(self):
        pass

    class FastThermosGCExecutor(ThermosGCExecutor):
      POLL_WAIT = Amount(1, Time.MILLISECONDS)

    detector = functools.partial(FakeExecutorDetector, task_id) if retain else FakeExecutorDetector
    executor = FastThermosGCExecutor(
        path_detector=FixedPathDetector(root),
        task_killer=FakeTaskKiller,
        executor_detector=detector,
        task_garbage_collector=FakeTaskGarbageCollector,
        clock=ThreadedClock(time.time()))
    return executor.garbage_collect()

  def test_active_task_no_runners(self):
    # TODO(jon): implement
    pass

  def test_active_task_running(self):
    # TODO(jon): implement
    pass

  def test_finished_task_corrupt(self):
    # TODO(jon): implement
    pass

  def test_gc_task_no_sandbox(self):
    with mock.patch(DIRECTORY_SANDBOX) as directory_mock:
      directory_sandbox = directory_mock.return_value
      with temporary_dir() as root:
        task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
        gcs = self.run_gc(root, task_id)
        directory_sandbox.exists.assert_called_with()
        assert len(gcs) == 1

  def test_gc_task_directory_sandbox(self):
    with mock.patch(DIRECTORY_SANDBOX) as directory_mock:
      directory_sandbox = directory_mock.return_value
      directory_sandbox.exists.return_value = True
      with temporary_dir() as root:
        task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
        gcs = self.run_gc(root, task_id)
        directory_sandbox.exists.assert_called_with()
        directory_sandbox.destroy.assert_called_with()
        assert len(gcs) == 1

  def test_gc_ignore_retained_task(self):
    with temporary_dir() as root:
      task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
      gcs = self.run_gc(root, task_id, retain=True)
      assert len(gcs) == 0
