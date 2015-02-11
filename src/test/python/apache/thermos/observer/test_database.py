import time

from twitter.common.contextutil import temporary_dir

from apache.thermos.monitoring.detector import PathDetector
from apache.thermos.monitoring.resource import ResourceMonitorBase
from apache.thermos.observer.database import TaskObserverDatabase

from gen.apache.thermos.ttypes import RunnerState


class NoOpResourceMonitor(ResourceMonitorBase):
  def __init__(self, task_monitor):
    self.task_monitor = task_monitor
    self.started = False
    self.stopped = False

  def start(self):
    self.started = True

  def kill(self):
    self.stopped = True

  def sample(self):
    return (time.time(), None)

  def sample_at(self, time):
    return (time.time(), None)

  def sample_by_process(self, name):
    return None


class EmptyPathDetector(PathDetector):
  def get_paths(self):
    return []


def test_active_to_removed():
  tod = TaskObserverDatabase(EmptyPathDetector(), resource_monitor_class=NoOpResourceMonitor)

  with temporary_dir() as td:
    tod._on_active(td, 'task1')
    assert 'task1' in tod.get_active_tasks()

    # sanity check on the ActiveTask object
    task1 = tod._active_tasks['task1']
    assert task1.state == RunnerState(processes={})
    assert task1.task_monitor.get_sandbox() is None
    assert task1.resource_monitor.started
    assert not task1.resource_monitor.stopped

    # remove
    tod._on_removed(td, 'task1')
    assert task1.resource_monitor.stopped


def test_finished_to_removed():
  tod = TaskObserverDatabase(EmptyPathDetector(), resource_monitor_class=NoOpResourceMonitor)

  with temporary_dir() as td:
    tod._on_finished(td, 'task1')
    assert 'task1' in tod.get_finished_tasks()

    # sanity check on the ActiveTask object
    task1 = tod._finished_tasks['task1']
    assert task1.state == RunnerState(processes={})

    # remove
    tod._on_removed(td, 'task1')


# TODO(wickman)  Add tests for get_* methods.
