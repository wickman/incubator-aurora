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

import threading
import time
from abc import abstractmethod

from twitter.common.exceptions import ExceptionalThread
from twitter.common.lang import Interface
from twitter.common.quantity import Amount, Time

from apache.thermos.common.path import TaskPath
from apache.thermos.monitoring.monitor import TaskMonitor
from apache.thermos.monitoring.process import ProcessSample
from apache.thermos.monitoring.resource import ResourceMonitorBase, TaskResourceMonitor

from .detector import ObserverTaskDetector
from .observed_task import ActiveObservedTask, FinishedObservedTask


# The following methods should be implemented:
#   get_all_tasks()                -> return set of task_ids
#   get_active_tasks()             -> return set of active task_ids
#   get_finished_tasks()           -> return set of finished task_ids
#   get_state(*task_id)            -> return map {task_id => RunnerState} (possibly map to None)
#   get_task_resources(*task_id)   -> return map {task_id => (ProcessSample, disk)}
#                                     (possibly map to None)
#   get_process_resources(task_id) -> return map {process_name => ProcessSample}
class TaskObserverDatabaseInterface(Interface):
  @abstractmethod
  def get_all_tasks(self):
    """Return a set of all task_ids."""
    pass

  @abstractmethod
  def get_active_tasks(self):
    """Return a set of active task_ids."""
    pass

  @abstractmethod
  def get_finished_tasks(self):
    """Return a set of finished task_ids."""
    pass

  @abstractmethod
  def get_state(self, *task_id):
    """Return a map of RunnerState objects given a set of task_ids.

    If a task_id is not found, the mapped value will be None.
    """
    pass

  @abstractmethod
  def get_task_resources(self, *task_id):
    """Return a map of task resources.

    Returns a tuple of (ProcessSample, disk_usage_in_bytes) or None} given a
    set of task_ids.  If a task_id is not found, the mapped value will be
    None.
    """
    pass

  @abstractmethod
  def get_process_resources(self, task_id):
    """Return a map of process resources given a task_id.

    Returns a map of process name to ProcessSample, or an empty map if the
    task is not found.
    """
    pass


class TaskObserverDatabase(ExceptionalThread, TaskObserverDatabaseInterface):
  """
    The TaskObserver monitors the thermos checkpoint root for active/finished
    tasks.  It is used to be the oracle of the state of all thermos tasks on
    a machine.

    It currently returns JSON, but really should just return objects.  We should
    then build an object->json translator.
  """
  class UnexpectedError(Exception): pass
  class UnexpectedState(Exception): pass

  POLLING_INTERVAL = Amount(1, Time.SECONDS)

  def __init__(self, path_detector, resource_monitor_class=TaskResourceMonitor):
    self._detector = ObserverTaskDetector(
        path_detector,
        self._on_active,
        self._on_finished,
        self._on_removed)
    if not issubclass(resource_monitor_class, ResourceMonitorBase):
      raise ValueError("resource monitor class must implement ResourceMonitorBase!")
    self._resource_monitor = resource_monitor_class
    self._active_tasks = {}    # task_id => ActiveObservedTask
    self._finished_tasks = {}  # task_id => FinishedObservedTask
    self._stop_event = threading.Event()
    self._lock = threading.Lock()
    ExceptionalThread.__init__(self)
    self.daemon = True

  def stop(self):
    self._stop_event.set()

  def _on_active(self, root, task_id):
    pathspec = TaskPath(root=root)
    task_monitor = TaskMonitor(pathspec, task_id)
    resource_monitor = self._resource_monitor(task_monitor)
    resource_monitor.start()  # XXX .start/.kill etc are not part of the Interface :-\
    self._active_tasks[task_id] = ActiveObservedTask(
      task_id=task_id,
      pathspec=pathspec,
      task_monitor=task_monitor,
      resource_monitor=resource_monitor
    )

  def _on_finished(self, root, task_id):
    active_task = self._active_tasks.pop(task_id, None)
    if active_task:
      active_task.resource_monitor.kill()
    self._finished_tasks[task_id] = FinishedObservedTask(
      task_id=task_id,
      pathspec=TaskPath(root=root),
    )

  def _on_removed(self, root, task_id):
    active_task = self._active_tasks.pop(task_id, None)
    if active_task:
      active_task.resource_monitor.kill()
    self._finished_tasks.pop(task_id, None)

  def run(self):
    """
      The internal thread for the observer.  This periodically polls the
      checkpoint root for new tasks, or transitions of tasks from active to
      finished state.
    """
    while not self._stop_event.is_set():
      time.sleep(self.POLLING_INTERVAL.as_(Time.SECONDS))

      with self._lock:
        self._detector.refresh()

  # -- public methods
  def get_active_tasks(self):
    with self._lock:
      return set(self._active_tasks)

  def get_finished_tasks(self):
    with self._lock:
      return set(self._finished_tasks)

  def get_all_tasks(self):
    with self._lock:
      return set(self._active_tasks) | set(self._finished_tasks)

  def get_state(self, *task_ids):
    state_map = dict((task_id, None) for task_id in task_ids)
    with self._lock:
      for task_id in task_ids:
        task = self._active_tasks.get(task_id)
        if task is None:
          task = self._finished_tasks.get(task_id)
        if task is None:
          continue
        state_map[task_id] = task.state
    return state_map

  def get_task_resources(self, *task_ids):
    resources_map = dict((task_id, None) for task_id in task_ids)
    with self._lock:
      for task_id in task_ids:
        task = self._active_tasks.get(task_id)
        if task is not None:
          resource_sample = task.resource_monitor.sample()[1]
          resources_map[task_id] = (resource_sample.process_sample, resource_sample.disk_usage)
    return resources_map

  def get_process_resources(self, task_id):
    with self._lock:
      task = self._active_tasks.get(task_id)
      if task is None:
        return {}

      # this is O(N^2)
      def safe_get_resources(process_name):
        try:
          return task.resource_monitor.sample_by_process(process_name)
        except ValueError:
          return ProcessSample.empty()
      return dict(
          (process_name, safe_get_resources(process_name))
          for process_name in task.task_monitor.get_active_processes())
