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

"""Observe Thermos tasks on a system

This module provides a number of classes for exposing information about running (active) and
finished Thermos tasks on a system. The primary entry point is the TaskObserver, a thread which
polls a designated Thermos checkpoint root and collates information about all tasks it discovers.

"""
import os
from operator import attrgetter

from twitter.common import log
from twitter.common.lang import Lockable

from apache.thermos.monitoring.process import ProcessSample

from gen.apache.thermos.ttypes import ProcessState, TaskState


# XXX port everything to use the database instead.
class TaskObserver(object):
  def __init__(self, database):
    self._database = database

  @Lockable.sync
  def process_from_name(self, task_id, process_id):
    if task_id in self.all_tasks:
      task = self.all_tasks[task_id].task
      if task:
        for process in task.processes():
          if process.name().get() == process_id:
            return process

  @Lockable.sync
  def task_count(self):
    """
      Return the count of tasks that could be ready properly from disk.
      This may be <= self.task_id_count()
    """
    return dict(
      active=len(self.active_tasks),
      finished=len(self.finished_tasks),
      all=len(self.all_tasks),
    )

  @Lockable.sync
  def task_id_count(self):
    """
      Return the raw count of active and finished task_ids from the TaskDetector.
    """
    num_active = len(list(self._detector.get_task_ids(state='active')))
    num_finished = len(list(self._detector.get_task_ids(state='finished')))
    return dict(active=num_active, finished=num_finished, all=num_active + num_finished)

  def _get_tasks_of_type(self, type):
    """Convenience function to return all tasks of a given type"""
    tasks = {
      'active': self.active_tasks,
      'finished': self.finished_tasks,
      'all': self.all_tasks,
    }.get(type, None)

    if tasks is None:
      log.error('Unknown task type %s' % type)
      return {}

    return tasks

  @Lockable.sync
  def state(self, task_id):
    """Return a dict containing mapped information about a task's state"""
    real_state = self.raw_state(task_id)
    if real_state is None or real_state.header is None:
      return {}
    else:
      return dict(
        task_id=real_state.header.task_id,
        launch_time=real_state.header.launch_time_ms / 1000.0,
        sandbox=real_state.header.sandbox,
        hostname=real_state.header.hostname,
        user=real_state.header.user
      )

  @Lockable.sync
  def raw_state(self, task_id):
    """
      Return the current runner state (thrift blob: gen.apache.thermos.ttypes.RunnerState)
      of a given task id
    """
    if task_id not in self.all_tasks:
      return None
    return self.all_tasks[task_id].state

  @Lockable.sync
  def _task_processes(self, task_id):
    """
      Return the processes of a task given its task_id.

      Returns a map from state to processes in that state, where possible
      states are: waiting, running, success, failed.
    """
    if task_id not in self.all_tasks:
      return {}
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}

    waiting, running, success, failed, killed = [], [], [], [], []
    for process, runs in state.processes.items():
      # No runs ==> nothing started.
      if len(runs) == 0:
        waiting.append(process)
      else:
        if runs[-1].state in (None, ProcessState.WAITING, ProcessState.LOST):
          waiting.append(process)
        elif runs[-1].state in (ProcessState.FORKED, ProcessState.RUNNING):
          running.append(process)
        elif runs[-1].state == ProcessState.SUCCESS:
          success.append(process)
        elif runs[-1].state == ProcessState.FAILED:
          failed.append(process)
        elif runs[-1].state == ProcessState.KILLED:
          killed.append(process)
        else:
          # TODO(wickman)  Consider log.error instead of raising.
          raise self.UnexpectedState(
            "Unexpected ProcessHistoryState: %s" % state.processes[process].state)

    return dict(waiting=waiting, running=running, success=success, failed=failed, killed=killed)

  @Lockable.sync
  def main(self, type=None, offset=None, num=None):
    """Return a set of information about tasks, optionally filtered

      Args:
        type = (all|active|finished|None) [default: all]
        offset = offset into the list of task_ids [default: 0]
        num = number of results to return [default: 20]

      Tasks are sorted by interest:
        - active tasks are sorted by start time
        - finished tasks are sorted by completion time

      Returns:
        {
          tasks: [task_id_1, ..., task_id_N],
          type: query type,
          offset: next offset,
          num: next num
        }

    """
    type = type or 'all'
    offset = offset or 0
    num = num or 20

    # Get a list of all ObservedTasks of requested type
    tasks = sorted((task for task in self._get_tasks_of_type(type).values()),
                   key=attrgetter('mtime'), reverse=True)

    # Filter by requested offset + number of results
    end = num
    if offset < 0:
      offset = offset % len(tasks) if len(tasks) > abs(offset) else 0
    end += offset
    tasks = tasks[offset:end]

    def task_row(observed_task):
      """Generate an output row for a Task"""
      task = self._task(observed_task.task_id)
      # tasks include those which could not be found properly and are hence empty {}
      if task:
        return dict(
            task_id=observed_task.task_id,
            name=task['name'],
            role=task['user'],
            launch_timestamp=task['launch_timestamp'],
            state=task['state'],
            state_timestamp=task['state_timestamp'],
            ports=task['ports'],
            **task['resource_consumption'])

    return dict(
      tasks=filter(None, map(task_row, tasks)),
      type=type,
      offset=offset,
      num=num,
      task_count=self.task_count()[type],
    )

  def _sample(self, task_id):
    if task_id not in self.active_tasks:
      log.debug("Task %s not found in active tasks" % task_id)
      sample = ProcessSample.empty().to_dict()
      sample['disk'] = 0
    else:
      resource_sample = self.active_tasks[task_id].resource_monitor.sample()[1]
      sample = resource_sample.process_sample.to_dict()
      sample['disk'] = resource_sample.disk_usage
      log.debug("Got sample for task %s: %s" % (task_id, sample))
    return sample

  @Lockable.sync
  def task_statuses(self, task_id):
    """
      Return the sequence of task states.

      [(task_state [string], timestamp), ...]
    """

    # Unknown task_id.
    if task_id not in self.all_tasks:
      return []

    task = self.all_tasks[task_id]
    if task is None:
      return []

    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return []

    # Get the timestamp of the transition into the current state.
    return [
      (TaskState._VALUES_TO_NAMES.get(st.state, 'UNKNOWN'), st.timestamp_ms / 1000)
      for st in state.statuses]

  @Lockable.sync
  def tasks(self, task_ids):
    """
      Return information about an iterable of tasks [task_id1, task_id2, ...]
      in the following form.

      {
        task_id1 : self._task(task_id1),
        task_id2 : self._task(task_id2),
        ...
      }
    """
    res = {}
    for task_id in task_ids:
      d = self._task(task_id)
      task_struct = d.pop('task_struct')
      d['task'] = task_struct.get()
      res[task_id] = d
    return res

  @Lockable.sync
  def _task(self, task_id):
    """
      Return composite information about a particular task task_id, given the below
      schema.

      {
         task_id: string,
         name: string,
         user: string,
         launch_timestamp: seconds,
         state: string [ACTIVE, SUCCESS, FAILED]
         ports: { name1: 'url', name2: 'url2' }
         resource_consumption: { cpu:, ram:, disk: }
         processes: { -> names only
            waiting: [],
            running: [],
            success: [],
            failed:  []
         }
      }
    """
    # Unknown task_id.
    if task_id not in self.all_tasks:
      return {}

    task = self.all_tasks[task_id].task
    if task is None:
      # TODO(wickman)  Can this happen?
      log.error('Could not find task: %s' % task_id)
      return {}

    state = self.raw_state(task_id)
    if state is None or state.header is None:
      # TODO(wickman)  Can this happen?
      return {}

    # Get the timestamp of the transition into the current state.
    current_state = state.statuses[-1].state
    last_state = state.statuses[0]
    state_timestamp = 0
    for status in state.statuses:
      if status.state == current_state and last_state != current_state:
        state_timestamp = status.timestamp_ms / 1000
      last_state = status.state

    return dict(
       task_id=task_id,
       name=task.name().get(),
       launch_timestamp=state.statuses[0].timestamp_ms / 1000,
       state=TaskState._VALUES_TO_NAMES[state.statuses[-1].state],
       state_timestamp=state_timestamp,
       user=state.header.user,
       resource_consumption=self._sample(task_id),
       ports=state.header.ports,
       processes=self._task_processes(task_id),
       task_struct=task,
    )

  @Lockable.sync
  def _get_process_resource_consumption(self, task_id, process_name):
    if task_id not in self.active_tasks:
      log.debug("Task %s not found in active tasks" % task_id)
      return ProcessSample.empty().to_dict()
    sample = self.active_tasks[task_id].resource_monitor.sample_by_process(process_name).to_dict()
    log.debug('Resource consumption (%s, %s) => %s' % (task_id, process_name, sample))
    return sample

  @Lockable.sync
  def _get_process_tuple(self, history, run):
    """
      Return the basic description of a process run if it exists, otherwise
      an empty dictionary.

      {
        process_name: string
        process_run: int
        (optional) return_code: int
        state: string [WAITING, FORKED, RUNNING, SUCCESS, KILLED, FAILED, LOST]
        (optional) start_time: seconds from epoch
        (optional) stop_time: seconds from epoch
      }
    """
    if len(history) == 0:
      return {}
    if run >= len(history):
      return {}
    else:
      process_run = history[run]
      run = run % len(history)
      d = dict(
        process_name=process_run.process,
        process_run=run,
        state=ProcessState._VALUES_TO_NAMES[process_run.state],
      )
      if process_run.start_time:
        d.update(start_time=process_run.start_time)
      if process_run.stop_time:
        d.update(stop_time=process_run.stop_time)
      if process_run.return_code:
        d.update(return_code=process_run.return_code)
      return d

  @Lockable.sync
  def process(self, task_id, process, run=None):
    """
      Returns a process run, where the schema is given below:

      {
        process_name: string
        process_run: int
        used: { cpu: float, ram: int bytes, disk: int bytes }
        start_time: (time since epoch in millis (utc))
        stop_time: (time since epoch in millis (utc))
        state: string [WAITING, FORKED, RUNNING, SUCCESS, KILLED, FAILED, LOST]
      }

      If run is None, return the latest run.
    """
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}
    if process not in state.processes:
      return {}
    history = state.processes[process]
    run = int(run) if run is not None else -1
    tup = self._get_process_tuple(history, run)
    if not tup:
      return {}
    if tup.get('state') == 'RUNNING':
      tup.update(used=self._get_process_resource_consumption(task_id, process))
    return tup

  @Lockable.sync
  def _processes(self, task_id):
    """
      Return
        {
          process1: { ... }
          process2: { ... }
          ...
          processN: { ... }
        }

      where processK is the latest run of processK and in the schema as
      defined by process().
    """

    if task_id not in self.all_tasks:
      return {}
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}

    processes = self._task_processes(task_id)
    d = dict()
    for process_type in processes:
      for process_name in processes[process_type]:
        d[process_name] = self.process(task_id, process_name)
    return d

  @Lockable.sync
  def processes(self, task_ids):
    """
      Given a list of task_ids, returns a map of task_id => processes, where processes
      is defined by the schema in _processes.
    """
    if not isinstance(task_ids, (list, tuple)):
      return {}
    return dict((task_id, self._processes(task_id)) for task_id in task_ids)

  @Lockable.sync
  def get_run_number(self, runner_state, process, run=None):
    if runner_state is not None and runner_state.processes is not None:
      run = run if run is not None else -1
      if run < len(runner_state.processes[process]):
        if len(runner_state.processes[process]) > 0:
          return run % len(runner_state.processes[process])
