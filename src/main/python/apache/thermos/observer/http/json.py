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

import json
import urllib

from thrift.protocol import TJSONProtocol
from thrift.TSerialization import serialize as thrift_serialize
from twitter.common.http import HttpServer


def thrift_to_json(blob):
  return json.loads(thrift_serialize(blob, TJSONProtocol.TSimpleJSONProtocolFactory()))


class TaskObserverJSONBindings(object):
  """Mixin for Thermos observer JSON endpoints."""

  DEFAULT_PAGINATION = 20

  def __init__(self, database):
    self._database = database

  def __get_task_ids(self, which=None, offset=None, num=None):
    if which is None:
      task_ids = self._database.get_all_tasks()
    elif which == 'active':
      task_ids = self._database.get_active_tasks()
    elif which == 'finished':
      task_ids = self._database.get_finished_tasks()
    else:
      HttpServer.abort(404, 'Unknown task type: %s' % which)
    try:
      offset = int(offset) if offset is not None else 0
      num = int(num) if num is not None else 20
    except ValueError:
      HttpServer.abort(404, 'Invalid offset or count.')
    return sorted(task_ids)[offset:offset + num]

  @HttpServer.route("/j/task_ids")
  @HttpServer.route("/j/task_ids/:which")
  @HttpServer.route("/j/task_ids/:which/:offset")
  @HttpServer.route("/j/task_ids/:which/:offset/:num")
  def handle_task_ids(self, which=None, offset=None, num=None):
    return self.__get_task_ids(which, offset, num)

  @HttpServer.route("/j/task/:task_id")
  def handle_task(self, task_id):
    task = self._database.get_state(task_id).get(task_id)
    if task is None:
      HttpServer.abort(404, 'Task %s not found' % task_id)
    return thrift_to_json(task)

  @HttpServer.route("/j/tasks")
  def handle_tasks(self):
    """
      Additional parameters:
        task_id = comma separated list of task_ids.
    """
    task_ids = HttpServer.Request.GET.get('task_id', [])
    if task_ids:
      task_ids = urllib.unquote(task_ids).split(',')
    else:
      return {}
    return dict(
        (task_id, thrift_to_json(task_blob))
        for (task_id, task_blob) in self._database.get_state(*task_ids).items()
        if task_blob is not None)

  @HttpServer.route("/j/process/:task_id/:process")
  @HttpServer.route("/j/process/:task_id/:process/:run")
  def handle_process(self, task_id, process, run=None):
    task = self._database.get_state(task_id).get(task_id)
    if task is None:
      HttpServer.abort(404, 'Task %s not found' % task_id)
    run = run if run is not None else -1
    try:
      return thrift_to_json(task.processes[process][run])
    except (KeyError, ValueError):
      HttpServer.abort(404, 'Process %s or run %d not found.' % (process, run))

  @HttpServer.route("/j/processes")
  def handle_processes(self):
    """
      Additional parameters:
        task_ids = comma separated list of task_ids.
    """
    task_ids = HttpServer.Request.GET.get('task_id', [])
    if task_ids:
      task_ids = urllib.unquote(task_ids).split(',')
    else:
      return {}
    return dict(
        (task_id, runner_state.processes.keys())
        for (task_id, runner_state) in self._database.get_state(*task_ids).items()
        if runner_state is not None)


"""
from apache.thermos.observer.http.json import TaskObserverJSONBindings
from apache.thermos.observer.database import TaskObserverDatabase
from apache.thermos.monitoring.detector import FixedPathDetector
fpd = FixedPathDetector('/var/run/thermos')
db = TaskObserverDatabase(fpd)
db.start()
js = TaskObserverJSONBindings(db)
"""
