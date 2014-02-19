#
# Copyright 2013 Apache Software Foundation
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

from abc import abstractmethod

from .entry_point import get_validated_entry_point
from .status_checker import StatusChecker

from twitter.common.lang import Interface


class TaskError(Exception):
  pass


class TaskRunner(StatusChecker):
  # For now, TaskRunner should just maintain the StatusChecker API.
  pass


class TaskRunnerProvider(Interface):
  class InvalidTask(Exception): pass

  @abstractmethod
  def from_assigned_task(self, assigned_task, sandbox):
    pass


class PkgResourcesTaskRunnerProvider(TaskRunnerProvider):
  def __init__(self, candidate_entry_points):
    self.__candidate_entry_points = [
        get_validated_entry_point(ep, TaskRunnerProvider) for ep in candidate_entry_points]

  def from_assigned_task(self, assigned_task, sandbox):
    for ep in self.__candidate_entry_points:
      try:
        return ep.from_assigned_task(assigned_task, sandbox)
      except TaskRunnerProvider.InvalidTask as e:
        log.debug('%s does not match task: %r' % (ep, e))
        continue
    raise self.Error('No matching runner providers for task!')
