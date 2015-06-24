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

import time

from twitter.common import log
from twitter.common.quantity import Amount, Time

from apache.aurora.common.http_signaler import HttpSignaler

from .common.task_info import resolve_ports
from .common.task_runner import TaskError, TaskRunner


class HTTPLifecycleManager(TaskRunner):
  ESCALATION_WAIT = Amount(5, Time.SECONDS)

  @classmethod
  def wrap(cls, runner, job, assigned_ports):
    portmap = resolve_ports(job, assigned_ports)
    if portmap and 'health' in portmap:
      escalation_endpoints = [
          job.lifecycle().graceful_shutdown_endpoint().get(),
          job.lifecycle().shutdown_endpoint().get()
      ]
      return cls(runner, 'health', escalation_endpoints)
    else:
      return runner

  def __init__(self,
               runner,
               lifecycle_port,
               escalation_endpoints,
               clock=time):
    self._runner = runner
    self._lifecycle_port = lifecycle_port
    self._escalation_endpoints = escalation_endpoints
    self._clock = clock
    self.__started = False

  def _terminate_http(self):
    http_signaler = HttpSignaler(self._lifecycle_port)

    for endpoint in self._escalation_endpoints:
      handled, _ = http_signaler(endpoint, use_post_method=True)

      if handled:
        self._clock.sleep(self.ESCALATION_WAIT.as_(Time.SECONDS))
        if self._runner.status is not None:
          return True

  # --- public interface
  def start(self, timeout=None):
    self.__started = True
    return self._runner.start(timeout=timeout if timeout is not None else self._runner.MAX_WAIT)

  def stop(self, timeout=None):
    """Stop the runner.  If it's already completed, no-op.  If it's still running, issue a kill."""
    if not self.__started:
      raise TaskError('Failed to call TaskRunner.start.')

    log.info('Invoking runner HTTP teardown.')
    self._terminate_http()

    return self._runner.stop(timeout=timeout if timeout is not None else self._runner.MAX_WAIT)

  @property
  def status(self):
    """Return the StatusResult of this task runner.  This returns None as
       long as no terminal state is reached."""
    return self._runner.status
