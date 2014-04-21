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

import contextlib
import unittest

from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.options import parse_instances
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey
from twitter.common.contextutil import temporary_file

from gen.apache.aurora.api.ttypes import (
    Identity,
    ScheduleStatusResult,
    TaskQuery,
)

from mock import Mock, patch


class TestInstancesParser(unittest.TestCase):
  def test_parse_instances(self):
    instances = '0,1-3,5'
    x = parse_instances(instances)
    assert x == [0, 1, 2, 3, 5]

  def test_parse_none(self):
    assert parse_instances(None) is None
    assert parse_instances("") is None


class TestClientKillCommand(AuroraClientCommandTest):
  @classmethod
  def get_kill_job_response(cls):
    return cls.create_simple_success_response()

  @classmethod
  def assert_kill_job_called(cls, mock_api):
    assert mock_api.kill_job.call_count == 1

  def test_killall_job(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_scheduler_proxy = Mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):

      api = mock_context.get_api('west')
      mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_call_result()
      api.kill_job.return_value = self.get_kill_job_response()
      mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'killall', '--no-batching', '--config=%s' % fp.name, 'west/bozo/test/hello'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), None)

  def test_killall_job(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_scheduler_proxy = Mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):

      api = mock_context.get_api('west')
      api.kill_job.return_value = self.get_kill_job_response()
      mock_context.add_expected_status_query_result(self.create_status_call_result())
      mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'killall', '--config=%s' % fp.name, 'west/bozo/test/hello'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 4
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'), [15, 16, 17, 18, 19])

  def test_kill_job_with_instances_nobatching(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      self.setup_get_tasks_status_calls(api.scheduler_proxy)
      api.kill_job.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--no-batching', 'west/bozo/test/hello/0,2,4-6'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'),
          [0, 2, 4, 5, 6])

  def test_kill_job_with_instances_batched(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      status_result = self.create_status_call_result()
      mock_context.add_expected_status_query_result(status_result)
      api.kill_job.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-6'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'),
          [0, 2, 4, 5, 6])

  def test_kill_job_with_instances_batched_large(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      status_result = self.create_status_call_result()
      mock_context.add_expected_status_query_result(status_result)
      api.kill_job.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-13'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 3
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/bozo/test/hello'),
          [12, 13])

  def test_kill_job_with_instances_batched_maxerrors(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      status_result = self.create_status_call_result()
      failed_status_result  = self.create_error_response()
      mock_context.add_expected_status_query_result(status_result)
      mock_context.add_expected_status_query_result(failed_status_result)
      mock_context.add_expected_status_query_result(failed_status_result)
      mock_context.add_expected_status_query_result(status_result)
      mock_context.add_expected_status_query_result(status_result)
      api.kill_job.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--max-total-failures=1', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-13'])

      # Now check that the right API calls got made. We should have aborted after the third batch.
      assert api.kill_job.call_count == 3


  def test_kill_job_with_empty_instances_batched(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      # set up an empty instance list in the getTasksStatus response
      status_response = self.create_simple_success_response()
      schedule_status = Mock(spec=ScheduleStatusResult)
      status_response.result.scheduleStatusResult = schedule_status
      mock_task_config = Mock()
      schedule_status.tasks = []
      mock_context.add_expected_status_query_result(status_response)
      api.kill_job.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-13'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 0


  def test_kill_job_with_instances_deep_api(self):
    """Test kill client-side API logic."""
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
      self.setup_get_tasks_status_calls(mock_scheduler_proxy)
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/bozo/test/hello/0,2,4-6'])
      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 1
      mock_scheduler_proxy.killTasks.assert_called_with(
        TaskQuery(jobName='hello', environment='test', instanceIds=frozenset([0, 2, 4, 5, 6]),
            owner=Identity(role='bozo')), None)
