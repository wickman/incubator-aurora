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

import json
from collections import namedtuple
from difflib import unified_diff

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AddInstancesConfig,
    JobKey,
    Identity,
    Lock,
    LockKey,
    LockValidation,
    Response,
    ResponseCode,
    TaskQuery,
)

from .instance_watcher import InstanceWatcher
from .quota_check import CapacityRequest, QuotaCheck
from .scheduler_client import SchedulerProxy
from .updater_util import FailureThreshold, UpdaterConfig

from thrift.protocol import TJSONProtocol
from thrift.TSerialization import serialize
from twitter.common import log

class Updater(object):
  """Update the instances of a job in batches."""

  class Error(Exception): pass

  InstanceState = namedtuple('InstanceState', ['instance_id', 'is_updated'])
  OperationConfigs = namedtuple('OperationConfigs', ['from_config', 'to_config'])
  InstanceConfigs = namedtuple(
      'InstanceConfigs',
      ['remote_config_map', 'local_config_map', 'instances_to_process']
  )

  def __init__(self,
               config,
               health_check_interval_seconds,
               scheduler=None,
               instance_watcher=None,
               quota_check=None):
    self._config = config
    self._job_key = JobKey(role=config.role(), environment=config.environment(), name=config.name())
    self._health_check_interval_seconds = health_check_interval_seconds
    self._scheduler = scheduler or SchedulerProxy(config.cluster())
    self._quota_check = quota_check or QuotaCheck(self._scheduler)
    try:
      self._update_config = UpdaterConfig(**config.update_config().get())
    except ValueError as e:
      raise self.Error(str(e))
    self._lock = None
    self._watcher = instance_watcher or InstanceWatcher(
        self._scheduler,
        self._job_key,
        self._update_config.restart_threshold,
        self._update_config.watch_secs,
        self._health_check_interval_seconds)

  def _start(self):
    """Starts an update by applying an exclusive lock on a job being updated.

       Returns:
         Response instance from the scheduler call.
    """
    resp = self._scheduler.acquireLock(LockKey(job=self._job_key))
    if resp.responseCode == ResponseCode.OK:
      self._lock = resp.result.acquireLockResult.lock
    return resp

  def _finish(self):
    """Finishes an update by removing an exclusive lock on an updated job.

       Returns:
         Response instance from the scheduler call.
    """
    resp = self._scheduler.releaseLock(self._lock, LockValidation.CHECKED)

    if resp.responseCode == ResponseCode.OK:
      self._lock = None
    else:
      log.error('There was an error finalizing the update: %s' % resp.message)
    return resp

  def _update(self, instance_configs):
    """Drives execution of the update logic. Performs a batched update/rollback for all instances
    affected by the current update request.

    Arguments:
    instance_configs -- list of instance update configurations to go through.

    Returns the set of instances that failed to update.
    """
    failure_threshold = FailureThreshold(
        self._update_config.max_per_instance_failures,
        self._update_config.max_total_failures
    )

    instance_operation = self.OperationConfigs(
      from_config=instance_configs.remote_config_map,
      to_config=instance_configs.local_config_map
    )

    remaining_instances = [
        self.InstanceState(instance_id, is_updated=False)
        for instance_id in instance_configs.instances_to_process
    ]

    log.info('Starting job update.')
    while remaining_instances and not failure_threshold.is_failed_update():
      batch_instances = remaining_instances[0 : self._update_config.batch_size]
      remaining_instances = list(set(remaining_instances) - set(batch_instances))
      instances_to_restart = [s.instance_id for s in batch_instances if s.is_updated]
      instances_to_update = [s.instance_id for s in batch_instances if not s.is_updated]

      instances_to_watch = []
      if instances_to_restart:
        instances_to_watch += self._restart_instances(instances_to_restart)

      if instances_to_update:
        instances_to_watch += self._update_instances(instances_to_update, instance_operation)

      failed_instances = self._watcher.watch(instances_to_watch) if instances_to_watch else set()

      if failed_instances:
        log.error('Failed instances: %s' % failed_instances)

      unretryable_instances = failure_threshold.update_failure_counts(failed_instances)
      if unretryable_instances:
        log.warn('Not restarting failed instances %s, which exceeded '
                 'maximum allowed instance failure limit of %s' %
                 (unretryable_instances, self._update_config.max_per_instance_failures))
      retryable_instances = list(set(failed_instances) - set(unretryable_instances))
      remaining_instances += [
          self.InstanceState(instance_id, is_updated=True) for instance_id in retryable_instances
      ]
      remaining_instances.sort(key=lambda tup: tup.instance_id)

    if failure_threshold.is_failed_update():
      untouched_instances = [s.instance_id for s in remaining_instances if not s.is_updated]
      instances_to_rollback = list(
          set(instance_configs.instances_to_process) - set(untouched_instances)
      )
      self._rollback(instances_to_rollback, instance_configs)

    return not failure_threshold.is_failed_update()

  def _rollback(self, instances_to_rollback, instance_configs):
    """Performs a rollback operation for the failed instances.

    Arguments:
    instances_to_rollback -- instance ids to rollback.
    instance_configs -- instance configuration to use for rollback.
    """
    if not self._update_config.rollback_on_failure:
      log.info('Rollback on failure is disabled in config. Aborting rollback')
      return

    log.info('Reverting update for %s' % instances_to_rollback)
    instance_operation = self.OperationConfigs(
        from_config=instance_configs.local_config_map,
        to_config=instance_configs.remote_config_map
    )
    instances_to_rollback.sort(reverse=True)
    failed_instances = []
    while instances_to_rollback:
      batch_instances = instances_to_rollback[0 : self._update_config.batch_size]
      instances_to_rollback = list(set(instances_to_rollback) - set(batch_instances))
      instances_to_rollback.sort(reverse=True)
      instances_to_watch = self._update_instances(batch_instances, instance_operation)
      failed_instances += self._watcher.watch(instances_to_watch)

    if failed_instances:
      log.error('Rollback failed for instances: %s' % failed_instances)

  def _hashable(self, element):
    if isinstance(element, (list, set)):
      return tuple(sorted(self._hashable(item) for item in element))
    elif isinstance(element, dict):
      return tuple(
          sorted((self._hashable(key), self._hashable(value)) for (key, value) in element.items())
      )
    return element

  def _thrift_to_json(self, config):
    return json.loads(
        serialize(config, protocol_factory=TJSONProtocol.TSimpleJSONProtocolFactory()))

  def _diff_configs(self, from_config, to_config):
    # Thrift objects do not correctly compare against each other due to the unhashable nature
    # of python sets. That results in occasional diff failures with the following symptoms:
    # - Sets are not equal even though their reprs are identical;
    # - Items are reordered within thrift structs;
    # - Items are reordered within sets;
    # To overcome all the above, thrift objects are converted into JSON dicts to flatten out
    # thrift type hierarchy. Next, JSONs are recursively converted into nested tuples to
    # ensure proper ordering on compare.
    return ''.join(unified_diff(repr(self._hashable(self._thrift_to_json(from_config))),
                                repr(self._hashable(self._thrift_to_json(to_config)))))

  def _create_kill_add_lists(self, instance_ids, operation_configs):
    """Determines a particular action (kill or add) to use for every instance in instance_ids.

    Arguments:
    instance_ids -- current batch of IDs to process.
    operation_configs -- OperationConfigs with update details.

    Returns lists of instances to kill and to add.
    """
    to_kill = []
    to_add = []
    for instance_id in instance_ids:
      from_config = operation_configs.from_config.get(instance_id)
      to_config = operation_configs.to_config.get(instance_id)

      if from_config and to_config:
        diff_output = self._diff_configs(from_config, to_config)
        if diff_output:
          log.debug('Task configuration changed for instance [%s]:\n%s' % (instance_id, diff_output))
          to_kill.append(instance_id)
          to_add.append(instance_id)
      elif from_config and not to_config:
        to_kill.append(instance_id)
      elif not from_config and to_config:
        to_add.append(instance_id)
      else:
        raise self.Error('Instance %s is outside of supported range' % instance_id)

    return to_kill, to_add

  def _update_instances(self, instance_ids, operation_configs):
    """Applies kill/add actions for the specified batch instances.

    Arguments:
    instance_ids -- current batch of IDs to process.
    operation_configs -- OperationConfigs with update details.

    Returns a list of added instances.
    """
    log.info('Examining instances: %s' % instance_ids)

    to_kill, to_add = self._create_kill_add_lists(instance_ids, operation_configs)

    unchanged = list(set(instance_ids) - set(to_kill + to_add))
    if unchanged:
      log.info('Skipping unchanged instances: %s' % unchanged)

    # Kill is a blocking call in scheduler -> no need to watch it yet.
    self._kill_instances(to_kill)
    self._add_instances(to_add, operation_configs.to_config)
    return to_add

  def _kill_instances(self, instance_ids):
    """Instructs the scheduler to kill instances and waits for completion.

    Arguments:
    instance_ids -- list of IDs to kill.
    """
    if instance_ids:
      log.info('Killing instances: %s' % instance_ids)
      query = self._create_task_query(instanceIds=frozenset(int(s) for s in instance_ids))
      self._check_and_log_response(self._scheduler.killTasks(query, self._lock))
      log.info('Instances killed')

  def _add_instances(self, instance_ids, to_config):
    """Instructs the scheduler to add instances.

    Arguments:
    instance_ids -- list of IDs to add.
    to_config -- OperationConfigs with update details.
    """
    if instance_ids:
      log.info('Adding instances: %s' % instance_ids)
      add_config = AddInstancesConfig(
          key=self._job_key,
          taskConfig=to_config[instance_ids[0]],  # instance_ids will always have at least 1 item.
          instanceIds=frozenset(int(s) for s in instance_ids))
      self._check_and_log_response(self._scheduler.addInstances(add_config, self._lock))
      log.info('Instances added')

  def _restart_instances(self, instance_ids):
    """Instructs the scheduler to restart instances.

    Arguments:
    instance_ids -- set of instances to be restarted by the scheduler.
    """
    log.info('Restarting instances: %s' % instance_ids)
    resp = self._scheduler.restartShards(self._job_key, instance_ids, self._lock)
    self._check_and_log_response(resp)
    return instance_ids

  def _validate_quota(self, instance_configs):
    """Validates job update will not exceed quota for production tasks.
    Arguments:
    instance_configs -- InstanceConfig with update details.

    Returns Response.OK if quota check was successful.
    """
    instance_operation = self.OperationConfigs(
      from_config=instance_configs.remote_config_map,
      to_config=instance_configs.local_config_map
    )

    def _aggregate_quota(ops_list, config_map):
      return sum(CapacityRequest.from_task(config_map[instance])
                    for instance in ops_list) or CapacityRequest()

    to_kill, to_add = self._create_kill_add_lists(
        instance_configs.instances_to_process,
        instance_operation)

    return self._quota_check.validate_quota_from_requested(
        self._job_key,
        self._config.job().taskConfig.production,
        _aggregate_quota(to_kill, instance_operation.from_config),
        _aggregate_quota(to_add, instance_operation.to_config))

  def _get_update_instructions(self, instances=None):
    """Loads, validates and populates update working set.

    Arguments:
    instances -- (optional) set of instances to update.

    Returns:
    InstanceConfigs with the following data:
      remote_config_map -- dictionary of {key:instance_id, value:task_config} from scheduler.
      local_config_map  -- dictionary of {key:instance_id, value:task_config} with local
                           task configs validated and populated with default values.
      instances_to_process -- list of instance IDs to go over in update.
    """
    # Load existing tasks and populate remote config map and instance list.
    assigned_tasks = self._get_existing_tasks()
    remote_config_map = {}
    remote_instances = []
    for assigned_task in assigned_tasks:
      remote_config_map[assigned_task.instanceId] = assigned_task.task
      remote_instances.append(assigned_task.instanceId)

    # Validate local job config and populate local task config.
    local_task_config = self._validate_and_populate_local_config()

    # Union of local and remote instance IDs.
    job_config_instances = list(range(self._config.instances()))
    instance_superset = sorted(list(set(remote_instances) | set(job_config_instances)))

    # Calculate the update working set.
    if instances is None:
      # Full job update -> union of remote and local instances
      instances_to_process = instance_superset
    else:
      # Partial job update -> validate all instances are recognized
      instances_to_process = instances
      unrecognized = list(set(instances) - set(instance_superset))
      if unrecognized:
        raise self.Error('Instances %s are outside of supported range' % unrecognized)

    # Populate local config map
    local_config_map = dict.fromkeys(job_config_instances, local_task_config)

    return self.InstanceConfigs(remote_config_map, local_config_map, instances_to_process)

  def _get_existing_tasks(self):
    """Loads all existing tasks from the scheduler.

    Returns a list of AssignedTasks.
    """
    resp = self._scheduler.getTasksStatus(self._create_task_query())
    self._check_and_log_response(resp)
    return [t.assignedTask for t in resp.result.scheduleStatusResult.tasks]

  def _validate_and_populate_local_config(self):
    """Validates local job configuration and populates local task config with default values.

    Returns a TaskConfig populated with default values.
    """
    resp = self._scheduler.populateJobConfig(self._config.job())
    self._check_and_log_response(resp)

    # Safe to take the first element as Scheduler would throw in case zero instances provided.
    return list(resp.result.populateJobResult.populated)[0]

  def _replace_template_if_cron(self):
    """Checks if the provided job config represents a cron job and if so, replaces it.

    Returns True if job is cron and False otherwise.
    """
    if self._config.job().cronSchedule:
      resp = self._scheduler.replaceCronTemplate(self._config.job(), self._lock)
      self._check_and_log_response(resp)
      return True
    else:
      return False

  def _create_task_query(self, instanceIds=None):
    return TaskQuery(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.environment,
        jobName=self._job_key.name,
        statuses=ACTIVE_STATES,
        instanceIds=instanceIds)

  def _failed_response(self, message):
    return Response(responseCode=ResponseCode.ERROR, message=message)

  def update(self, instances=None):
    """Performs the job update, blocking until it completes.
    A rollback will be performed if the update was considered a failure based on the
    update configuration.

    Arguments:
    instances -- (optional) instances to update. If not specified, all instances will be updated.

    Returns a response object with update result status.
    """
    resp = self._start()
    if resp.responseCode != ResponseCode.OK:
      return resp

    try:
      # Handle cron jobs separately from other jobs.
      if self._replace_template_if_cron():
        log.info('Cron template updated, next run will reflect changes')
        return self._finish()
      else:
        try:
          instance_configs = self._get_update_instructions(instances)
          self._check_and_log_response(self._validate_quota(instance_configs))
        except self.Error as e:
          # Safe to release the lock acquired above as no job mutation has happened yet.
          self._finish()
          return self._failed_response('Unable to start job update: %s' % e)

        if not self._update(instance_configs):
          log.warn('Update failures threshold reached')
          self._finish()
          return self._failed_response('Update reverted')
        else:
          log.info('Update successful')
          return self._finish()
    except self.Error as e:
      return self._failed_response('Aborting update without rollback! Fatal error: %s' % e)

  @classmethod
  def cancel_update(cls, scheduler, job_key):
    """Cancels an update process by removing an exclusive lock on a provided job.

    Arguments:
    scheduler -- scheduler instance to use.
    job_key -- job key to cancel update for.

    Returns a response object with cancel update result status.
    """
    return scheduler.releaseLock(
        Lock(key=LockKey(job=job_key.to_thrift())),
        LockValidation.UNCHECKED)

  def _check_and_log_response(self, resp):
    """Checks scheduler return status, raises Error in case of unexpected response.

    Arguments:
    resp -- scheduler response object.

    Raises Error in case of unexpected response status.
    """
    name, message = ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message
    if resp.responseCode == ResponseCode.OK:
      log.debug('Response from scheduler: %s (message: %s)' % (name, message))
    else:
      raise self.Error(message)
