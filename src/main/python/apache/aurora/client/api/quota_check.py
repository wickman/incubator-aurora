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

import operator

from copy import deepcopy

from gen.apache.aurora.api.ttypes import ResourceAggregate, Response, ResponseCode

from twitter.common import log


class CapacityRequest(object):
  """Facilitates Quota manipulations."""

  @classmethod
  def from_task(cls, task):
    return cls(ResourceAggregate(numCpus=task.numCpus, ramMb=task.ramMb, diskMb=task.diskMb))

  def __init__(self, quota=None):
    self._quota = quota or ResourceAggregate(numCpus=0.0, ramMb=0, diskMb=0)

  def __add__(self, other):
    return self._op(operator.__add__, other)

  def __radd__(self, other):
    return self._op(operator.__add__, other)

  def __sub__(self, other):
    return self._op(operator.__sub__, other)

  def __eq__(self, other):
    return self._quota == other._quota

  def _op(self, op, other):
    if not isinstance(other, CapacityRequest):
      return self

    return CapacityRequest(
        ResourceAggregate(numCpus=op(self._quota.numCpus, other._quota.numCpus),
              ramMb=op(self._quota.ramMb, other._quota.ramMb),
              diskMb=op(self._quota.diskMb, other._quota.diskMb)))

  def valid(self):
    return self._quota.numCpus >= 0.0 and self._quota.ramMb >= 0 and self._quota.diskMb >= 0

  def quota(self):
    return deepcopy(self._quota)


class QuotaCheck(object):
  """Performs quota checks for the provided job/task configurations."""

  def __init__(self, scheduler):
    self._scheduler = scheduler

  def validate_quota_from_requested(self, job_key, production, released, acquired):
    """Validates requested change will not exceed the available quota.

    Arguments:
    job_key -- job key.
    production -- production flag.
    released -- CapacityRequest to be released (in case of job update).
    acquired -- CapacityRequest to be acquired.

    Returns: ResponseCode.OK if check is successful.
    """
    resp_ok = Response(responseCode=ResponseCode.OK, message='Quota check successful.')
    if not production:
      return resp_ok

    resp = self._scheduler.getQuota(job_key.role)
    if resp.responseCode != ResponseCode.OK:
      log.error('Failed to get quota from scheduler: %s' % resp.message)
      return resp

    allocated = CapacityRequest(resp.result.getQuotaResult.quota)
    consumed = CapacityRequest(resp.result.getQuotaResult.prodConsumption)
    requested = acquired - released
    effective = allocated - consumed - requested

    if not effective.valid():
      log.info('Not enough quota to create/update job.')
      print_quota(allocated.quota(), 'Total allocated quota', job_key.role)
      print_quota(consumed.quota(), 'Consumed quota', job_key.role)
      print_quota(requested.quota(), 'Requested', job_key.name)
      return Response(responseCode=ResponseCode.INVALID_REQUEST, message='Failed quota check.')

    return resp_ok


def print_quota(quota, msg, subj):
  quota_fields = [
      ('CPU', quota.numCpus),
      ('RAM', '%f GB' % (float(quota.ramMb) / 1024)),
      ('Disk', '%f GB' % (float(quota.diskMb) / 1024))
  ]
  log.info('%s for %s:\n\t%s' %
           (msg, subj, '\n\t'.join(['%s\t%s' % (k, v) for (k, v) in quota_fields])))

