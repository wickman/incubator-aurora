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

import functools
import threading
import time

from apache.aurora.common.auth import make_session_key, SessionKeyError
from apache.aurora.common.cluster import Cluster

from gen.apache.aurora import AuroraAdmin
from gen.apache.aurora.constants import CURRENT_API_VERSION

from pystachio import Boolean, Default, Integer, String
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.rpc.transports.tsslsocket import DelayedHandshakeTSSLSocket
from twitter.common.zookeeper.kazoo_client import TwitterKazooClient
from twitter.common.zookeeper.serverset import ServerSet


class SchedulerClientTrait(Cluster.Trait):
  zk                = String
  zk_port           = Default(Integer, 2181)
  scheduler_zk_path = String
  scheduler_uri     = String
  proxy_url         = String
  auth_mechanism    = Default(String, 'UNAUTHENTICATED')
  use_thrift_ssl    = Default(Boolean, False)


class SchedulerClient(object):
  THRIFT_RETRIES = 5
  RETRY_TIMEOUT = Amount(1, Time.SECONDS)

  class CouldNotConnect(Exception): pass

  # TODO(wickman) Refactor per MESOS-3005 into two separate classes with separate traits:
  #   ZookeeperClientTrait
  #   DirectClientTrait
  @classmethod
  def get(cls, cluster, **kwargs):
    if not isinstance(cluster, Cluster):
      raise TypeError('"cluster" must be an instance of Cluster, got %s' % type(cluster))
    cluster = cluster.with_trait(SchedulerClientTrait)
    if cluster.zk:
      return ZookeeperSchedulerClient(
          cluster, port=cluster.zk_port, ssl=cluster.use_thrift_ssl, **kwargs)
    elif cluster.scheduler_uri:
      try:
        host, port = cluster.scheduler_uri.split(':', 2)
        port = int(port)
      except ValueError:
        raise ValueError('Malformed Cluster scheduler_uri: %s' % cluster.scheduler_uri)
      return DirectSchedulerClient(host, port, ssl=cluster.use_thrift_ssl)
    else:
      raise ValueError('"cluster" does not specify zk or scheduler_uri')

  def __init__(self, verbose=False, ssl=False):
    self._client = None
    self._verbose = verbose
    self._ssl = ssl

  def get_thrift_client(self):
    if self._client is None:
      self._client = self._connect()
    return self._client

  # per-class implementation -- mostly meant to set up a valid host/port
  # pair and then delegate the opening to SchedulerClient._connect_scheduler
  def _connect(self):
    return None

  @staticmethod
  def _connect_scheduler(host, port, with_ssl=False):
    if with_ssl:
      socket = DelayedHandshakeTSSLSocket(host, port, delay_handshake=True, validate=False)
    else:
      socket = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    schedulerClient = AuroraAdmin.Client(protocol)
    for _ in range(SchedulerClient.THRIFT_RETRIES):
      try:
        transport.open()
        return schedulerClient
      except TTransport.TTransportException:
        time.sleep(SchedulerClient.RETRY_TIMEOUT.as_(Time.SECONDS))
        continue
      except Exception as e:
        # Monkey-patched proxies, like socks, can generate a proxy error here.
        # without adding a dependency, we can't catch those in a more specific way.
        raise SchedulerClient.CouldNotConnect('Connection to scheduler failed: %s' % e)
    raise SchedulerClient.CouldNotConnect('Could not connect to %s:%s' % (host, port))


class ZookeeperSchedulerClient(SchedulerClient):
  SERVERSET_TIMEOUT = Amount(10, Time.SECONDS)

  @classmethod
  def get_scheduler_serverset(cls, cluster, port=2181, verbose=False, **kw):
    if cluster.zk is None:
      raise ValueError('Cluster has no associated zookeeper ensemble!')
    if cluster.scheduler_zk_path is None:
      raise ValueError('Cluster has no defined scheduler path, must specify scheduler_zk_path '
                       'in your cluster config!')
    zk = TwitterKazooClient.make(str('%s:%s' % (cluster.zk, port)), verbose=verbose)
    return zk, ServerSet(zk, cluster.scheduler_zk_path, **kw)

  def __init__(self, cluster, port=2181, ssl=False, verbose=False):
    SchedulerClient.__init__(self, verbose=verbose, ssl=ssl)
    self._cluster = cluster
    self._zkport = port
    self._endpoint = None
    self._http = None

  def _connect(self):
    joined = threading.Event()
    def on_join(elements):
      joined.set()
    zk, serverset = self.get_scheduler_serverset(self._cluster, verbose=self._verbose,
        port=self._zkport, on_join=on_join)
    joined.wait(timeout=self.SERVERSET_TIMEOUT.as_(Time.SECONDS))
    serverset_endpoints = list(serverset)
    if len(serverset_endpoints) == 0:
      raise self.CouldNotConnect('No schedulers detected in %s!' % self._cluster.name)
    instance = serverset_endpoints[0]
    self._endpoint = instance.service_endpoint
    self._http = instance.additional_endpoints.get('http')
    zk.stop()
    return self._connect_scheduler(self._endpoint.host, self._endpoint.port, self._ssl)

  @property
  def url(self):
    proxy_url = self._cluster.proxy_url
    if proxy_url:
      return proxy_url
    if self._http is None:
      self._connect()
    if self._http:
      return 'http://%s:%s' % (self._http.host, self._http.port)


class DirectSchedulerClient(SchedulerClient):
  def __init__(self, host, port, ssl=False):
    SchedulerClient.__init__(self, verbose=True, ssl=ssl)
    self._host = host
    self._port = port

  def _connect(self):
    return self._connect_scheduler(self._host, self._port, with_ssl=self._ssl)

  @property
  def url(self):
    # TODO(wickman) This is broken -- make this tunable in MESOS-3005
    return 'http://%s:8081' % self._host


class SchedulerProxy(object):
  """
    This class is responsible for creating a reliable thrift client to the
    twitter scheduler.  Basically all the dirty work needed by the
    AuroraClientAPI.
  """
  CONNECT_MAXIMUM_WAIT = Amount(1, Time.MINUTES)
  RPC_RETRY_INTERVAL = Amount(5, Time.SECONDS)
  RPC_MAXIMUM_WAIT = Amount(10, Time.MINUTES)
  UNAUTHENTICATED_RPCS = frozenset([
    'populateJobConfig',
    'getTasksStatus',
    'getJobs',
    'getQuota',
    'getVersion',
  ])

  class Error(Exception): pass
  class TimeoutError(Error): pass
  class AuthenticationError(Error): pass
  class APIVersionError(Error): pass

  def __init__(self, cluster, verbose=False, session_key_factory=make_session_key):
    """A callable session_key_factory should be provided for authentication"""
    self.cluster = cluster
    # TODO(Sathya): Make this a part of cluster trait when authentication is pushed to the transport
    # layer.
    self._session_key_factory = session_key_factory
    self._client = self._scheduler_client = None
    self.verbose = verbose

  def with_scheduler(method):
    """Decorator magic to make sure a connection is made to the scheduler"""
    def _wrapper(self, *args, **kwargs):
      if not self._scheduler_client:
        self._construct_scheduler()
      return method(self, *args, **kwargs)
    return _wrapper

  def invalidate(self):
    self._client = self._scheduler_client = None

  @with_scheduler
  def client(self):
    return self._client

  @with_scheduler
  def scheduler_client(self):
    return self._scheduler_client

  def session_key(self):
    try:
      return self._session_key_factory(self.cluster.auth_mechanism)
    except SessionKeyError as e:
      raise self.AuthenticationError('Unable to create session key %s' % e)

  def _construct_scheduler(self):
    """
      Populates:
        self._scheduler_client
        self._client
    """
    self._scheduler_client = SchedulerClient.get(self.cluster, verbose=self.verbose)
    assert self._scheduler_client, "Could not find scheduler (cluster = %s)" % self.cluster.name
    start = time.time()
    while (time.time() - start) < self.CONNECT_MAXIMUM_WAIT.as_(Time.SECONDS):
      try:
        self._client = self._scheduler_client.get_thrift_client()
        break
      except SchedulerClient.CouldNotConnect as e:
        log.warning('Could not connect to scheduler: %s' % e)
    if not self._client:
      raise self.TimeoutError('Timed out trying to connect to scheduler at %s' % self.cluster.name)

    server_version = self._client.getVersion().result.getVersionResult
    if server_version != CURRENT_API_VERSION:
      raise self.APIVersionError("Client Version: %s, Server Version: %s" %
                                 (CURRENT_API_VERSION, server_version))

  def __getattr__(self, method_name):
    # If the method does not exist, getattr will return AttributeError for us.
    method = getattr(AuroraAdmin.Client, method_name)
    if not callable(method):
      return method

    @functools.wraps(method)
    def method_wrapper(*args):
      start = time.time()
      while (time.time() - start) < self.RPC_MAXIMUM_WAIT.as_(Time.SECONDS):
        auth_args = () if method_name in self.UNAUTHENTICATED_RPCS else (self.session_key(),)
        try:
          method = getattr(self.client(), method_name)
          if not callable(method):
            return method
          return method(*(args + auth_args))
        except (TTransport.TTransportException, self.TimeoutError) as e:
          log.warning('Connection error with scheduler: %s, reconnecting...' % e)
          self.invalidate()
          time.sleep(self.RPC_RETRY_INTERVAL.as_(Time.SECONDS))
      raise self.TimeoutError('Timed out attempting to issue %s to %s' % (
          method_name, self.cluster.name))

    return method_wrapper
