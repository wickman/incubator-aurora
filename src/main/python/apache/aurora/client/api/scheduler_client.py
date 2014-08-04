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
import traceback

from pystachio import Default, Integer, String
from thrift.protocol import TJSONProtocol
from thrift.transport import THttpClient, TTransport
from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.kazoo_client import TwitterKazooClient
from twitter.common.zookeeper.serverset import ServerSet

from apache.aurora.common.auth import make_session_key, SessionKeyError
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.transport import TRequestsTransport

from gen.apache.aurora.api import AuroraAdmin, ReadOnlyScheduler
from gen.apache.aurora.api.constants import CURRENT_API_VERSION

try:
  from urlparse import urljoin
except ImportError:
  from urllib.parse import urljoin


class SchedulerClientTrait(Cluster.Trait):
  zk                = String  # noqa
  zk_port           = Default(Integer, 2181)  # noqa
  scheduler_zk_path = String  # noqa
  scheduler_uri     = String  # noqa
  proxy_url         = String  # noqa
  auth_mechanism    = Default(String, 'UNAUTHENTICATED')  # noqa


class SchedulerClient(object):
  THRIFT_RETRIES = 5
  RETRY_TIMEOUT = Amount(1, Time.SECONDS)

  class Error(Exception): pass
  class CouldNotConnect(Error): pass

  # TODO(wickman) Refactor per MESOS-3005 into two separate classes with separate traits:
  #   ZookeeperClientTrait
  #   DirectClientTrait
  @classmethod
  def get(cls, cluster, **kwargs):
    if not isinstance(cluster, Cluster):
      raise TypeError('"cluster" must be an instance of Cluster, got %s' % type(cluster))
    cluster = cluster.with_trait(SchedulerClientTrait)
    if cluster.zk:
      return ZookeeperSchedulerClient(cluster, port=cluster.zk_port, **kwargs)
    elif cluster.scheduler_uri:
      return DirectSchedulerClient(cluster.scheduler_uri)
    else:
      raise ValueError('"cluster" does not specify zk or scheduler_uri')

  def __init__(self, verbose=False):
    self._client = None
    self._verbose = verbose

  def get_thrift_client(self):
    if self._client is None:
      self._client = self._connect()
    return self._client

  # per-class implementation -- mostly meant to set up a valid host/port
  # pair and then delegate the opening to SchedulerClient._connect_scheduler
  def _connect(self):
    return None

  @classmethod
  def _connect_scheduler(cls, uri, clock=time):
    transport = TRequestsTransport(uri)
    protocol = TJSONProtocol.TJSONProtocol(transport)
    schedulerClient = AuroraAdmin.Client(protocol)
    for _ in range(cls.THRIFT_RETRIES):
      try:
        transport.open()
        return schedulerClient
      except TTransport.TTransportException:
        clock.sleep(cls.RETRY_TIMEOUT.as_(Time.SECONDS))
        continue
      except Exception as e:
        # Monkey-patched proxies, like socks, can generate a proxy error here.
        # without adding a dependency, we can't catch those in a more specific way.
        raise cls.CouldNotConnect('Connection to scheduler failed: %s' % e)
    raise cls.CouldNotConnect('Could not connect to %s' % uri)


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

  def __init__(self, cluster, port=2181, verbose=False):
    SchedulerClient.__init__(self, verbose=verbose)
    self._cluster = cluster
    self._zkport = port
    self._endpoint = None
    self._uri = None

  def _resolve(self):
    """Resolve the uri associated with this scheduler from zookeeper."""
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
    if 'https' in instance.additional_endpoints:
      endpoint = instance.additional_endpoints['https']
      self._uri = 'https://%s:%s' % (endpoint.host, endpoint.port)
    elif 'http' in instance.additional_endpoints:
      endpoint = instance.additional_endpoints['http']
      self._uri = 'http://%s:%s' % (endpoint.host, endpoint.port)
    zk.stop()

  def _connect(self):
    if self._uri is None:
      self._resolve()
    if self._uri is not None:
      return self._connect_scheduler(urljoin(self._uri, 'api'))

  @property
  def url(self):
    proxy_url = self._cluster.proxy_url
    if proxy_url:
      return proxy_url
    if self._uri is None:
      self._resolve()
    if self._uri:
      return self._uri


class DirectSchedulerClient(SchedulerClient):
  def __init__(self, uri):
    SchedulerClient.__init__(self, verbose=True)
    self._uri = uri

  def _connect(self):
    return self._connect_scheduler(urljoin(self._uri, 'api'))

  @property
  def url(self):
    return self._uri


class SchedulerProxy(object):
  """
    This class is responsible for creating a reliable thrift client to the
    twitter scheduler.  Basically all the dirty work needed by the
    AuroraClientAPI.
  """
  CONNECT_MAXIMUM_WAIT = Amount(1, Time.MINUTES)
  RPC_RETRY_INTERVAL = Amount(5, Time.SECONDS)
  RPC_MAXIMUM_WAIT = Amount(10, Time.MINUTES)

  class Error(Exception): pass
  class TimeoutError(Error): pass
  class AuthenticationError(Error): pass
  class APIVersionError(Error): pass
  class ThriftInternalError(Error): pass

  def __init__(self, cluster, verbose=False, session_key_factory=make_session_key):
    """A callable session_key_factory should be provided for authentication"""
    self.cluster = cluster
    # TODO(Sathya): Make this a part of cluster trait when authentication is pushed to the transport
    # layer.
    self._session_key_factory = session_key_factory
    self._client = self._scheduler_client = None
    self.verbose = verbose
    self._lock = threading.RLock()
    self._terminating = threading.Event()

  def with_scheduler(method):
    """Decorator magic to make sure a connection is made to the scheduler"""
    def _wrapper(self, *args, **kwargs):
      if not self._scheduler_client:
        self._construct_scheduler()
      return method(self, *args, **kwargs)
    return _wrapper

  def invalidate(self):
    self._client = self._scheduler_client = None

  def terminate(self):
    """Requests immediate termination of any retry attempts and invalidates client."""
    self._terminating.set()
    self.invalidate()

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
        # this can wind up generating any kind of error, because it turns into
        # a call to a dynamically set authentication module.
        self._client = self._scheduler_client.get_thrift_client()
        break
      except SchedulerClient.CouldNotConnect as e:
        log.warning('Could not connect to scheduler: %s' % e)
      except Exception as e:
        # turn any auth module exception into an auth error.
        log.debug('Warning: got an unknown exception during authentication:')
        log.debug(traceback.format_exc())
        raise self.AuthenticationError('Error connecting to scheduler: %s' % e)
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
      with self._lock:
        start = time.time()
        # TODO(wfarner): The while loop causes failed unit tests to spin for the retry
        # period (currently 10 minutes).  Figure out a better approach.
        while not self._terminating.is_set() and (
            time.time() - start) < self.RPC_MAXIMUM_WAIT.as_(Time.SECONDS):

          # Only automatically append a SessionKey if this is not part of the read-only API.
          auth_args = () if hasattr(ReadOnlyScheduler.Iface, method_name) else (self.session_key(),)
          try:
            method = getattr(self.client(), method_name)
            if not callable(method):
              return method
            return method(*(args + auth_args))
          except (TTransport.TTransportException, self.TimeoutError) as e:
            if not self._terminating:
              log.warning('Connection error with scheduler: %s, reconnecting...' % e)
              self.invalidate()
              self._terminating.wait(self.RPC_RETRY_INTERVAL.as_(Time.SECONDS))
          except Exception as e:
            # Take any error that occurs during the RPC call, and transform it
            # into something clients can handle.
            if not self._terminating:
              raise self.ThriftInternalError("Error during thrift call %s to %s: %s" %
                                            (method_name, self.cluster.name, e))
        if not self._terminating:
          raise self.TimeoutError('Timed out attempting to issue %s to %s' % (
              method_name, self.cluster.name))

    return method_wrapper
