import posixpath
import socket
import threading
import time

from twitter.common import log
from twitter.common.concurrent.deferred import defer
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import LambdaGauge, Observable
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.kazoo_client import TwitterKazooClient
from twitter.common.zookeeper.serverset import Endpoint, ServerSet

from apache.aurora.executor.common.status_checker import StatusChecker, StatusCheckerProvider
from apache.aurora.executor.common.task_info import (
    mesos_task_instance_from_assigned_task,
    resolve_ports
)


def make_endpoints(hostname, portmap, primary_port):
  """
    Generate primary, additional endpoints from a portmap and primary_port.
    primary_port must be a name in the portmap dictionary.
  """
  # Do int check as stop-gap measure against incompatible downstream clients.
  additional_endpoints = dict(
      (name, Endpoint(hostname, port)) for (name, port) in portmap.items()
      if isinstance(port, int))

  # It's possible for the primary port to not have been allocated if this task
  # is using autoregistration, so register with a port of 0.
  return Endpoint(hostname, portmap.get(primary_port, 0)), additional_endpoints


class DefaultAnnouncerProvider(StatusCheckerProvider):
  def __init__(self, ensemble, root='/aurora'):
    self.__ensemble = ensemble
    self.__root = root

  def make_client(self):
    return TwitterKazooClient.make(self.__ensemble)

  def from_assigned_task(self, assigned_task, _):
    mesos_task = mesos_task_instance_from_assigned_task(assigned_task)

    if not mesos_task.has_announce():
      return None

    portmap = resolve_ports(mesos_task, assigned_task.assignedPorts)

    endpoint, additional = make_endpoints(
        socket.gethostname(),
        portmap,
        mesos_task.announce().primary_port().get())

    path = posixpath.join(
        self.__root,
        mesos_task.role().get(),
        mesos_task.environment().get(),
        assigned_task.task.jobName)

    zookeeper = self.make_client()
    serverset = ServerSet(zookeeper, path)

    checker = AnnouncerChecker(
        serverset, endpoint, additional=additional, shard=assigned_task.instanceId)

    if isinstance(zookeeper, Observable):
      checker.metrics.register_observable('ensemble', zookeeper)

    return checker


class ServerSetJoinThread(ExceptionalThread):
  """Background thread to reconnect to Serverset on session expiration."""

  LOOP_WAIT = Amount(1, Time.SECONDS)

  def __init__(self, event, joiner, loop_wait=LOOP_WAIT):
    self._event = event
    self._joiner = joiner
    self._stopped = threading.Event()
    self._loop_wait = loop_wait
    super(ServerSetJoinThread, self).__init__()
    self.daemon = True

  def run(self):
    while True:
      if self._stopped.is_set():
        break
      self._event.wait(timeout=self._loop_wait.as_(Time.SECONDS))
      if not self._event.is_set():
        continue
      log.debug('Join event triggered, joining serverset.')
      self._event.clear()
      self._joiner()

  def stop(self):
    self._stopped.set()


class Announcer(Observable):
  class Error(Exception): pass

  EXCEPTION_WAIT = Amount(15, Time.SECONDS)

  def __init__(self,
               serverset,
               endpoint,
               additional=None,
               shard=None,
               clock=time,
               exception_wait=EXCEPTION_WAIT):
    self._membership = None
    self._membership_termination = clock.time()
    self._endpoint = endpoint
    self._additional = additional or {}
    self._shard = shard
    self._serverset = serverset
    self._rejoin_event = threading.Event()
    self._clock = clock
    self._thread = None
    self._exception_wait = exception_wait

  def disconnected_time(self):
    # Lockless membership length check
    membership_termination = self._membership_termination
    if membership_termination is None:
      return 0
    return self._clock.time() - membership_termination

  def _join_inner(self):
    return self._serverset.join(
        endpoint=self._endpoint,
        additional=self._additional,
        shard=self._shard,
        expire_callback=self.on_expiration)

  def _join(self):
    if self._membership is not None:
      raise self.Error("join called, but already have membership!")
    while True:
      try:
        self._membership = self._join_inner()
        self._membership_termination = None
      except Exception as e:
        log.error('Failed to join ServerSet: %s' % e)
        self._clock.sleep(self._exception_wait.as_(Time.SECONDS))
      else:
        break

  def start(self):
    self._thread = ServerSetJoinThread(self._rejoin_event, self._join)
    self._thread.start()
    self.rejoin()

  def rejoin(self):
    self._rejoin_event.set()

  def stop(self):
    thread, self._thread = self._thread, None
    thread.stop()
    if self._membership:
      self._serverset.cancel(self._membership)

  def on_expiration(self):
    self._membership = None
    if not self._thread:
      return
    self._membership_termination = self._clock.time()
    log.info('TwitterService session expired.')
    self.rejoin()


class AnnouncerChecker(StatusChecker):
  def __init__(self, serverset, endpoint, additional=None, shard=None):
    self._announcer = Announcer(serverset, endpoint, additional=additional, shard=shard)
    self.metrics.register(LambdaGauge('disconnected_time', self._announcer.disconnected_time))

  @property
  def status(self):
    return None  # always return healthy

  def name(self):
    return 'announcer'

  def start(self):
    self._announcer.start()

  def stop(self):
    defer(self._announcer.stop)
