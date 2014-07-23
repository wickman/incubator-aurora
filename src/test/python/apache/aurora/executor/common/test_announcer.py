import threading

import mock
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from twitter.common.quantity import Amount, Time
from twitter.common.testing.clock import ThreadedClock
from twitter.common.zookeeper.serverset import Endpoint, ServerSet

from apache.aurora.executor.common.announcer import (
    Announcer,
    DefaultAnnouncerProvider,
    make_endpoints,
    ServerSetJoinThread
)


def test_serverset_join_thread():
  join = threading.Event()
  joined = threading.Event()

  def join_function():
    joined.set()

  ssjt = ServerSetJoinThread(join, join_function, loop_wait=Amount(1, Time.MILLISECONDS))
  ssjt.start()
  ssjt.stop()
  ssjt.join(timeout=1.0)
  assert not ssjt.is_alive()
  assert not joined.is_set()

  ssjt = ServerSetJoinThread(join, join_function, loop_wait=Amount(1, Time.MILLISECONDS))
  ssjt.start()

  def test_loop():
    join.set()
    joined.wait(timeout=1.0)
    assert not join.is_set()  # join is cleared
    assert joined.is_set()  # joined has been called
    joined.clear()

  # validate that the loop is working
  test_loop()
  test_loop()

  ssjt.stop()
  ssjt.join(timeout=1.0)
  assert not ssjt.is_alive()
  assert not joined.is_set()


def test_announcer_under_normal_circumstances():
  joined = threading.Event()

  def joined_side_effect(*args, **kw):
    joined.set()
    return 'membership foo'

  mock_serverset = mock.MagicMock(spec=ServerSet)
  mock_serverset.join = mock.MagicMock()
  mock_serverset.join.side_effect = joined_side_effect
  mock_serverset.cancel = mock.MagicMock()

  endpoint = Endpoint('localhost', 12345)
  clock = ThreadedClock(31337.0)

  announcer = Announcer(mock_serverset, endpoint, clock=clock)
  assert announcer.disconnected_time() == 0.0
  clock.tick(1.0)
  assert announcer.disconnected_time() == 1.0, (
      'Announcer should advance disconnection time when not yet initially connected.')

  announcer.start()

  try:
    joined.wait(timeout=1.0)
    assert joined.is_set()

    assert announcer.disconnected_time() == 0.0
    clock.tick(1.0)
    assert announcer.disconnected_time() == 0.0, (
        'Announcer should not advance disconnection time when connected.')
    assert announcer._membership == 'membership foo'

  finally:
    announcer.stop()

  mock_serverset.cancel.assert_called_with('membership foo')

  assert announcer.disconnected_time() == 0.0
  clock.tick(1.0)
  assert announcer.disconnected_time() == 0.0, (
      'Announcer should not advance disconnection time when stopped.')


def test_announcer_on_expiration():
  joined = threading.Event()
  operations = []

  def joined_side_effect(*args, **kw):
    # 'global' does not work within python nested functions, so we cannot use a
    # counter here, so instead we do append/len (see PEP-3104)
    operations.append(1)
    if len(operations) == 1 or len(operations) == 3:
      joined.set()
      return 'membership %d' % len(operations)
    else:
      raise KazooException('Failed to reconnect')

  mock_serverset = mock.MagicMock(spec=ServerSet)
  mock_serverset.join = mock.MagicMock()
  mock_serverset.join.side_effect = joined_side_effect
  mock_serverset.cancel = mock.MagicMock()

  endpoint = Endpoint('localhost', 12345)
  clock = ThreadedClock(31337.0)

  announcer = Announcer(
      mock_serverset, endpoint, clock=clock, exception_wait=Amount(2, Time.SECONDS))
  announcer.start()

  try:
    joined.wait(timeout=1.0)
    assert joined.is_set()
    assert announcer._membership == 'membership 1'
    assert announcer.disconnected_time() == 0.0
    clock.tick(1.0)
    assert announcer.disconnected_time() == 0.0
    announcer.on_expiration()  # expect exception
    clock.tick(1.0)
    assert announcer.disconnected_time() == 1.0, (
        'Announcer should be disconnected on expiration.')
    clock.tick(1.0)
    assert announcer.disconnected_time() == 0.0, (
        'Announcer should not advance disconnection time when connected.')
    assert announcer._membership == 'membership 3'

  finally:
    announcer.stop()


def test_announcer_under_abnormal_circumstances():
  mock_serverset = mock.MagicMock(spec=ServerSet)
  mock_serverset.join = mock.MagicMock()
  mock_serverset.join.side_effect = [
      KazooException('Whoops the ensemble is down!'),
      'member0001',
  ]
  mock_serverset.cancel = mock.MagicMock()

  endpoint = Endpoint('localhost', 12345)
  clock = ThreadedClock(31337.0)

  announcer = Announcer(
       mock_serverset, endpoint, clock=clock, exception_wait=Amount(2, Time.SECONDS))
  announcer.start()

  try:
    clock.tick(1.0)
    assert announcer.disconnected_time() == 1.0
    clock.tick(2.0)
    assert announcer.disconnected_time() == 0.0, (
        'Announcer should recover after an exception thrown internally.')
    assert announcer._membership == 'member0001'
  finally:
    announcer.stop()


def make_assigned_task(thermos_config, assigned_ports=None):
  from gen.apache.aurora.api.constants import AURORA_EXECUTOR_NAME
  from gen.apache.aurora.api.ttypes import AssignedTask, ExecutorConfig, TaskConfig

  assigned_ports = assigned_ports or {}
  executor_config = ExecutorConfig(name=AURORA_EXECUTOR_NAME, data=thermos_config.json_dumps())

  return AssignedTask(
      instanceId=12345,
      task=TaskConfig(jobName=thermos_config.name().get(), executorConfig=executor_config),
      assignedPorts=assigned_ports)


def make_job(role, environment, name, primary_port, portmap):
  from apache.aurora.config.schema.base import (
      Announcer,
      Job,
      Process,
      Resources,
      Task,
  )
  task = Task(
      name='ignore2',
      processes=[Process(name='ignore3', cmdline='ignore4')],
      resources=Resources(cpu=1, ram=1, disk=1))
  job = Job(
      role=role,
      environment=environment,
      name=name,
      cluster='ignore1',
      task=task,
      announce=Announcer(primary_port=primary_port, portmap=portmap))
  return job


def test_make_empty_endpoints():
  hostname = 'aurora.example.com'
  portmap = {}
  primary_port = 'http'

  # test no bound 'http' port
  primary, additional = make_endpoints(hostname, portmap, primary_port)
  assert primary == Endpoint(hostname, 0)
  assert additional == {}


@mock.patch('apache.aurora.executor.common.announcer.ServerSet')
def test_default_announcer_provider(mock_serverset_provider):
  mock_client = mock.MagicMock(spec=KazooClient)

  class TestDefaultAnnouncerProvider(DefaultAnnouncerProvider):
    def make_client(self):
      return mock_client

  mock_serverset = mock.MagicMock(spec=ServerSet)
  mock_serverset_provider.return_value = mock_serverset

  dap = TestDefaultAnnouncerProvider('zookeeper.example.com', root='/aurora')
  job = make_job('aurora', 'prod', 'proxy', 'primary', portmap={'http': 80, 'admin': 'primary'})
  assigned_task = make_assigned_task(job, assigned_ports={'primary': 12345})
  checker = dap.from_assigned_task(assigned_task, None)
  mock_serverset_provider.assert_called_once_with(mock_client, '/aurora/aurora/prod/proxy')
  assert checker.name() == 'announcer'
  assert checker.status is None


def test_default_announcer_provider_without_announce():
  from pystachio import Empty

  job = make_job('aurora', 'prod', 'proxy', 'primary', portmap={})
  job = job(announce=Empty)
  assigned_task = make_assigned_task(job)

  assert DefaultAnnouncerProvider('foo.bar').from_assigned_task(assigned_task, None) is None
