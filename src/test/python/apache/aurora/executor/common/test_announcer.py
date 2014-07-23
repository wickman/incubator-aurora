import threading

import mock
from kazoo.exceptions import KazooException
from twitter.common.quantity import Amount, Time
from twitter.common.testing.clock import ThreadedClock
from twitter.common.zookeeper.serverset import Endpoint, ServerSet

from apache.aurora.executor.common.announcer import Announcer, ServerSetJoinThread


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

  joined.wait(timeout=1.0)
  assert joined.is_set()

  assert announcer.disconnected_time() == 0.0
  clock.tick(1.0)
  assert announcer.disconnected_time() == 0.0, (
      'Announcer should not advance disconnection time when connected.')
  assert announcer._membership == 'membership foo'

  announcer.stop()

  mock_serverset.cancel.assert_called_with('membership foo')

  assert announcer.disconnected_time() == 0.0
  clock.tick(1.0)
  assert announcer.disconnected_time() == 0.0, (
      'Announcer should not advance disconnection time when stopped.')


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
