import threading

import mock
from twitter.common.quantity import Amount, Time
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


def test_announcer():
  mock_serverset = mock.MagicMock(spec=ServerSet)
  mock_serverset.join = mock.MagicMock()
  mock_serverset.join.return_value = 'membership foo'
  mock_serverset.cancel = mock.MagicMock()

  endpoint = Endpoint('localhost', 12345)

  announcer = Announcer(mock_serverset, endpoint)
  announcer.start()
