import mock
import pytest
from bottle import HTTPError

from apache.thermos.observer.database import TaskObserverDatabaseInterface
from apache.thermos.observer.http.json_observer import TaskObserverJSONBindings, thrift_to_json

from gen.apache.thermos.ttypes import RunnerState


def test_handle_task_ids():
  database = mock.create_autospec(TaskObserverDatabaseInterface, spec_set=True)
  database.get_all_tasks.return_value = ['task1', 'task2']
  database.get_active_tasks.return_value = ['task1']
  database.get_finished_tasks.return_value = ['task2']
  tob = TaskObserverJSONBindings(database)

  assert tob.handle_task_ids() == ['task1', 'task2']
  database.get_all_tasks.assert_called_once_with()
  assert database.get_active_tasks.call_count == 0
  assert database.get_finished_tasks.call_count == 0

  database.reset_mock()

  assert tob.handle_task_ids(which='active') == ['task1']
  database.get_active_tasks.assert_called_once_with()
  assert database.get_all_tasks.call_count == 0
  assert database.get_finished_tasks.call_count == 0

  database.reset_mock()

  assert tob.handle_task_ids(which='finished') == ['task2']
  database.get_finished_tasks.assert_called_once_with()
  assert database.get_all_tasks.call_count == 0
  assert database.get_active_tasks.call_count == 0

  database.reset_mock()


def test_handle_task_ids_errors():
  database = mock.create_autospec(TaskObserverDatabaseInterface, spec_set=True)
  tob = TaskObserverJSONBindings(database)

  with pytest.raises(HTTPError):
    tob.handle_task_ids(which='horf')

  with pytest.raises(HTTPError):
    tob.handle_task_ids(offset='gorf')

  with pytest.raises(HTTPError):
    tob.handle_task_ids(num='lorp')


def test_handle_task():
  database = mock.create_autospec(TaskObserverDatabaseInterface, spec_set=True)
  database.get_state.return_value = {'task1': RunnerState(processes={})}
  tob = TaskObserverJSONBindings(database)

  assert tob.handle_task('task1') == thrift_to_json(RunnerState(processes={}))
  database.get_state.assert_called_once_with('task1')

  with pytest.raises(HTTPError):
    assert tob.handle_task('task2')


def test_handle_task_driver():
  database = mock.create_autospec(TaskObserverDatabaseInterface, spec_set=True)
  database.get_state.return_value = {'task1': RunnerState(processes={})}
  tob = TaskObserverJSONBindings(database)

  assert tob._tasks(task_ids=['task1', 'task2']) == {'task1': RunnerState(processes={})}
