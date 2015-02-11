python_test_suite(
  name = 'all',
  dependencies = [
    ':file_browser',
    ':json_observer',
  ]
)

python_tests(
  name = 'file_browser',
  sources = ['test_file_browser.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/thermos/observer:database',
    'src/main/python/apache/thermos/observer/http:file_browser',
  ]
)

python_tests(
  name = 'json_observer',
  sources = ['test_json_observer.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/thermos/observer:database',
    'src/main/python/apache/thermos/observer/http:json_observer',
  ]
)
