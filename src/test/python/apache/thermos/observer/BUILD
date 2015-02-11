python_test_suite(
  name = 'all',
  dependencies = [
    ':test_database',
    ':test_detector',
  ]
)

python_tests(
  name = 'test_database',
  sources = ['test_database.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/thermos/observer:database',
  ]
)

python_tests(
  name = 'test_detector',
  sources = ['test_detector.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/thermos/observer:detector',
  ]
)
