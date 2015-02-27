python_library(
  name = 'common',
  sources = ['common.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/thermos/common:constants',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/core:runner',
    'src/main/python/apache/thermos/monitoring:detector',
  ]
)

python_library(
  name = 'main',
  sources = ['main.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    ':common',
    'src/main/python/apache/thermos/cli/commands',
    'src/main/python/apache/thermos/common:constants',
    'src/main/python/apache/thermos/monitoring:detector',
  ]
)

python_binary(
  name = 'thermos',
  entry_point = 'apache.thermos.cli.main:proxy_main',
  dependencies = [
    ':main',
  ]
)