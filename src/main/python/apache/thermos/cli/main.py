from twitter.common import app

from apache.thermos.monitoring.detector import FixedPathDetector

from .common import register_path_detector


def register_commands(app):
  from apache.thermos.cli.common import generate_usage
  from apache.thermos.cli.commands import (
      gc as gc_command,
      help as help_command,
      inspect as inspect_command,
      kill as kill_command,
      read as read_command,
      run as run_command,
      simplerun as simplerun_command,
      status as status_command,
      tail as tail_command,
  )

  app.register_commands_from(
      gc_command,
      help_command,
      inspect_command,
      kill_command,
      read_command,
      run_command,
      simplerun_command,
      status_command,
      tail_command,
  )

  generate_usage()


def register_root(option, opt, value, parser):
  print('Registering new root: %s' % value)
  register_path_detector(FixedPathDetector(value))


def register_options(app):
  from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT

  print('Registering --root option.')

  app.add_option(
      '--root',
      dest='root',
      metavar='PATH',
      type='string',
      default=DEFAULT_CHECKPOINT_ROOT,
      action='callback',
      callback=register_root,
      help="the thermos config root")


register_commands(app)
register_options(app)


proxy_main = app.main


proxy_main()
