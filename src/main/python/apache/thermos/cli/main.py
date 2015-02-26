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
