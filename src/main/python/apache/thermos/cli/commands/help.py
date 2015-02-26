from __future__ import print_function

import sys

from twitter.common import app


@app.command(name='help')
def help_command(args, options):
  """Get help about a specific command.
  """
  if len(args) == 0:
    app.help()
  for (command, doc) in app.get_commands_and_docstrings():
    if args[0] == command:
      print('command %s:' % command)
      print(doc)
      app.quit(0)
  print('unknown command: %s' % args[0], file=sys.stderr)
