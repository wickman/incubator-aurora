import sys


class CommandProcessor(object):
  """A wrapper for anything which can receive a set of command-line parameters and execute
  something using them.

  This is built assuming that the first command-line parameter is the name of
  a command to be executed. For example, if this was being used to build a command-line
  tool named "tool", then a typical invocation from the command-line would look like
  "tool cmd arg1 arg2". "cmd" would be the name of the command to execute, and
  "arg1" and "arg2" would be the parameters to that command.
  """

  def execute(self, args):
    """Execute the command-line tool wrapped by this processor.

    :param args: a list of the parameters used to invoke the command. Typically,
        this will be sys.argv.
    """
    pass

  def get_commands(self):
    """Get a list of the commands that this processor can handle."""
    pass


class Bridge(object):
  """Given multiple command line programs, each represented by a "CommandProcessor" object,
  refer command invocations to the command line that knows how to process them.
  """

  def __init__(self, command_processors, default=None):
    """
    :param command_processors: a list of command-processors.
    :param default: the default command processor. any command which is not
      reported by "get_commands" as part of any of the registered processors
      will be passed to the default.
    """
    self.command_processors = command_processors
    self.default = default

  def execute(self, args):
    """Dispatch a command line to the appropriate CommandProcessor"""
    for cl in self.command_processors:
      if args[1] in cl.get_commands():
        return cl.execute(args)
    if self.default is not None:
      return self.default.execute(args)
    else:
      print('Unknown command: %s' % args[1])
      sys.exit(1)
