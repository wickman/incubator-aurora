from __future__ import print_function

import getpass

from twitter.common import app

from apache.thermos.cli.common import really_run
from apache.thermos.common.options import add_binding_to, add_port_to
from apache.thermos.config.loader import ThermosTaskWrapper
from apache.thermos.config.schema import Process, Resources, Task


@app.command
@app.command_option("--user", metavar="USER", default=getpass.getuser(), dest='user',
                    help="run as this user.  if not $USER, must have setuid privilege.")
@app.command_option("--name", metavar="STRING", default='simple', dest='name',
                    help="The name to give this task.")
@app.command_option("--task_id", metavar="STRING", default=None, dest='task_id',
                    help="The id to which this task should be bound, synthesized from the task "
                    "name if none provided.")
@app.command_option("-P", "--port", type="string", nargs=1, action="callback",
                    callback=add_port_to('prebound_ports'), dest="prebound_ports", default=[],
                    metavar="NAME:PORT", help="bind named PORT to NAME.")
@app.command_option("-E", "--environment", type="string", nargs=1, action="callback",
                    callback=add_binding_to('bindings'), default=[], dest="bindings",
                    metavar="NAME=VALUE",
                    help="bind the configuration environment variable NAME to VALUE.")
@app.command_option("--daemon", default=False, action='store_true', dest='daemon',
                    help="fork and daemonize the thermos runner.")
def simplerun(args, options):
  """Run a simple command line as a thermos task.

    Usage: thermos simplerun [options] [--] commandline
    Options:
      --user=USER		   run as this user.  if not $USER, must have setuid privilege.
      --name=STRING		   the name to give this task. ('simple' by default)
      --task_id=STRING		   the id to which this task should be bound, synthesized from the
                                   task name if none provided.
      -P/--port=NAME:PORT	   bind the named port NAME to port number PORT (may be specified
                                   multiple times to bind multiple names.)
      -E/--environment=NAME=VALUE  bind the configuration environment variable NAME to
                                   VALUE.
      --daemon			   Fork and daemonize the task.
  """
  try:
    cutoff = args.index('--')
    cmdline = ' '.join(args[cutoff + 1:])
  except ValueError:
    cmdline = ' '.join(args)

  print("Running command: '%s'" % cmdline)

  thermos_task = ThermosTaskWrapper(Task(
    name=options.name,
    resources=Resources(cpu=1.0, ram=256 * 1024 * 1024, disk=0),
    processes=[Process(name=options.name, cmdline=cmdline)]))

  really_run(thermos_task,
             options.root,  # hrm -- we should make this dep explicit ideally
             None,
             task_id=options.task_id,
             user=options.user,
             prebound_ports=options.prebound_ports,
             chroot=False,
             daemon=options.daemon)
