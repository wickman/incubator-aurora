from __future__ import print_function

import getpass

from twitter.common import app

from apache.thermos.cli.common import get_task_from_options, really_run
from apache.thermos.common.options import add_binding_to, add_port_to


@app.command
@app.command_option("--user", metavar="USER", default=getpass.getuser(), dest='user',
                    help="run as this user.  if not $USER, must have setuid privilege.")
@app.command_option("--enable_chroot", dest="chroot", default=False, action='store_true',
                    help="chroot tasks to the sandbox before executing them, requires "
                    "root privileges.")
@app.command_option("--task", metavar="TASKNAME", default=None, dest='task',
                    help="The thermos task within the config that should be run. Only required if "
                    "there are multiple tasks exported from the thermos configuration.")
@app.command_option("--task_id", metavar="STRING", default=None, dest='task_id',
                    help="The id to which this task should be bound, synthesized from the task "
                    "name if none provided.")
@app.command_option("--json", default=False, action='store_true', dest='json',
                    help="Read the source file in json format.")
@app.command_option("--sandbox", metavar="PATH", default="/var/lib/thermos/sandbox", dest='sandbox',
                    help="The sandbox in which to run the task.")
@app.command_option("-P", "--port", type="string", nargs=1, action="callback",
                    callback=add_port_to('prebound_ports'), dest="prebound_ports", default=[],
                    metavar="NAME:PORT", help="bind named PORT to NAME.")
@app.command_option("-E", "--environment", type="string", nargs=1, action="callback",
                    callback=add_binding_to('bindings'), default=[], dest="bindings",
                    metavar="NAME=VALUE",
                    help="bind the configuration environment variable NAME to VALUE.")
@app.command_option("--daemon", default=False, action='store_true', dest='daemon',
                    help="fork and daemonize the thermos runner.")
def run(args, options):

  """Run a thermos task.

    Usage: thermos run [options] config
    Options:
      --user=USER		   run as this user.  if not $USER, must have setuid privilege.
      --enable_chroot		   chroot into the sandbox for this task, requires superuser
                                   privilege
      --task=TASKNAME		   the thermos task within the config that should be run.  only
                                   required if there are multiple tasks exported from the thermos
                                   configuration.
      --task_id=STRING		   the id to which this task should be bound, synthesized from the
                                   task name if none provided.
      --json			   specify that the config is in json format instead of pystachio
      --sandbox=PATH		   the sandbox in which to run the task
                                   [default: /var/lib/thermos/sandbox]
      -P/--port=NAME:PORT	   bind the named port NAME to port number PORT (may be specified
                                   multiple times to bind multiple names.)
      -E/--environment=NAME=VALUE  bind the configuration environment variable NAME to
                                   VALUE.
      --daemon			   Fork and daemonize the task.
  """
  thermos_task = get_task_from_options(args, options)
  really_run(thermos_task,
             options.root,
             options.sandbox,
             task_id=options.task_id,
             user=options.user,
             prebound_ports=options.prebound_ports,
             chroot=options.chroot,
             daemon=options.daemon)
