from __future__ import print_function

import os
import sys
import time

from twitter.common import app
from twitter.common.dirutil import tail_f
from twitter.common.dirutil.tail import tail as tail_closed

from apache.thermos.common.ckpt import CheckpointDispatcher
from apache.thermos.common.path import TaskPath
from apache.thermos.monitoring.detector import TaskDetector
from apache.thermos.monitoring.monitor import TaskMonitor


@app.command
@app.command_option("--stderr", default=False, dest='use_stderr', action='store_true',
                    help="Tail stderr instead of stdout")
def tail(args, options):
  """Tail the logs of a task process.

    Usage: thermos tail task_name [process_name]
  """
  if len(args) == 0:
    app.error('Expected a task to tail, got nothing!')
  if len(args) not in (1, 2):
    app.error('Expected at most two arguments (task and optional process), got %d' % len(args))

  task_id = args[0]
  detector = TaskDetector(root=options.root)
  checkpoint = CheckpointDispatcher.from_file(detector.get_checkpoint(task_id))
  log_dir = checkpoint.header.log_dir
  process_runs = [(process, run) for (process, run) in detector.get_process_runs(task_id, log_dir)]
  if len(args) == 2:
    process_runs = [(process, run) for (process, run) in process_runs if process == args[1]]

  if len(process_runs) == 0:
    print('ERROR: No processes found.', file=sys.stderr)
    sys.exit(1)

  processes = set([process for process, _ in process_runs])
  if len(processes) != 1:
    print('ERROR: More than one process matches query.', file=sys.stderr)
    sys.exit(1)

  process = processes.pop()
  run = max([run for _, run in process_runs])

  logdir = TaskPath(root=options.root, task_id=args[0], process=process,
     run=run, log_dir=log_dir).getpath('process_logdir')
  logfile = os.path.join(logdir, 'stderr' if options.use_stderr else 'stdout')

  monitor = TaskMonitor(TaskPath(root=options.root), args[0])
  def log_is_active():
    active_processes = monitor.get_active_processes()
    for process_status, process_run in active_processes:
      if process_status.process == process and process_run == run:
        return True
    return False

  if not log_is_active():
    print('Tail of terminal log %s' % logfile)
    for line in tail_closed(logfile):
      print(line.rstrip())
    return

  now = time.time()
  next_check = now + 5.0
  print('Tail of active log %s' % logfile)
  for line in tail_f(logfile, include_last=True, forever=False):
    print(line.rstrip())
    if time.time() > next_check:
      if not log_is_active():
        break
      else:
        next_check = time.time() + 5.0
