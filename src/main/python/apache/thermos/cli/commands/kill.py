from __future__ import print_function

import sys

from twitter.common import app

from apache.thermos.cli.common import tasks_from_re
from apache.thermos.core.helper import TaskRunnerHelper


@app.command
def kill(args, options):
  """Kill task(s)

  Usage: thermos kill task_id1 [task_id2 ...]

  Regular expressions may be used to match multiple tasks.
  """
  if not args:
    print('Must specify tasks!', file=sys.stderr)
    return

  matched_tasks = tasks_from_re(args, options.root, state='active')

  if not matched_tasks:
    print('No active tasks matched.')
    return

  for task_id in matched_tasks:
    print('Killing %s...' % task_id, end='')
    TaskRunnerHelper.kill(task_id, options.root, force=True)
    print('done.')
