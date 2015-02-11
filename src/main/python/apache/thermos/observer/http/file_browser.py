#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
from xml.sax.saxutils import escape

import bottle
from twitter.common import log
from twitter.common.http import HttpServer

from .templating import HttpTemplate

MB = 1024 * 1024
DEFAULT_CHUNK_LENGTH = MB
MAX_CHUNK_LENGTH = 16 * MB


def _read_chunk(filename, offset=None, length=None):
  offset = offset or -1
  length = length or -1

  try:
    length = long(length)
    offset = long(offset)
  except ValueError:
    return {}

  if not os.path.isfile(filename):
    return {}

  try:
    fstat = os.stat(filename)
  except Exception as e:
    log.error('Could not read from %s: %s' % (filename, e))
    return {}

  if offset == -1:
    offset = fstat.st_size

  if length == -1:
    length = fstat.st_size - offset

  with open(filename, 'r') as fp:
    fp.seek(offset)
    try:
      data = fp.read(length)
    except IOError as e:
      log.error('Failed to read %s: %s' % (filename, e), exc_info=True)
      return {}

  if data:
    return dict(offset=offset, length=len(data), data=escape(data.decode('utf8', 'replace')))

  return dict(offset=offset, length=0)


# _observer.logs(task_id, process, [run]) => {stdout: (chroot, path), stderr: (chroot, path)}
# _observer.valid_file(task_id, path) => (chroot, path) iff also a file else None, None
# _observer.valid_path(task_id, path) => (chroot, path) iff valid else None, None
class TaskObserverFileBrowser(object):
  """
    Mixin for Thermos observer File browser.
  """

  def __init__(self, database):
    self._database = database

  def __get_state_attribute(self, task_id, attr):
    state = self._database.get_state(task_id).get(task_id)
    if state is None or state.header is None:
      return None
    return getattr(state.header, attr, None)

  def __get_sandbox(self, task_id):
    return self.__get_state_attribute(task_id, 'sandbox')

  def __get_log_dir(self, task_id, process, run):
    return self.__get_state_attribute(task_id, 'log_dir')

  def __sanitize_path(self, root, relpath):
    normalized_base = os.path.realpath(root)
    normalized = os.path.realpath(os.path.join(root, relpath))
    if normalized.startswith(normalized_base):
      return (normalized_base, os.path.relpath(normalized, normalized_base))
    return (None, None)

  def __get_file(self, task_id, path):
    sandbox = self.__get_sandbox(self, task_id)
    return self.__sanitize_path(sandbox, path)

  def __get_log_filename(self, task_id, process, run, logtype):
    if logtype not in ('stdout', 'stderr'):
      bottle.abort(404, 'No such log type: %s' % logtype)
    log_dir = self.__get_log_dir(task_id, process, run)
    if log_dir is None:
      bottle.abort(404, 'File not found.')
    return os.path.join(log_dir, logtype)

  @HttpServer.route('/logs/:task_id/:process/:run/:logtype')
  @HttpServer.mako_view(HttpTemplate.load('logbrowse'))
  def handle_logs(self, task_id, process, run, logtype):
    filename = self.__get_log_filename(task_id, process, run, logtype)
    return {
      'task_id': task_id,
      'filename': filename,
      'process': process,
      'run': run,
      'logtype': logtype
    }

  @HttpServer.route('/logdata/:task_id/:process/:run/:logtype')
  def handle_logdata(self, task_id, process, run, logtype):
    offset = HttpServer.Request.GET.get('offset', -1)
    length = HttpServer.Request.GET.get('length', -1)
    filename = self.__get_log_filename(task_id, process, run, logtype)
    return _read_chunk(filename, offset, length)

  @HttpServer.route('/file/:task_id/:path#.+#')
  @HttpServer.mako_view(HttpTemplate.load('filebrowse'))
  def handle_file(self, task_id, path):
    if path is None:
      bottle.abort(404, 'No such file or directory.')
    return {
      'task_id': task_id,
      'filename': path,
    }

  @HttpServer.route('/filedata/:task_id/:path#.+#')
  def handle_filedata(self, task_id, path):
    if path is None:
      return {}
    offset = HttpServer.Request.GET.get('offset', -1)
    length = HttpServer.Request.GET.get('length', -1)
    chroot, path = self.__get_file(task_id, path)
    if chroot is None or path is None:
      bottle.abort(404, 'Invalid file.')
    filename = os.path.join(chroot, path)
    if not os.path.isfile(filename):
      bottle.abort(404, 'Not a file.')
    return _read_chunk(os.path.join(chroot, path), offset, length)

  @HttpServer.route('/browse/:task_id')
  @HttpServer.route('/browse/:task_id/:path#.*#')
  @HttpServer.mako_view(HttpTemplate.load('filelist'))
  def handle_dir(self, task_id, path=None):
    path = path or '.'
    chroot, path = self.__get_file(task_id, path)
    if chroot is None or path is None:
      bottle.abort(404, 'No such file or directory.')
    return dict(task_id=task_id, chroot=chroot, path=path)

  @HttpServer.route('/download/:task_id/:path#.+#')
  def handle_download(self, task_id, path=None):
    chroot, path = self.__get_file(task_id, path)
    if chroot is None or path is None:
      bottle.abort(404, 'No such file or directory.')
    return bottle.static_file(path, root=chroot, download=True)
