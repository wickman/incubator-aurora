#
# Copyright 2014 Apache Software Foundation
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


from twitter.common.lang import Compatibility


class EntryPointError(Exception):
  pass


def parse_entry_point(entry_point_str):
  if not isinstance(entry_point_str, Compatibility.string):
    raise TypeError('parse_entry_point expects string, got %s' % type(entry_point_str))
  try:
    module, attribute = entry_point_str.split(':')
  except TypeError:
    raise ValueError('Invalid entry point: %r' % entry_point_str)
  return module, attribute


def get_validated_entry_point(entry_point_str, interface):
  module, attribute = parse_entry_point(entry_point_str)

  try:
    entry_module = __import__(module, globals(), globals(), [__name__])
  except ImportError as e:
    raise EntryPointError('Could not find entry point: %r' % e)

  entry_point = getattr(entry_module, attribute, None)

  if entry_point is None:
    raise EntryPointError('Could not find entry point: %r' % attribute)

  if not issubclass(entry_point, interface):
    raise EntryPointError('Entry point %r = %r is not of the expected type %r' % (
        entry_point_str, entry_point, interface))

  return entry_point
