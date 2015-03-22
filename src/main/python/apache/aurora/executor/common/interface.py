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

"""
The mesos.interface module provides pb2 stubs compiled with protobuf 2.5.0.
pesos vendorizes stubs compiled with protobuf 2.6.1 in order to be Python
3.x compatible.  These are not code-compatible with each other and cannot
mix and match.  As such, we need this bridge to detect when pesos is present
to use the pesos vendorized versions.  Note that if pesos is available, we
can no longer use the mesos native driver, so this is an all-or-nothing
proposition.
"""

try:
  from pesos.vendor.mesos import mesos_pb2  # noqa
except ImportError:
  from mesos.interface import mesos_pb2  # noqa

__all__ = ('mesos_pb2',)
