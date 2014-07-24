#!/bin/bash -x
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
#
# Common utility functions used by different variants of the
# end to end tests.

# Preserve original stderr so output from signal handlers doesn't get redirected to /dev/null.
exec 4>&2

_curl() { curl --silent --fail --retry 4 --retry-delay 10 "$@" ; }

collect_result() {
  (
    if [[ $RETCODE = 0 ]]
    then
      echo "***"
      echo "OK (all tests passed)"
      echo "***"
    else
      echo "!!!"
      echo "FAIL (something returned non-zero)"
      echo ""
      echo "This may be a transient failure (as in scheduler failover) or it could be a real issue"
      echo "with your code. Either way, this script DNR merging to master. Note you may need to"
      echo "reconcile state manually."
      echo "!!!"
      vagrant ssh -c "aurora killall devcluster/vagrant/test/http_example"
    fi
    exit $RETCODE
  ) >&4 # Send to the stderr we had at startup.
}

validate_serverset() {
  # default python return code
  retcode=0

  # launch aurora client in interpreter mode to get access to the kazoo client
  cat <<EOF | vagrant ssh -c "env SERVERSET="$1" PEX_INTERPRETER=1 aurora" >& /dev/null || retcode=$?
import os, posixpath, sys, time
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

OK = 1
DID_NOT_REGISTER = 2
DID_NOT_RECOVER_FROM_EXPIRY = 3

serverset = os.getenv('SERVERSET')
client = KazooClient('localhost:2181')
client.start()

def wait_until_znodes(count, timeout=30):
  now = time.time()
  timeout += now
  while now < timeout:
    try:
      children = client.get_children(serverset)
    except NoNodeError:
      children = []
    if len(children) == count:
      return [posixpath.join(serverset, child) for child in children]
    time.sleep(1)
    now += 1
  return []

znodes = wait_until_znodes(2, timeout=10)
if not znodes:
  sys.exit(DID_NOT_REGISTER)

client.delete(znodes[0])

znodes = wait_until_znodes(2, timeout=10)
if not znodes:
  sys.exit(DID_NOT_RECOVER_FROM_EXPIRY)

sys.exit(OK)
EOF

  if [[ $retcode = 1 ]]; then
    echo "Validated announced job."
    return 0
  elif [[ $retcode = 2 ]]; then
    echo "Job failed to announce in serverset."
  elif [[ $retcode = 3 ]]; then
    echo "Job failed to re-announce when expired."
  else
    echo "Unknown failure in test script."
  fi

  return 1
}
