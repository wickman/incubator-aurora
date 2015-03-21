#!/bin/bash -e
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

# A script to update sources within the vagrant environment an rebuild/install aurora components.
# Usage: aurorabuild [COMPONENT]...
# where COMPONENT is a name for an aurora component that makes up part of the infrastructure.
# Run with zero arguments for a full list of components that may be built.

set -o nounset

REPO_DIR=/home/vagrant/aurora
DIST_DIR=$REPO_DIR/dist
AURORA_HOME=/usr/local/aurora

function upstart_update {
  # Stop and start is necessary to update a the configuration of
  # an upstart job.  We'll rarely change the configuration, but
  # it's probably better to do this upfront and avoid surprises/confusion.
  # Executing true on failure to please bash -e
  sudo stop $1  || true
  sudo start $1 || true
}

function build_client {
  ./pants binary src/main/python/apache/aurora/client/cli:aurora
  sudo ln -sf $DIST_DIR/aurora.pex /usr/local/bin/aurora
}

function build_admin_client {
  ./pants binary src/main/python/apache/aurora/admin:aurora_admin
  sudo ln -sf $DIST_DIR/aurora_admin.pex /usr/local/bin/aurora_admin
}

function build_scheduler {
  ./gradlew installApp

  export LD_LIBRARY_PATH=/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server
  sudo mkdir -p $AURORA_HOME/scheduler
  if sudo mesos-log initialize --path="$AURORA_HOME/scheduler/db"
  then
    echo "Replicated log initialized."
  else
    echo "Replicated log initialization failed with code $? (likely already initialized)."
  fi
  unset LD_LIBRARY_PATH
  upstart_update aurora-scheduler
}

function build_executor {
  ./pants binary src/main/python/apache/aurora/executor/bin:gc_executor
  ./pants binary src/main/python/apache/aurora/executor/bin:thermos_executor_pesos
  ./pants binary src/main/python/apache/thermos/bin:thermos_runner
  mv -f dist/thermos_executor_pesos.pex dist/thermos_executor.pex

  # Package runner within executor.
  python <<EOF
import contextlib
import zipfile
with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex', 'a')) as zf:
  zf.writestr('apache/aurora/executor/resources/__init__.py', '')
  zf.write('dist/thermos_runner.pex', 'apache/aurora/executor/resources/thermos_runner.pex')
EOF

  chmod +x /home/vagrant/aurora/dist/thermos_executor.pex
}

function build_observer {
  ./pants binary src/main/python/apache/thermos/observer/bin:thermos_observer
  upstart_update aurora-thermos-observer
}

function build_all {
  build_admin_client
  build_client
  build_executor
  build_observer
  build_scheduler
}

function print_components {
  echo 'Please select from: admin_client, client, executor, observer, scheduler or all.'
}

if [ "$#" -eq 0 ]
then
  echo 'Must specify at least one component to build'
  print_components
  exit 1
fi

for component in "$@"
do
  type "build_$component" >/dev/null && continue
  echo "Component $component is unrecognized."
  print_components
  exit 1
done

cd $REPO_DIR
update-sources
for component in "$@"
do
  build_$component
done

exit 0
