# -*- mode: ruby -*-
# vi: set ft=ruby :
#
# Copyright 2013 Apache Software Foundation
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

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# TODO(ksweeney): RAM requirements are not empirical and can probably be significantly lowered.
Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "precise64"

  # The url from where the 'config.vm.box' box will be fetched if it
  # doesn't already exist on the user's system.
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"

  config.vm.define "devtools" do |devtools|
    devtools.vm.network :private_network, ip: "192.168.33.7"
    devtools.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "2048", "--cpus", "8"]
    end
    devtools.vm.provision "shell", path: "examples/vagrant/provision-dev-environment.sh"
  end

  config.vm.define "zookeeper" do |zookeeper|
    zookeeper.vm.network :private_network, ip: "192.168.33.2"
    zookeeper.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    zookeeper.vm.provision "shell", path: "examples/vagrant/provision-zookeeper.sh"
  end

  config.vm.define "mesos-master" do |master|
    master.vm.network :private_network, ip: "192.168.33.3"
    master.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "1024"]
    end
    master.vm.provision "shell", path: "examples/vagrant/provision-mesos-master.sh"
  end

  config.vm.define "mesos-slave1" do |slave|
    slave.vm.network :private_network, ip: "192.168.33.4"
    slave.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    slave.vm.provision "shell", path: "examples/vagrant/provision-mesos-slave.sh"
  end

  config.vm.define "mesos-slave2" do |slave|
    slave.vm.network :private_network, ip: "192.168.33.5"
    slave.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    slave.vm.provision "shell", path: "examples/vagrant/provision-mesos-slave.sh"
  end

  config.vm.define "aurora-scheduler" do |scheduler|
    scheduler.vm.network :private_network, ip: "192.168.33.6"
    scheduler.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "1024"]
    end
    scheduler.vm.provision "shell", path: "examples/vagrant/provision-aurora-scheduler.sh"
  end
end

