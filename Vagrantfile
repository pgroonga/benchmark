# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  vms = [
    {
      :id => "centos-7",
      :box => "bento/centos-7.4",
      :ip => "192.168.56.1",
    },
    {
      :id => "almalinux-8-primary",
      :box => "bento/almalinux-8.5",
      :ip => "192.168.56.11",
    },
    {
      :id => "almalinux-8-standby",
      :box => "bento/almalinux-8.5",
      :ip => "192.168.56.12",
    },
  ]

  n_cpus = ENV["VM_N_CPUS"] || 2
  n_cpus = Integer(n_cpus) if n_cpus
  memory = ENV["VM_MEMORY"] || 4096
  memory = Integer(memory) if memory
  synced_folders = (ENV["VM_SYNCED_FOLDERS"] || "").split(",")
  vms.each do |vm|
    id = vm[:id]
    box = vm[:box]
    config.vm.define(id) do |node|
      node.vm.box = box
      node.vm.provider("virtualbox") do |virtual_box|
        virtual_box.cpus = n_cpus if n_cpus
        virtual_box.memory = memory if memory
      end
      node.vm.provision("ansible") do |ansible|
        ansible.playbook = "ansible/#{id}/playbook.yml"
        ansible.groups = {
          "servers" => [id],
        }
        ansible.host_key_checking = false
        # ansible.raw_arguments  = [
        #   "-vvv",
        # ]
      end
      synced_folders.each do |synced_folder|
        node.vm.synced_folder(*synced_folder.split(":", 2))
      end
      node.vm.network "private_network", ip: vm[:ip]
    end
  end
end
