# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  vms = [
    {
      :id => "debian-stretch",
      :box => "bento/debian-9.3",
    },
    {
      :id => "ubuntu-17.10",
      :box => "bento/ubuntu-17.10",
    },
    {
      :id => "centos-7",
      :box => "bento/centos-7.4",
    },
  ]

  id_prefix = ENV["VM_ID_PREFIX"]
  n_cpus = ENV["VM_N_CPUS"] || 2
  n_cpus = Integer(n_cpus) if n_cpus
  memory = ENV["VM_MEMORY"] || 1024
  memory = Integer(memory) if memory
  vms.each do |vm|
    id = vm[:id]
    box = vm[:box] || id
    id = "#{id_prefix}#{id}" if id_prefix
    config.vm.define(id) do |node|
      node.vm.box = box
      node.vm.provider("virtualbox") do |virtual_box|
        virtual_box.cpus = n_cpus if n_cpus
        virtual_box.memory = memory if memory
      end
      node.vm.provision("ansible") do |ansible|
        os = id.split("-").first
        ansible.playbook = "ansible/#{os}/playbook.yml"
        ansible.groups = {
          "servers" => [id],
        }
        ansible.host_key_checking = false
        # ansible.raw_arguments  = [
        #   "-vvv",
        # ]
      end
    end
  end

  config.vm.network "public_network"
end
