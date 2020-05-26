from fabric.api import local


def deploy():
    local('ansible-playbook db_setup.yml -i hosts.yml')
