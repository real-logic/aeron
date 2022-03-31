from invoke import task
from invoke import run as local
from fabric import Connection

@task()
def prepare_deploy(c):
    local("./gradlew --console=verbose clean", env = {"JAVA_HOME": "/home/mike/opt/jdk/jdk8"})
    local("./gradlew --console=verbose :aeron-all:jar", env = {"JAVA_HOME": "/home/mike/opt/jdk/jdk8"})

@task()
def deploy(c, java_home=None, provisioning_host=None):
    if (provisioning_host is None):
        provisioning_host = c.host

    with open('version.txt') as f:
        lines = f.readlines()
        c.run("rm -rf provisioning")
        c.run("mkdir -p provisioning")
        c.put("aeron-system-tests/scripts/provisioning/provisioning_server.sh", "provisioning/.")
        c.put("aeron-all/build/libs/aeron-all-{}.jar".format(lines[0]), "provisioning/.")
        c.run("AERON_HOME=./provisioning JAVA_HOME={} PROVISIONING_HOST={} ./provisioning/provisioning_server.sh start < /dev/null > ./provisioning/log 2>&1".format(java_home, provisioning_host), pty=False)
        c.run("sleep 2 ; AERON_HOME=./provisioning ./provisioning/provisioning_server.sh status")

@task()
def test(c, provisioning_host=None, test_host=None, aeron_dir=None):
    if (provisioning_host is None):
        provisioning_host = c.host

    command = [
        "./gradlew",
        "-Daeron.test.system.binding.remote.host={}".format(provisioning_host),
        "--console=verbose",
        ":aeron-system-test:test",
        "--tests",
        "'*RemoteEchoTest'",
    ]

    if (not test_host is None):
        command.insert(1, "-Daeron.test.system.binding.local.host={}".format(test_host))

    if (not aeron_dir is None):
        command.insert(1, "-Daeron.test.system.aeron.dir={}".format(aeron_dir))

    local(" ".join(command))

@task()
def stop(c):
    c.run("AERON_HOME=./provisioning ./provisioning/provisioning_server.sh stop")
