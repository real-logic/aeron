from invoke import task
from invoke import run as local
from fabric import Connection
import re

@task()
def prepare_deploy(c):
    local("./gradlew --console=verbose clean", env = {"JAVA_HOME": "/home/mike/opt/jdk/jdk8"})
    local("./gradlew --console=verbose :aeron-all:jar", env = {"JAVA_HOME": "/home/mike/opt/jdk/jdk8"})

def parse_version(version_string):
    g = re.search("version \"(.*)\\.(.*)\\..*\"", version_string)

    if (g is None):
        raise RuntimeError("Unable to get version information: {}".format(r.stdout))

    if (g.group(1) == "1"):
        version = int(g.group(2))
    else:
        version = int(g.group(1))

    return version

@task()
def version(c, java_home=None):
    if (java_home is None):
        raise RuntimeError("--java-home must be specified")

    r = c.run("{}/bin/java -version 2>&1".format(java_home), hide=True)
    print(parse_version(r.stdout.splitlines()[0]))

@task()
def deploy(c, java_home=None, provisioning_host=None):
    if (java_home is None):
        raise RuntimeError("--java-home must be specified")

    r = c.run("{}/bin/java -version 2>&1".format(java_home), hide=True)
    java_version = parse_version(r.stdout.splitlines()[0])

    if (provisioning_host is None):
        provisioning_host = c.host

    command = [
      "{}/bin/java".format(java_home),
      "-Dcom.sun.management.jmxremote",
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dcom.sun.management.jmxremote.ssl=false",
      "-Dcom.sun.management.jmxremote.port=10000",
      "-Djava.rmi.server.hostname={}".format(provisioning_host),
      "-cp ./provisioning/aeron-all-1.38.0-SNAPSHOT.jar",
      "io.aeron.samples.echo.ProvisioningServerMain",
      "< /dev/null > ./provisioning/log 2>&1",
      "&"
    ]

    if (8 < java_version):
        command.insert(1, "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED")

    with open('version.txt') as f:
        lines = f.readlines()
        c.run("rm -rf provisioning")
        c.run("mkdir -p provisioning")
        c.put("aeron-all/build/libs/aeron-all-{}.jar".format(lines[0]), "provisioning/.")
        c.run(" ".join(command))
        c.run("sleep 2 ; pgrep -f io.aeron.samples.echo.ProvisioningServerMain")

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
    c.run("pkill -f io.aeron.samples.echo.ProvisioningServerMain", warn=True)
