# Remote Provisioning for Bindings Test

This script use the fabric deployment tool (https://docs.fabfile.org) v2.
It should be run from the root directory of the project. It will:

1. Build a copy of aeron-all-<version>.jar
2. Copy the jar to a remote server
3. Ssh to a remote server, start an instance of the remote provision service
4. Run the RemoteEchoTest
5. Stop the service

Usage

```shell
$ fab -r aeron-system-tests/scripts/provisioning \
  -H <ip address to run the remote service on>
  stop \
  prepare-deploy \
  deploy \ 
    --java-home=<location of the java home directory on the remote server> \
  test \ 
    --test-host=<optional, ip address of the host running RemoteEchoTest> \ 
    --aeron-dir=<optional, path to an already running media driver>  
```
