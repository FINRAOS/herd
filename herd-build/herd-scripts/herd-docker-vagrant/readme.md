# Docker and Vagrant Demo Environments

## The Vagrant Build Environment

The environment is here to provide a consistent Docker build environment. If you have one already,
you probably don't need it, but it's here as a reference. 

Usage:

1. Install Vagrant from https://vagrantup.com
  * Windows, OSX, Linux are supported
  * Requires a supported virtualization environment
  * provided Vagrantfile tested with Oracle VirtualBox (FOSS Edition) on Windows and OSX
2. From the current directory, issue the command `vagrant up`
3. Once the system has finished configuring itself, connect to it using `vagrant ssh`
4. After the initial remote key approval etc, `cd external` and you should be in the same
   folder this file is in
5. Follow the Docker build directions

*Notes: The ports shared by the Vagrant box are only exposed to localhost - 127.0.0.1 by default. If you want to share the fruits of
your labors with others on your LAN you'll need to change that.*


## Building the Docker Images

Because I tend to run a large number of builds back to back when trying to get a systems config
correct, I download a number of things ahead of time. Also, configuring the Dockerfiles to
minimize the number of repeat downloads to rebuilds is good practice.

### 1. Set Environment Variables and Edit Files

The system will want to know the UI is at, because some things like CORS headers for good 
HTTP security depend on the system knowing what it's expected to be called. 

`export HERD_UI_HOST=127.0.0.1` will map the CORS to allow a simple localhost connection, and if you're doing 
this just for yourself this is what you'll want.

If you're going to drop this somewhere else, figure out its hostname or IP address and use that.

### 2. Download Prerequisites

`./docker-compose-build.sh` will download a number of prerequisites and arrange them so the subsequent 
steps will work.

What happens:

The script downloads a number of things from all over, unpacks and re-organized them so it's something
that the Docker containers can ingest when they run for the first time. This is important, since the
version defined in the script defines the versions of the SQL scripts and stuff it downloads.

### 3. Run `docker-compose` as you normally would

Run `docker-compose build` to pull down and build the images, or if you have a lot of faith,
simply go ahead and run `docker-compose up`.

What happens:

The Postgresql container mounts a new directory prepared by the `docker-compose-build.sh` script and executes
all the sql for the release therein. 

The other containers download what they need to, and stand themselves up, and they use the same repositories referenced
in the AWS CloudFormation scripts.