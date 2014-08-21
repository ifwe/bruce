## Building and Installing Bruce

Once you have finished [setting up your build environment](https://github.com/tagged/bruce#setting-up-a-build-environment),
you are ready to build Bruce.  If you are building on CentOS 6, remember to set
your `PATH` and `LD_LIBRARY_PATH` environment variables and activate your
Python virtualenv environment before building, as detailed
[here](https://github.com/tagged/bruce/blob/master/doc/centos_6_5_env.md).  The
first step is to clone Bruce's Git repository:

```
git clone https://github.com/tagged/bruce.git
```

### Building an RPM package

If you will be running Bruce on an RPM-based distribution such as CentOS or
RHEL, you will probably want to build an RPM package.  Here you have two
choices.  You can build either an RPM package that includes bruce's init script
and config files or an RPM package that omits these files.  You might prefer
the latter choice if you prefer to manage the config files separately using a
tool such as Puppet.  To choose the first option, do the following:

```
cd bruce
./pkg rpm
```

To choose the second option, do the following:

```
cd bruce
./pkg rpm_noconfig
```

In the former case, the resulting RPM packages can be found in directory
`out/pkg/rpm`.  In the latter case, they can be found in
`out/pkg/rpm_noconfig`.

The init script for bruce (see `config/bruce.init`) is an older System V type
of script.  Scripts that work with the newer *systemd* included in CentOS 7 and
*upstart* included in recent Ubuntu distributions are currently not available.
Contributions from the community would be much appreciated.

### Building Bruce Directly

Bruce may be built directly using SCons as follows:

```
cd bruce
source bash_defs
cd src/bruce
build --release bruce
cd ../..
```

The path to the newly built Bruce executable is now `out/release/bruce/bruce`.

Additionally, a simple command line client for sending messages to Bruce can be
built in the same manner as above:

```
cd bruce
source bash_defs
cd src/bruce/simple_client
build --release simple_bruce_client
cd ../../..
```

The path to the client program is now
`out/release/bruce/simple_client/simple_bruce_client`.  This client is included
in Bruce's RPM package.

### Installing Bruce

If you built an RPM package containing bruce, they you can install it using an
RPM command such as:

```
rpm -Uvh out/pkg/rpm/bruce-1.0.6.38.g66c5a2d-1.el6.x86_64.rpm
```

Otherwise, you can copy the Bruce executable to a location of your choice, such
as `/usr/bin`.  If you are running on CentOS 6, remember that the gcc482 RPM
package described [here](https://github.com/tagged/bruce/blob/master/doc/centos_6_5_env.md#building-and-installing-gcc-482)
must be installed, and `LD_LIBRARY_PATH` must contain `/opt/gcc/lib64` in the
shell that you execute Bruce from.  If you built your RPM package using the
`rpm_noconfig` option described above, or you built Bruce directly using SCons,
you will need to install Bruce's init script, sysconfig file, and configuration
file (which mostly contains settings related to batching and compression)
separately.  Assuming you are in the root of the Git repository (where Bruce's
`SConstruct` file is found), you can do this as follows:

```
cp config/bruce.init /etc/init.d/bruce
cp config/bruce.sysconfig /etc/sysconfig/bruce
mkdir /etc/bruce  # or some other suitable location
cp config/bruce_conf.xml /etc/bruce
```

At this point, you are ready to [run Bruce with a basic configuration](https://github.com/tagged/bruce#running-bruce-with-basic-configuration).
