## Building and Installing Bruce

Once you have finished [setting up your build environment](../README.md#setting-up-a-build-environment),
you are ready to build Bruce.  If you are building on CentOS 6, remember to set
your `PATH` and `LD_LIBRARY_PATH` environment variables and activate your
Python virtualenv environment before building, as detailed
[here](centos_6_5_env.md).  The first step is to clone Bruce's Git repository:

```
git clone https://github.com/tagged/bruce.git
```

### Building an RPM package

If you will be running Bruce on an RPM-based distribution such as CentOS or
RHEL, you will probably want to build an RPM package.  Here you have two
choices.  You can build either an RPM package that includes Bruce's init script
and config files or an RPM package that omits these files.  You might prefer
the latter choice if you want to manage the config files separately using a
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

The init script for Bruce (see [config/bruce.init](../config/bruce.init)) is an
older System V type of script.  Scripts that work with the newer *systemd*
included in CentOS 7 and *upstart* included in recent Ubuntu distributions are
currently not available.  Contributions from the community would be much
appreciated.

### Building Bruce Directly

For all platforms except CentOS 6, Bruce may be built directly using SCons as
follows:

```
cd bruce
source bash_defs
cd src/bruce
build --release bruce
cd ../..
```

For CentOS 6, the steps are identical except that instead of
`build --release bruce` you should type `build --release --import_path bruce`.
The `--import_path` option tells the SCons build configuration to use the PATH
environment variable setting from the external environment.  This is needed
so that your customized PATH setting (as described
[here](centos_6_5_env.md#building-and-installing-gcc-482)) is seen and the
newer version of gcc is used.  After performing the above steps, the path to
the newly built Bruce executable is now `out/release/bruce/bruce`.

### Building Bruce's Client Library

Additionally, there is a C library for clients that send messages to Bruce.
There is also a simple command line client program that uses the library.
These may be built in the same manner as above:

```
cd bruce
source bash_defs
cd src/bruce/client
build --release libbruce_client.so
build --release libbruce_client.a
build --release simple_bruce_client
cd ../../..
```

In the case of CentOS 6, remember to use the `--import_path` option with the
`build` command.  The newly built library files and client program are now
located in `out/release/bruce/client`.  If installing them manually, rename
`libbruce_client.so` to `libbruce_client.so.0` when copying it to your system's
library directory (`/usr/lib64` on CentOS/RedHat, or `/usr/lib` on Ubuntu), and
remember to run `/sbin/ldconfig` afterwards.  Also remember to install the
client library header files as follows:

```
mkdir -p /usr/include/bruce/client
cp src/bruce/client/*.h /usr/include/bruce/client
```
One of the header files,
[bruce/client/bruce_client_socket.h](../src/bruce/client/bruce_client_socket.h),
provides a simple C++ wrapper class for the library functions that deal with
Bruce's UNIX domain socket.  Once the library is installed, C and C++ clients
can specify `-lbruce_client` when building with gcc.  The client library,
header files, and command line program are included in Bruce's RPM package.
For example C code that uses the client library to send a message to Bruce, see
the big comment at the top of
[bruce/client/bruce_client.h](../src/bruce/client/bruce_client.h).  For example
C++ code, see
[bruce/client/simple_bruce_client.cc](../src/bruce/client/simple_bruce_client.cc).

### Installing Bruce

If you built an RPM package containing Bruce, then you can install it using an
RPM command such as:

```
rpm -Uvh out/pkg/rpm/bruce-1.0.6.38.g66c5a2d-1.el6.x86_64.rpm
```

Otherwise, you can copy the Bruce executable to a location of your choice, such
as `/usr/bin`.  If you are running on CentOS 6, remember that the gcc482 RPM
package described [here](centos_6_5_env.md#building-and-installing-gcc-482)
must be installed, and `LD_LIBRARY_PATH` must contain `/opt/gcc/lib64` in the
shell that you execute Bruce from.  If you built your RPM package using the
`rpm_noconfig` option described above, or you built Bruce directly using SCons,
you will need to install Bruce's init script, sysconfig file, and configuration
file (which mostly contains settings related to batching and compression)
separately.  Assuming you are in the root of the Git repository (where Bruce's
[SConstruct](../SConstruct) file is found), you can do this as follows:

```
cp config/bruce.init /etc/init.d/bruce
cp config/bruce.sysconfig /etc/sysconfig/bruce
mkdir /etc/bruce  # or some other suitable location
cp config/bruce_conf.xml /etc/bruce
```

At this point, you are ready to
[run Bruce with a basic configuration](../README.md#running-bruce-with-basic-configuration).

-----

build_install.md: Copyright 2014 if(we), Inc.

build_install.md is licensed under a Creative Commons Attribution-ShareAlike
4.0 International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
