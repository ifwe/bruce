## Setting Up a CentOS 7 Build Environment

First, install RPM packages as follows:

```
yum groupinstall "Development Tools"
yum install git
yum install libasan
yum install snappy-devel
yum install boost-devel
yum install rpm-build
```

Then download and install the SCons RPM package, which may be obtained from
[http://sourceforge.net/projects/scons/files/scons/2.3.2/](http://sourceforge.net/projects/scons/files/scons/2.3.2/).

Now proceed to
[build, install, and configure Bruce](../README.md#building-and-installing-bruce).

-----

centos_7_env.md: Copyright 2014 if(we), Inc.

centos_7_env.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
