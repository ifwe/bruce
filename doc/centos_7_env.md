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

Now proceed to [build, install, and configure Bruce](https://github.com/tagged/bruce/blob/master/README.md#building-and-installing-bruce).
