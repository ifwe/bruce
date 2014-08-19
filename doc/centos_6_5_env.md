## Setting Up a CentOS 6.5 Build Environment

Bruce is implemented in C++, and makes extensive use of C++11 language
features.  Therefore, it must be built using a more recent version of gcc than
what CentOS 6.5 provides.  Likewise, a newer version of Python is required to
support Bruce's SCons-based Python build scripts.

### Building and Installing gcc 4.8.2

For simplicity, the following instructions assume that you are running on an
x86-64 architecture Linux installation.  To build and install gcc 4.8.2, do the
following:

1. First install 32-bit versions of the glibc binaries (in addition to the
   existing 64-bit binaries).  To do this, start by appending the following
   line to /etc/yum.conf:

   ```
   multilib_policy=all
   ```

   Next execute the following commands:

   ```
   yum install glibc
   yum install glibc-devel
   yum install glibc-static
   ```

   Finally, remove the line you appended to /etc/yum.conf above.

2. Next execute the following commands:

   ```
   yum groupinstall "Development tools"
   yum install elfutils-devel
   yum install systemtap-runtime
   yum install zlib-devel
   yum install gettext
   yum install dejagnu
   yum install bison
   yum install flex
   yum install texinfo
   yum install sharutils
   yum install gmp-devel
   yum install mpfr-devel
   wget http://dl.fedoraproject.org/pub/epel/6/x86_64/libmpc-0.8-3.el6.x86_64.rpm
   wget http://dl.fedoraproject.org/pub/epel/6/x86_64/libmpc-devel-0.8-3.el6.x86_64.rpm
   rpm -Uvh libmpc-0.8-3.el6.x86_64.rpm
   rpm -Uvh libmpc-devel-0.8-3.el6.x86_64.rpm
   yum install rpm-build
   mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
   git clone https://github.com/tagged/bruce.git
   cp bruce/centos6/gcc482.spec ~/rpmbuild/SPECS
   wget http://mirrors.kernel.org/gnu/gcc/gcc-4.8.2/gcc-4.8.2.tar.bz2
   cp gcc-4.8.2.tar.bz2 ~/rpmbuild/SOURCES
   cd ~/rpmbuild
   rpmbuild --define "topdir `pwd`" -ba SPECS/gcc482.spec
   rpm -Uvh RPMS/x86_64/gcc482-4.8.2-1.el6.x86_64.rpm
   ```

Once gcc 4.8.2 has been built and installed, you should do the following before
attempting to build and execute programs with the new compiler version:
```
export PATH=/opt/gcc/bin:$PATH
export LD_LIBRARY_PATH=/opt/gcc/lib64
```

### Building and Installing Python 2.7

(content will be added here soon)

Now proceed to [build, install, and configure Bruce](https://github.com/tagged/bruce/blob/master/README.md#building-installing-and-configuring-bruce).
