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
   (content to be added here)
   ```

Now proceed to [build, install, and configure Bruce](https://github.com/tagged/bruce/blob/master/README.md#Building,-Installing,-and-Configuring-Bruce).
