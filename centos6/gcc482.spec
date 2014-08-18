%global multilib_64_archs x86_64
%global gcc_target_platform %{_target_platform}
%global _prefix /opt/gcc
%define debug_package %{nil}

Name:           gcc482
Version:        4.8.2
Release:        1%{?dist}
Summary:        Various compilers (C, C++, Objective-C, ...) 

License: 	GPLv3+ and GPLv3+ with exceptions and GPLv2+ with exceptions
Group: 		Development/Languages
URL:           	http://gcc.gnu.org 
Source0:        gcc-%{version}.tar.bz2
#Source1:	pr52634_0.c
#Source2:	pr52634_1.c
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

#Patch0: 	lto.cgraph.c.patch
#Patch1: 	lto.c.patch

BuildRequires: binutils >= 2.19.51.0.14-33
#BuildRequires: binutils-gold
BuildRequires: make

# While gcc doesn't include statically linked binaries, during testing
# -static is used several times.
BuildRequires: glibc-static
BuildRequires: zlib-devel, gettext, dejagnu, bison, flex, texinfo, sharutils
# For VTA guality testing
BuildRequires: gdb
BuildRequires: glibc-devel%{?_isa} >= 2.4.90-13
BuildRequires: elfutils-devel >= 0.72
# http://www.spinics.net/lists/gcchelp/msg39230.html
BuildRequires: systemtap-runtime >= 1.8-7
%ifarch %{multilib_64_archs}
BuildRequires: /lib/libc.so.6 /usr/lib/libc.so /lib64/libc.so.6 /usr/lib64/libc.so
%endif
# Make sure gdb will understand DW_FORM_strp
#Requires: binutils-gold
Conflicts: gdb < 5.1-2
Requires: glibc-devel >= 2.2.90-12
Provides: gcc = %{version}
Provides: libgcc = %{version}
Provides: gcc-c++ = %{version}
Provides: libstdc++ = %{version}
Provides: libstdc++-devel = %{version}
Provides: libstdc++-static = %{version}
Provides: libstdc++-docs = %{version}
Provides: gcc-objc = %{version}
Provides: gcc-objc++ = %{version}
Provides: libobjc = %{version}
BuildRequires: gmp-devel >= 4.1.2-8, mpfr-devel >= 2.2.1, libmpc-devel
Provides: gcc-gfortran = %{version}
Provides: cpp = %{version}
Provides: libgfortran = %{version}
Provides: libgomp = %{version}
Provides: libmudflap = %{version}
Provides: libmudflap-devel = %{version}
Provides: libmudflap-static = %{version}
Requires(post): /sbin/install-info
Requires(preun): /sbin/install-info

%description
The gcc package contains the GNU Compiler Collection version %{version}.
You'll need this package in order to compile C code.

%prep
%setup -q -n gcc-%{version}

%build
#./configure --prefix=%{_prefix} \
#	--enable-bootstrap --enable-shared --enable-threads=posix \
#	--enable-checking=release --with-system-zlib --enable-__cxa_atexit \
#	--disable-libunwind-exceptions --enable-gnu-unique-object \
#	--enable-languages=c,c++,objc,obj-c++,fortran \
#        --with-plugin-ld=ld.gold --enable-gold --enable-plugin \
./configure --prefix=%{_prefix} \
	--enable-bootstrap --enable-shared --enable-threads=posix \
	--enable-checking=release --with-system-zlib --enable-__cxa_atexit \
	--disable-libunwind-exceptions --enable-gnu-unique-object \
	--enable-languages=c,c++,objc,obj-c++,fortran
#%ifarch %{ix86} x86_64
#        --with-tune=generic \
#%endif
%ifarch %{ix86}
        --with-arch=i686 \
%endif
#%ifarch x86_64
#        --with-arch_32=i686 \
#%endif
#%ifnarch sparc sparcv9 ppc
#        --build=%{gcc_target_platform}
#%endif

make -j %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files 
%defattr(-,root,root,-)
%{_prefix}
%docdir /opt/gcc/share/man
%docdir /opt/gcc/share/info


%changelog
