## Setting Up the Google Test Framework

The following instructions assume that you are running on an x86-64
architecture CentOS or RHEL installation.  For Ubuntu installations (64 or 32
bit), or 32-bit CentOS/RHEL installations, the libraries would get copied to
`/usr/lib` rather than `/usr/lib64`.  To set up the Google Test Framework,
execute the following commands:

```
wget http://googletest.googlecode.com/files/gtest-1.7.0.zip
unzip gtest-1.7.0.zip
cd gtest-1.7.0
./configure
make
cd lib/.libs
mv *.a *.lai *.so* ../*.la /usr/lib64
ldconfig
cd ../..
cp -a include/gtest /usr/include
```

Now
[continue setting up your build environment](../README.md#setting-up-a-build-environment).

-----

gtest.md: Copyright 2014 if(we), Inc.

gtest.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
