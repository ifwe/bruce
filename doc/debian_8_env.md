## Setting Up a Debian 8 Build Environment

These instructions are based on limited familiarity with Debian, and can
probably be improved a lot.  Suggestions for improvements would be appreciated.

First use `apt-get` to install packages as follows:

```
sudo apt-get install build-essential
sudo apt-get install cmake
sudo apt-get install libasan0
sudo apt-get install git
```

Next use the `dpkg -i <some package>.deb` command to install the following
packages, which are found on the [DVD images for installing Debian]
(https://www.debian.org/CD/http-ftp/):

* Scons.  For instance, you can install the following package from the Debian
  8.2.0 DVD #2: `pool/main/s/scons/scons_2.3.1-2_all.deb`
* libsnappy1.  For instance, you can install the following package from the
  Debian 8.2.0 DVD #2: `pool/main/s/snappy/libsnappy1_1.1.2-3_amd64.deb`
* libsnappy-dev.  For instance, you can install the following package from the
  Debian 8.2.0 DVD #3: `pool/main/s/snappy/libsnappy-dev_1.1.2-3_amd64.deb`
* Boost C++ libraries.  For instance, you can install the following package
  from the Debian 8.2.0 DVD #2:
  `pool/main/b/boost1.55/libboost1.55-dev_1.55.0+dfsg-3_amd64.deb`

Now proceed to
[build, install, and configure Bruce](../README.md#building-and-installing-bruce).

-----

debian_8_env.md: Copyright 2016 Dave Peterson <dave@dspeterson.com>

debian_8_env.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
