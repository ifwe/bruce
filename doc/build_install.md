## Building an RPM package

Once you have finished [setting up your build environment](https://github.com/tagged/bruce#setting-up-a-build-environment),
you are ready to build Bruce.  If you are building on CentOS 6, remember to set
your `PATH` and `LD_LIBRARY_PATH` environment variables and activate your
Python virtualenv environment before building, as detailed
[here](https://github.com/tagged/bruce#setting-up-a-build-environment).  The
first step is to clone Bruce's Git repository:

```
git clone https://github.com/tagged/bruce.git
```

If you will be running Bruce on an RPM-based
distribution such as CentOS or RHEL, you will probably want to build an RPM
package.  Here you have two choices.  You can build either an RPM package that
includes bruce's init script and config files or an RPM package that omits
these files.  You might prefer the latter choice if you prefer to manage the
config files separately using a tool such as Puppet.  To choose the first
option, do the following:

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
