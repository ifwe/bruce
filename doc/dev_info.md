## Modifying Bruce's Implementation

Before making code changes, it is helpful to become familiar with Bruce's build
system, which is based on [SCons](http://www.scons.org/).

### Build System

Files [SConstruct](../SConstruct) and [src/SConscript](../src/SConscript)
contain the build configuration.  As shown
[here](build_install.md#building-bruce-directly), to build something, first
source the file [bash_defs](../bash_defs) in the root of Bruce's Git
repository.  Then `cd` into the `src` directory or any directory beneath `src`
and use the `build` command to build a particular target.  In general, to build
an executable you simply specify its name when invoking the `build` command.
For instance:

```
source bash_defs
cd src/bruce
build bruce  # builds bruce executable
build client/simple_bruce_client  # builds command line client
build msg.o  # compile source file msg.cc
build conf/conf.o  # compile source file conf/conf.cc
```

All build results are in the `out` directory (relative to the root of the Git
repository).  For instance, the built `bruce` executable will be
`out/debug/bruce/bruce`.  To build a release version of a target, use the
`--release` option with the `build` command.  For instance,
`build --release bruce` will build a release version of `bruce`, which will be
`out/release/bruce/bruce`.

Source files ending in `.test.cc` are unit tests, which can be executed as
standalone executables.  For instance, in the above example if we typed
`build input_thread.test`, this would build the unit test for Bruce's input
thread, which would then appear as executable file
`out/debug/bruce/input_thread.test`.  If you type
`build --test input_thread.test`, that will build the unit test and then
immediately execute it.  Before building or running any unit tests, you must
have the Google Test Framework installed, as documented [here](gtest.md).

If you type `build -c`, that will remove all build artifacts by deleting the
`out` directory.  For `make` users, this is the equivalent of `make clean`.
Alternatively, you can just type `rm -fr out` from the root of the Git
repository.  If you wish to change any compiler or linker flags, look for the
section of code in the SConstruct file that looks roughly like this:

```Python
# Environment.
prog_libs = {'pthread', 'dl', 'rt'}
gtest_libs = {'gtest', 'gtest_main', 'pthread'}
env = Environment(CCFLAGS=['-Wall', '-Wextra', '-Werror'],
                  CPPDEFINES=[('SRC_ROOT', '\'"' + src.abspath + '"\'')],
                  CPPPATH=[src, tclap],
                  CXXFLAGS=['-std=c++11', '-Wold-style-cast'],
                  DEP_SUFFIXES=['.cc', '.cpp', '.c', '.cxx', '.c++', '.C'],
                  PROG_LIBS=[lib for lib in prog_libs],
                  TEST_LIBS=[lib for lib in prog_libs | gtest_libs],
                  TESTSUFFIX='.test',
                  GENERATED_SOURCE_MAP={},
                  LIB_HEADER_MAP={})

if GetOption('import_path'):
    env['ENV']['PATH'] = os.environ['PATH']


def set_debug_options():
    # Note: If you specify -fsanitize=address, you must also specify
    # -fno-omit-frame-pointer and be sure libasan is installed (RPM package
    # libasan on RHEL, Fedora, and CentOS).
    env.AppendUnique(CCFLAGS=['-g', '-fsanitize=address',
                              '-fno-omit-frame-pointer',
                              '-fvisibility=hidden'])
    env.AppendUnique(CXXFLAGS=['-D_GLIBCXX_DEBUG',
                               '-D_GLIBCXX_DEBUG_PEDANTIC'])
    env.AppendUnique(LINKFLAGS=['-fsanitize=address', '-rdynamic'])


def set_release_options():
    env.AppendUnique(CCFLAGS=['-O2', '-DNDEBUG', '-Wno-unused',
                              '-Wno-unused-parameter', '-flto',
                              '-fvisibility=hidden'])
    env.AppendUnique(LINKFLAGS=['-flto', '-rdynamic'])
```

Note that SCons build files are actually Python scripts, so you can add
arbitrary Python code.  Adding, removing or renaming source files does not
require any changes to the build scripts, since they are written to figure out
the dependencies on their own.  If you want to build all targets (or a
substantial subset of all targets) with a single command, you can execute the
[build_all](../build_all) script in the root of the Git repository.  For
instance, `build_all run_tests` will build and run all unit tests.  Type
`build_all --help` for a full description of the command line options.
Eventually it would be nice to eliminate the `build_all` script and integrate
its functionality directly into the SCons configuration.

### Debug Builds

The GNU C++ library provides a
[debug mode](https://gcc.gnu.org/onlinedocs/libstdc++/manual/debug_mode.html)
which implements various assertion checks for STL containers.  Bruce makes use
of this in its debug build. A word of caution is therefore necessary. Suppose
you have the following piece of code:

```C++
/* Do something interesting to an array of int values.  'begin' points to the
   beginning of the array and 'end' points one position past the last element.
   'begin' and 'end' will be equal in the case of an empty input. */
void DoSomethingToIntArray(int *begin, int *end) {
  assert(begin || (end == begin));
  assert(end >= begin);
  // do something interesting ...
}

void foo(std::vector<int> &v) {
  DoSomethingToIntArray(&v[0], &v[v.size()]);
}
```

The above code is totally legitimate C++.  However, the expression
`&v[v.size()]` will cause an out of range vector index to be reported when
running with debug mode enabled.  In fact, the expression `&v[0]` is enough to
cause an error to be reported in the case where `v` is empty.  Therefore the
code needs to be written a bit differently to avoid spurious errors in debug
builds. For instance, one might instead implement foo() like this:

```C++
void foo(std::vector<int> &v) {
  if (!v.empty()) {
    DoSomethingToIntArray(&v[0], &v[0] + v.size());
  }
}
```

Although this is a bit less elegant than the previous implementation, the
benefits of tools such as debug mode can be great when tracking down problems.
Therefore please avoid code such as the first version of `foo()` when making
changes to Bruce.  In GCC 4.8, support was added for
[AddressSanitizer](http://code.google.com/p/address-sanitizer/), another useful
debugging tool.  This is enabled in debug builds of Bruce.

### Contributing Code

Information on contributing to Bruce is provided [here](../CONTRIBUTING.md).

Information on getting help with Bruce is provided
[here](../README.md#getting-help).

-----

dev_info.md: Copyright 2014 if(we), Inc.

dev_info.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
