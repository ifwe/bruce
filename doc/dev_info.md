## Modifying Bruce's Implementation

Before making code changes, it is helpful to become familiar with Bruce's build
system, which is based on [SCons](http://www.scons.org/).

### Build System

Files `SConstruct` and `src/SConscript` contain the build configuration.  As
shown
[here](https://github.com/tagged/bruce/blob/master/doc/build_install.md#building-bruce-directly),
to build something, first source the file `bash_defs` in the root of Bruce's
Git repository.  Then `cd` into the `src` directory or any directory beneath
`src` and use the `build` command to build a particular target.  In general,
to build an executable you simply specify its name when invoking the `build`
command.  For instance:

```
source bash_defs
cd src/bruce
build bruce  # builds bruce executable
build simple_client/simple_bruce_client  # builds command line client
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
have the Google Test Framework installed, as documented
[here](https://github.com/tagged/bruce/blob/master/doc/gtest.md).

If you type `build -c`, that will remove all build artifacts by deleting the
`out` directory.  For `make` users, this is the equivalent of `make clean`.
Alternatively, you can just type `rm -fr out` from the root of the Git
repository.  If you wish to change any compiler or linker flags, you can edit
the following part of the SConstruct file:

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
                  GENERATED_SOURCE_MAP={})

if GetOption('import_path'):
    env['ENV']['PATH'] = os.environ['PATH']


def set_debug_options():
    # Note: If you specify -fsanitize=address, you must also specify
    # -fno-omit-frame-pointer and be sure libasan is installed (RPM package
    # libasan on RHEL, Fedora, and CentOS).
    env.AppendUnique(CCFLAGS=['-g', '-fsanitize=address',
                              '-fno-omit-frame-pointer'])
    env.AppendUnique(CXXFLAGS=['-D_GLIBCXX_DEBUG',
                               '-D_GLIBCXX_DEBUG_PEDANTIC'])
    env.AppendUnique(LINKFLAGS=['-fsanitize=address'])


def set_release_options():
    env.AppendUnique(CCFLAGS=['-O2', '-DNDEBUG', '-Wno-unused',
                              '-Wno-unused-parameter', '-flto'])
    env.AppendUnique(LINKFLAGS=['-flto'])
```

Note that SCons build files are actually Python scripts, so you can add
arbitrary Python code.  Adding, removing or renaming source files does not
require any changes to the build scripts, since they are written to figure out
the dependencies on their own.  If you want to build all targets (or a
substantial subset of all targets) with a single command, you can execute the
`build_all` script in the root of the Git repository.  For instance,
`build_all run_tests` will build and run all unit tests.  Type
`build_all --help` for a full description of the command line options.
Eventually it would be nice to eliminate the `build_all` script and integrate
its functionality directly into the SCons configuration.

### Contributing Code

The coding conventions for Bruce are documented
[here](https://github.com/tagged/bruce/blob/master/doc/coding.md).  To
contribute code, create a pull request as described
[here](https://help.github.com/articles/using-pull-requests).  Use the "fork
and pull" model for making contributions.  A quick summary of the process is as
follows:

1. Fork Bruce's GitHub repository.  To fork the repo, click the
[fork button](http://github.com/tagged/bruce/fork) on the
[GitHub](http://github.com) web interface.

2. Clone the forked repository to your local machine.

3. Make your code changes in your local repository.  Within your own forked and
cloned repository, you are free to handle the code however you wish. Fork it,
tag it, commit it.  "Whatever works best for you" is the right answer to
"What's the process?" when you're in your own local repository.

4. Push your changes to your cloned repository.  Once you like how your changes
are functioning in your locally cloned repo, it's time to
[push them back up](https://help.github.com/articles/pushing-to-a-remote) to
your cloned repo on GitHub.

5. [Create a pull request](https://help.github.com/articles/using-pull-requests)
to the master branch of the bruce repository.

Information on getting help with Bruce is provided
[here](https://github.com/tagged/bruce#getting-help).
