#!/usr/bin/env python

# -----------------------------------------------------------------------------
# The copyright notice below is for the SCons build scripts included with this
# software, which were initially developed by Michael Park
# (see https://github.com/mpark/bob) and customized at if(we).  Many thanks to
# Michael for this useful contribution.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# The MIT License (MIT)
# Copyright (c) 2014 Michael Park
# Copyright (c) 2014 if(we)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# -----------------------------------------------------------------------------

import filecmp
import os
import shutil
import stat
import subprocess
import sys


def is_path_prefix(path1, path2):
    if path1 is None:
        return True

    c1 = path1.split('/')

    if c1[-1] == '':
        # Eliminate empty item after trailing '/'.
        c1 = c1[:-1]

    c2 = path2.split('/')

    if len(c1) > len(c2):
        return False

    for i in range(len(c1)):
        if c2[i] != c1[i]:
            return False

    return True


# Options.
AddOption('--test',
          action='store_true',
          help='Compile and execute unit tests.')
AddOption('--release',
          action='store_true',
          help='Build release targets (debug targets are built by default).')
AddOption('--import_path',
          action='store_true',
          help='Import PATH environment variable into build environment.')

# Our mode.
if GetOption('release') is None:
    mode = 'debug'
else:
    mode = 'release'

# Paths.
root = Dir('#')
src = root.Dir('src')
third_party = src.Dir('third_party')
tclap = third_party.Dir('tclap-1.2.0').Dir('include')
out_base = root.Dir('out')
out = out_base.Dir(mode)

if GetOption('clean'):
    try:
        shutil.rmtree(out_base.get_path())
    except OSError:
        pass

    sys.exit(0)

if not is_path_prefix(src.get_abspath(), GetLaunchDir()):
    sys.stderr.write('You must build from within the "src" part of the ' + \
            'tree, unless you are doing a "clean" operation (-c option).\n')
    sys.exit(1)

if not BUILD_TARGETS:
    print 'No build target specified'
    sys.exit(0)


# Look for Bruce's generated version file.  If the file exists and its version
# string is incorrect, delete it.  Deleting the file will cause the build to
# regenerate it with the correct version string.
def check_version_file(ver_file):
    check_ver = src.Dir('bruce').Dir('scripts').File('gen_version') \
            .get_abspath()

    try:
        subprocess.check_call([check_ver, '-c', ver_file]);
    except subprocess.CalledProcessError:
        sys.stderr.write(
            'Failed to execute script that checks generated version file ' + \
            ver_file +'\n')
        sys.exit(1)


check_version_file(out.Dir('bruce').File('build_id.c').get_abspath())
check_version_file(out.Dir('bruce').Dir('client').File('build_id.c').
        get_abspath())

# Environment.
prog_libs = {'pthread', 'dl', 'rt'}
gtest_libs = {'gtest', 'gtest_main', 'pthread'}
env = Environment(CFLAGS=['-Wwrite-strings'],
                  CCFLAGS=['-Wall', '-Wextra', '-Werror', '-Wformat=2',
                          '-Winit-self', '-Wunused-parameter', '-Wshadow',
                          '-Wpointer-arith', '-Wcast-align', '-Wlogical-op'],
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


# Append 'mode' specific environment variables.
{'debug'  : lambda: set_debug_options(),
 'release': lambda: set_release_options()
}[mode]()

# Export variables.
Export(['env', 'src', 'out'])

# Run the SConscript file.
env.SConscript(src.File('SConscript'), variant_dir=out, duplicate=False)
