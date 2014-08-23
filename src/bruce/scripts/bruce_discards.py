#!/usr/bin/env python

###############################################################################
# -----------------------------------------------------------------------------
# Copyright 2013-2014 Tagged
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------
#
# This script requests a discard report from bruce, analyzes it, stores it in
# an Oracle database, writes it to standard output if a problem was found, and
# exits with the appropriate Nagios code.  All errors are written to standard
# output rather than standard error, since that is what Nagios expects.
#
# Program arguments:
#    -v
#    --verbose: (optional)
#        Enable verbose output.
#
#    -w DIR
#    --work_dir DIR: (required)
#        Specifies a directory under the Nagios user's home directory where the
#        script keeps all of its data files.  This directory will be created if
#        it does not already exist.
#
#    -s SERVER
#    --nagios_server SERVER: (required)
#        This is a unique identifier for the Nagios server that triggered the
#        current execution of this script.
#
#    -a AGE
#    --max_report_age AGE: (optional, default = 28800)
#        If any old discard report files exceed this age in seconds, the
#        situation is treated as Warning.  This handles cases where the
#        database is down for an unusually long time.
#
#    -u USER
#    --nagios_user USER: (optional, default = 'nrpe')
#        The name of the nagios user.  The script will create a directory under
#        the nagios user's home directory (see '-d DIR' option above).
#
#    -b HOST
#    --bruce_host HOST: (required)
#        The host running bruce that we should connect to for discard data.
#        Host may be specified by name or address.  Regardless of whether
#        name or address is specified, this script will convert the host to a
#        canonicalized hostname.  For instance, 'web001' will be converted to
#        'web001.tagged.com'.  Likewise, the IP address for web001 will be
#        converted to 'web001.tagged.com'.
#
#    -p PORT
#    --bruce_status_port PORT: (optional, default = 9090)
#        The port to connect to when asking bruce for discard data.
#
#    -t TESTFILE
#    --testfile TESTFILE: (optional)
#        This is for testing.  If specified, the discard report is obtained
#        from file TESTFILE rather than connecting to bruce.  When using this
#        option, you must still specify the host running bruce (-b option),
#        since the host is stored with the discard information.
#
#    -D DBHOST
#    --dbhost DBHOST: (optional)
#        Specifies that Oracle database to write discard reports to is running
#        on host DBHOST.
#
#    -P DBPORT
#    --dbport DBPORT: (optional, default = 1521)
#        Specifies port that Oracle server is listening on.
#
#    -d DATABASE
#    --database DATABASE: (required if -D option is specified)
#        Specifies the database name to use for storing discard reports.
#
#    -T SECONDS
#    --database_timeout SECONDS: (optional, default = 15)
#        Specifies a timeout for storing a discard report in the database.
#
#    -c CREDENTIALS_FILE
#    --credentials CREDENTIALS_FILE: (required if -D option is specified)
#        Specifies a file containing the username and password to use for
#        database access.  The file is expected to contain JSON that looks like
#        this:
#
#            { "password": "quack", "username": "daffyduck" }
###############################################################################

import cx_Oracle
import errno
import getopt
import json
import os
import signal
import socket
import subprocess
import sys
import time
import types

from urllib2 import URLError
from urllib2 import urlopen

# Nagios exit codes
EC_SUCCESS = 0
EC_WARNING = 1
EC_CRITICAL = 2
EC_UNKNOWN = 3

# This will contain program options information.
Opts = None

###############################################################################
# Print a message and exit with the given exit code.
###############################################################################
def Die(exit_code, msg):
    print msg
    sys.exit(exit_code)
###############################################################################

###############################################################################
# Create the directory given by 'path' if it doesn't already exist.
###############################################################################
def MakePathExist(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            Die(EC_UNKNOWN, 'Failed to create directory ' + path + ': ' +
                e.strerror)
###############################################################################

###############################################################################
# Convert the given Nagios exit code to a string and return the result.
###############################################################################
def NagiosCodeToString(nagios_code):
    if nagios_code == EC_SUCCESS:
        return 'Success'

    if nagios_code == EC_WARNING:
        return 'Warning'

    if nagios_code == EC_CRITICAL:
        return 'Critical'

    if nagios_code != EC_UNKNOWN:
        Die(EC_UNKNOWN, 'Cannot convert unknown Nagios code ' +
            str(nagios_code) + ' to string')

    return 'Unknown'
###############################################################################

###############################################################################
# Return the home directory of the Nagios user as a string.
###############################################################################
def GetNagiosDir():
    try:
        p = subprocess.Popen(['/bin/bash', '-c',
                             'echo -n ~' + Opts.NagiosUser],
                             stdout=subprocess.PIPE)
        out, err = p.communicate()
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to execute shell command to determine '
            'Nagios home directory: ' + e.strerror)

    if not out:
        Die(EC_UNKNOWN, 'Got empty result from shell command to determine '
            'Nagios home directory')

    if out[0] == '~':
        Die(EC_UNKNOWN, 'Nagios home directory not found')

    if out[0] != '/':
        Die(EC_UNKNOWN, 'Got strange output while trying to determine '
            'Nagios home directory: [' + out + ']')

    return out
###############################################################################

###############################################################################
# Return the number of seconds since the epoch.
###############################################################################
def GetEpochSeconds():
    return int(time.time())
###############################################################################

###############################################################################
# Find all names of files in the directory given by 'path' that may contain
# discard reports based on their names.  Return the filenames converted to
# integers (each representing seconds since the epoch) as a list.  The returned
# list will be sorted in ascending order.
###############################################################################
def FindDiscardFileTimes(path):
    try:
        file_list = os.listdir(path)
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to list contents of directory ' + path + ': ' +
            e.strerror)
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to list contents of directory ' + path + ': ' +
            e.strerror)

    result = [ ]

    for item in file_list:
        try:
            value = int(item)
        except ValueError:
            # The filename can't be converted to an integer, so it must not be
            # a discard report file.
            continue

        if value >= 0:
            result.append(value)

    result.sort()
    return result
###############################################################################

###############################################################################
# Return True if 'address' is a valid IPV4 address.  Else return False.
###############################################################################
def IsValidIpv4Address(address):
    try:
        socket.inet_pton(socket.AF_INET, address)
    except AttributeError:  
        try:
            socket.inet_aton(address)
        except socket.error:
            return False

        return address.count('.') == 3
    except socket.error:  
        return False

    return True
###############################################################################

###############################################################################
# Return True if 'address' is a valid IPV6 address.  Else return False.
###############################################################################
def IsValidIpv6Address(address):
    try:
        socket.inet_pton(socket.AF_INET6, address)
    except socket.error:  
        return False

    return True
###############################################################################

###############################################################################
# 'host' may be either a hostname or a valid IP address (IPV4 or IPV6).  Return
# the host represented as a canonicalized hostname.  For instance, for input
# 'web001', this function will return 'web001.tagged.com'.  Likewise, if the IP
# address of web001 is given as input, this function returns
# 'web001.tagged.com'.  Throw a subclass of socket.error if there is a failure
# during name or address lookup.
###############################################################################
def CanonicalizeHost(host):
    if IsValidIpv4Address(host) or IsValidIpv6Address(host):
        return socket.gethostbyaddr(host)[0]

    return socket.gethostbyaddr(socket.gethostbyname(host))[0]
###############################################################################

###############################################################################
# Send a "get discards" HTTP request to bruce and return the response as a list
# of lines of text with trailing newline characters preserved.
###############################################################################
def GetDiscardResponse(url):
    try:
        response = urlopen(url)
    except URLError as e:
        # Treat this as Critical, since it indicates that bruce is probably not
        # running.
        Die(EC_CRITICAL, 'Failed to open discards URL: ' + str(e.reason))

    result = [ ]

    for line in response:
        result.append(line)

    return result
###############################################################################

###############################################################################
# Class for storing an unsupported message version number and the associated
# count of messages of that version.  A discard report contains a possibly
# empty list of these.
#
# members:
#     MsgVersion: The the unsupported message version.
#     MsgCount: A count of messages of that version.
###############################################################################
class TUnsupportedMsgVersionInfo(object):
    'class for storing unsupported message version info'
    def __init__(self, msg_version, msg_count):
        self.MsgVersion = msg_version
        self.MsgCount = msg_count

    def __str__(self):
        return 'version: ' + str(self.MsgVersion) + ' count: ' + \
                str(self.MsgCount)
###############################################################################

###############################################################################
# Class for storing error information (either discards or possible duplicate
# messages) for a topic.  A discard report contains a possibly empty list of
# these representing per-topic discard information and a possibly empty list of
# these representing per-topic possible duplicate message information.
#
# members:
#     Topic: The Kafka topic.
#     Timestamp1: The earliest message timestamp in a range of messages.  This
#         is an integer value whose units are opaque from bruce's point of
#         view.  Analytics intends to use milliseconds since the epoch.
#     Timestamp2: The latest message timestamp in a range of messages.  This is
#         an integer value whose units are the same as for 'Timestamp1'.
#     Count: The number of messages in the range.
###############################################################################
class TTopicErrorInfo(object):
    'class for storing discard or possible duplicate message information'
    def __init__(self, topic, timestamp_1, timestamp_2, count):
        self.Topic = topic
        self.Timestamp1 = timestamp_1
        self.Timestamp2 = timestamp_2
        self.Count = count

    def __str__(self):
        return 'topic: [' + self.Topic + '] begin ' + \
                str(self.Timestamp1) + ' end ' + str(self.Timestamp2) + \
                ' count ' + str(self.Count)
###############################################################################

###############################################################################
# Class for storing a discard report
#
# members:
#     Host: Name of host where report came from.
#     Port: The port bruce listens on for HTTP status requests.  In the case
#         where multiple instances of bruce run on a single host, this
#         distinguishes the instances.
#     BruceReportId: ID assigned to report by bruce.  For a given host, this
#         value is _not_ guaranteed unique, since bruce will reuse IDs if it is
#         restarted.
#     ReportStart: The start time of the report in seconds since the epoch.
#     ReportEnd: The end time of the report in seconds since the epoch.  The
#         report contains events each recorded at some time t where
#         ReportStart <= t < ReportEnd.  In other words, ReportEnd is the start
#         of the next reporting period.
#     MalformedMsgCount: The number of malformed messages seen.
#     UnsupportedVersionMsgCount: The number of messages seen with a version
#         number that is unsupported.
#     BadTopicMsgCount: The number of messages seen with a bad topic.
#     BrucePid: The process ID of the bruce daemon that the report came from.
#     MalformedMsgList: A possibly empty list of strings representing malformed
#         messages.  For malformed messages exceeding N bytes, only the first N
#         bytes are stored.
#     TooLongMsgList: A possibly empty list of strings representing prefixes of
#         messages that are too long.
#     UnsupportedVersionInfoList: A possibly empty list of
#         TUnsupportedMsgVersionInfo objects.
#     BadTopicList: A possibly empty list of strings representing bad topics.
#     DiscardTopicInfoList: A possibly empty list of TTopicErrorInfo objects
#         representing per-topic information on discarded messages.
#     DupTopicInfoList: A possibly empty list of TTopicErrorInfo objects
#         representing per-topic information on possible duplicate messages.
###############################################################################
class TDiscardReport(object):
    'class for storing a discard report'
    def __init__(self, host, port, bruce_report_id, report_start, report_end,
                 malformed_msg_count, unsupported_api_key_msg_count,
                 unsupported_version_msg_count, bad_topic_msg_count, bruce_pid,
                 malformed_msg_list, too_long_msg_list,
                 unsupported_version_info_list, bad_topic_list,
                 discard_topic_info_list, dup_topic_info_list):
        self.Host = host
        self.Port = port
        self.BruceReportId = bruce_report_id
        self.ReportStart = report_start
        self.ReportEnd = report_end
        self.MalformedMsgCount = malformed_msg_count
        self.UnsupportedApiKeyMsgCount = unsupported_api_key_msg_count
        self.UnsupportedVersionMsgCount = unsupported_version_msg_count
        self.BadTopicMsgCount = bad_topic_msg_count
        self.BrucePid = bruce_pid
        self.MalformedMsgList = malformed_msg_list
        self.TooLongMsgList = too_long_msg_list
        self.UnsupportedVersionInfoList = unsupported_version_info_list
        self.BadTopicList = bad_topic_list
        self.DiscardTopicInfoList = discard_topic_info_list
        self.DupTopicInfoList = dup_topic_info_list

    def __str__(self):
        result = '    host: ' + self.Host + '\n' + \
                '    port: ' + str(self.Port) + '\n' + \
                '    bruce PID: ' + str(self.BrucePid) + '\n' + \
                '    report ID: ' + str(self.BruceReportId) + '\n' + \
                '    start time: ' + str(self.ReportStart) + '\n' + \
                '    end time: ' + str(self.ReportEnd) + '\n' + \
                '    malformed msg count: ' + str(self.MalformedMsgCount) + \
                '\n' + \
                '    unsupported API key msg count: ' + \
                str(self.UnsupportedApiKeyMsgCount) + '\n' + \
                '    unsupported version msg count: ' + \
                str(self.UnsupportedVersionMsgCount) + '\n' + \
                '    bad topic msg count: ' + \
                str(self.BadTopicMsgCount) + '\n' + \
                '    malformed msg list:\n'

        for msg in self.MalformedMsgList:
            result += '        [' + msg + ']\n'

        result += '    too long msg list:\n'

        for msg in self.TooLongMsgList:
            result += '        [' + msg + ']\n'

        result += '    unsupported version msg list:\n'

        for obj in self.UnsupportedVersionInfoList:
            result += '        ' + str(obj) + '\n'

        result += '    bad topic list:\n'

        for t in self.BadTopicList:
            result += '        [' + t + ']\n'

        result += '    discard topic list:\n'

        for obj in self.DiscardTopicInfoList:
            result += '        ' + str(obj) + '\n'

        result += '    possible duplicate topic list:\n'

        for obj in self.DupTopicInfoList:
            result += '        ' + str(obj) + '\n'

        return result
###############################################################################

###############################################################################
# Class for iterating over lines of a response from bruce.
###############################################################################
class TResponseLineIterator(object):
    'class for iterating over lines from a response sent by bruce'
    def __init__(self, response):
        self.Response = response
        self.NextLineIndex = 0

    # Return the next line, or None if there are no more lines available.  If
    # True is specified for 'skip_blank_lines', then lines that are empty or
    # contain only whitespace will be skipped.  If True is specified for
    # 'strip_newline', the returned line will have its trailing newline
    # character (if any) removed.  If True is specified for 'lstrip_ws', then
    # the returned line will have leading whitespace removed.
    def GetNextLine(self, skip_blank_lines, strip_newline, lstrip_ws):
        while self.NextLineIndex < len(self.Response):
            if skip_blank_lines and \
                    not self.Response[self.NextLineIndex].strip():
                self.NextLineIndex += 1
                continue

            index = self.NextLineIndex
            self.NextLineIndex += 1
            result = self.Response[index]

            if strip_newline:
                result = result.rstrip('\n')

            if lstrip_ws:
                result = result.lstrip()

            return result

        return None

    # Rewind the iterator to the previous line.  Note that this method does
    # _not_ rewind past any blank lines skipped during the previous call to
    # GetNextLine().
    def RewindOneLine(self):
        if self.NextLineIndex > 0:
            self.NextLineIndex -= 1
###############################################################################

###############################################################################
# On entry, it is assumed that the next line provided by 'response_iter' will
# be a line whose form looks something like this:
#
#     report ID: 999
#
# or like this:
#
#     start time: 1396656791 Fri Apr  4 17:13:11 2014
#
# In the first case, parameter 'prefix_blurb' would be set to 'report ID: ' and
# the integer value 999 would be returned.
#
# In the second case, parameter 'prefix_blurb' would be set to 'start time: '
# and 'extra_junk_after_value_ok' would be set to True (indicating that the
# ' Fri Apr  4 17:13:11 2014' should be ignored).  The integer value 1396656791
# would be returned.
#
# In case of fatal error, 'error_blurb' is included in the error message.
###############################################################################
def GetNextIntValueFromResponse(response_iter, prefix_blurb, error_blurb,
                                extra_junk_after_value_ok):
    line = response_iter.GetNextLine(True, True, True)

    if line == None:
        Die(EC_UNKNOWN, 'discard response is truncated')

    if line[:len(prefix_blurb)] != prefix_blurb:
        Die(EC_UNKNOWN, 'discard response is invalid: \'' + prefix_blurb + \
            '\' expected')

    # Remove the prefix blurb.  Then the integer value we want will be next.
    line = line[len(prefix_blurb):]

    if extra_junk_after_value_ok:
        index = line.find(' ')

        if index != -1:
            # Remove extra junk after integer value.
            line = line[:index]

    try:
        value = int(line)
    except ValueError:
        Die(EC_UNKNOWN, 'discard response has invalid ' + error_blurb)

    if value < 0:
        Die(EC_UNKNOWN, 'discard response has negative ' + error_blurb)

    return value
###############################################################################

###############################################################################
# Try to process the next nonempty line of input provided by 'response_iter' as
# a string value of the following form:
#
#     recent bad topic: 3[duh]
#
# In the above case, parameter 'prefix_blurb' would be set to
# 'recent bad topic: ' and we would return the tuple ('duh', extra_junk) where
# 'extra_junk' is a possibly empty string containing any extra bytes following
# the closing square bracket.  The value 3 to the left of the opening square
# bracket indicates the length in bytes of the enclosed string.  Note that the
# string between the square brackets may contain embedded newline characters,
# resulting in a returned string that consists of multiple lines.
#
# If the next nonempty input line does not fit the above pattern, then rewind
# it one position and return None.
#
# If there are no more input lines, then return None.
#
# In case of fatal error, 'error_blurb' is included in the error message.
###############################################################################
def GetNextStringValueFromResponse(response_iter, prefix_blurb, error_blurb):
    line = response_iter.GetNextLine(True, False, True)

    if line == None:
        return None

    if line[:len(prefix_blurb)] != prefix_blurb:
        response_iter.RewindOneLine()
        return None

    # Remove the prefix blurb.
    line = line[len(prefix_blurb):]

    index = line.find('[')

    if index == -1:
        Die(EC_UNKNOWN, 'malformed input for ' + error_blurb)

    try:
        # Get length value immediately preceding opening square bracket.
        input_len = int(line[:index])
    except ValueError:
        Die(EC_UNKNOWN, 'malformed input for ' + error_blurb)

    if input_len < 0:
        Die(EC_UNKNOWN, 'malformed input for ' + error_blurb)

    # Remove everything through opening square bracket.  Then the string we
    # want is next, followed by the closing square bracket.
    line = line[index + 1:]

    result = ''

    if len(result) < input_len:
        # We iterate to handle the case where the string has one or more
        # embedded newline characters, and is therefore spread across multiple
        # lines.
        while True:
            remaining = input_len - len(result)
            available = len(line)

            if available == 0:
                Die(EC_UNKNOWN, 'malformed input for ' + error_blurb)

            num_bytes = min(remaining, available)
            result = result + line[:num_bytes]
            line = line[num_bytes:]

            if len(result) == input_len:
                if line == '':
                    line = response_iter.GetNextLine(False, False, False)

                    if line == None:
                        Die(EC_UNKNOWN, 'malformed input for ' + error_blurb)

                break

            line = response_iter.GetNextLine(False, False, False)

    if line == '':
        Die(EC_UNKNOWN, 'malformed input for ' + error_blurb)

    if line[0] != ']':
        Die(EC_UNKNOWN, 'malformed input for ' + error_blurb)

    line = line.rstrip('\n')
    extra_junk = line[1:]
    return (result, extra_junk)
###############################################################################

###############################################################################
# Try to process the next nonempty line of input provided by 'response_iter' as
# a string value of the form expected by GetNextStringValueFromResponse(), and
# continue processing input lines until a line not matching this form is
# encountered or there are no more input lines.  We impose an extra requirement
# that no input item recognized by GetNextStringValueFromResponse() contains
# extra junk following the closing square bracket, and treat a violation of
# this requirement as a fatal error.  Return a list of all such string values
# found, which will be empty in the case where none were found.  Once a
# nonempty line that doesn't fit what we are looking for is encountered,
# 'response_iter' will be rewinded to the previous line before we return our
# result.  In case of fatal error, 'error_blurb' is included in the error
# message.
###############################################################################
def GetStringValueSequence(response_iter, prefix_blurb, error_blurb):
    # This is a list of strings.
    result = [ ]

    while True:
        next_value = GetNextStringValueFromResponse(response_iter,
                prefix_blurb, error_blurb)

        if next_value == None:
            break

        if next_value[1] != '':
            Die(EC_UNKNOWN, 'extra junk at end of input for ' + error_blurb + \
                ': ' + next_value[1])

        result.append(next_value[0])

    return result
###############################################################################

###############################################################################
# The behavior of this function is the same as for GetStringValueSequence()
# except for the following differences:
#
#     1.  Extra junk after the closing square bracket is allowed.
#
#     2.  Instead of returning a list of strings, we return a list of 2-item
#         tuples.  The first item is the string (that was enclosed by square
#         brackets in the input), and the second item is a possibly empty
#         string containing any trailing junk found after the closing square
#         bracket.
###############################################################################
def GetStringValueSequenceWithExtraJunk(response_iter, prefix_blurb,
        error_blurb):
    # This is a list of 2-item tuples, as returned by
    # GetNextStringValueFromResponse().
    result = [ ]

    while True:
        next_value = GetNextStringValueFromResponse(response_iter,
                prefix_blurb, error_blurb)

        if next_value == None:
            break

        result.append(next_value)

    return result
###############################################################################

###############################################################################
# Try to process the next nonempty line of input provided by 'response_iter' as
# an unsupported message version line of the following form:
#
#     unsupported msg version: V count: C
#
# Where V is an integer and C is a nonnegative integer.  If such a line is
# found, return a TUnsupportedMsgVersionInfo object representing the given
# information.
#
# If the next nonempty input line does not fit the above pattern, then rewind
# it one position and return None.
#
# If there are no more input lines, then return None.
###############################################################################
def GetUnsupportedVersionInfo(response_iter):
    line = response_iter.GetNextLine(True, True, True)

    if line == None:
        return None

    prefix_blurb = 'unsupported msg version: '

    if line[:len(prefix_blurb)] != prefix_blurb:
        response_iter.RewindOneLine()
        return None

    # Remove prefix blurb.
    line = line[len(prefix_blurb):]

    index = line.find(' ')

    if index == -1:
        Die(EC_UNKNOWN, 'malformed unsupported msg version line')

    try:
        version = int(line[:index])
    except ValueError:
        Die(EC_UNKNOWN, 'unsupported msg version line has invalid version')

    line = line[index + 1:]
    s = 'count: '

    if line[:len(s)] != s:
        Die(EC_UNKNOWN, 'malformed unsupported msg version line')

    # Remove everything preceding the count.
    line = line[len(s):]

    try:
        count = int(line)
    except ValueError:
        Die(EC_UNKNOWN, 'unsupported msg version line has invalid count')

    if count < 0:
        Die(EC_UNKNOWN, 'unsupported msg version line has negative count')

    return TUnsupportedMsgVersionInfo(version, count)
###############################################################################

###############################################################################
# Try to process the next nonempty line of input provided by 'response_iter' as
# a line of the form expected by GetUnsupportedVersionInfo(), and continue
# processing input lines until a line not matching this form is encountered or
# there are no more input lines.  Return a list of TUnsupportedMsgVersionInfo
# objects representing all such lines found, which will be empty in the case
# where none were found.  Once a nonempty line that doesn't fit what we are
# looking for is encountered, 'response_iter' will be rewinded to the previous
# line before we return our result.
###############################################################################
def GetUnsupportedVersionInfoList(response_iter):
    # This is a list of 2-item tuples, where the first tuple item is a message
    # version and the second tuple item is a count of messages with that
    # version.
    result = [ ]

    while True:
        next_value = GetUnsupportedVersionInfo(response_iter)

        if next_value == None:
            break

        result.append(next_value)

    return result
###############################################################################

###############################################################################
# Try to process the next nonempty line of input provided by 'response_iter' as
# something that looks like this:
#
#     discard topic: 12[space aliens] begin [333] end [444] count 567
#
# In the above example, input parameter 'prefix_blurb' would be set to the
# string 'discard topic: '.  If such a line is found, return a TTopicErrorInfo
# object representing the given information.
#
# If the next nonempty input line does not fit the above pattern, then rewind
# it one position and return None.
#
# If there are no more input lines, then return None.
#
# Note that embedded newline characters within topics are supported, so a
# single input item may span multiple lines.
#
# In case of fatal error, 'error_blurb' is included in the error message.
###############################################################################
def GetTopicRangeAndCount(response_iter, prefix_blurb, error_blurb):
    topic_and_junk = GetNextStringValueFromResponse(response_iter,
            prefix_blurb, error_blurb)

    if topic_and_junk == None:
        return None

    topic = topic_and_junk[0]

    # The junk is everything following the closing square bracket that marks
    # the end of the topic.
    junk = topic_and_junk[1]

    s = ' begin ['

    if junk[:len(s)] != s:
        Die(EC_UNKNOWN, 'malformed ' + error_blurb + ' line')

    junk = junk[len(s):]
    index = junk.find(']')

    if index == -1:
        Die(EC_UNKNOWN, 'malformed ' + error_blurb + ' line')

    try:
        # Get first time point in interval.
        interval_first = int(junk[:index])
    except ValueError:
        Die(EC_UNKNOWN, error_blurb + ' line has invalid interval first')

    junk = junk[index + 1:]
    s = ' end ['

    if junk[:len(s)] != s:
        Die(EC_UNKNOWN, 'malformed ' + error_blurb + ' line')

    junk = junk[len(s):]
    index = junk.find(']')

    if index == -1:
        Die(EC_UNKNOWN, 'malformed ' + error_blurb + ' line')

    try:
        # Get last time point in interval.
        interval_last = int(junk[:index])
    except ValueError:
        Die(EC_UNKNOWN, error_blurb + ' line has invalid interval last')

    junk = junk[index + 1:]
    s = ' count '

    if junk[:len(s)] != s:
        Die(EC_UNKNOWN, 'malformed ' + error_blurb + ' line')

    junk = junk[len(s):]

    try:
        # Get count of events occurring within interval.
        count = int(junk.rstrip())
    except ValueError:
        Die(EC_UNKNOWN, error_blurb + ' line has invalid count')

    return TTopicErrorInfo(topic, interval_first, interval_last, count)
###############################################################################

###############################################################################
# Try to process the next nonempty line of input provided by 'response_iter' as
# a line of the form expected by GetTopicRangeAndCount(), and continue
# processing input lines until a line not matching this form is encountered or
# there are no more input lines.  Return a list of TTopicErrorInfo objects
# representing all found items, which will be empty in the case where none were
# found.  Once a nonempty line that doesn't fit what we are looking for is
# encountered, 'response_iter' will be rewinded to the previous line before we
# return our result.
###############################################################################
def GetTopicRangeAndCountList(response_iter, prefix_blurb, error_blurb):
    # This is a list of 4-item tuples, as returned by GetTopicRangeAndCount().
    result = [ ]

    while True:
        next_value = GetTopicRangeAndCount(response_iter, prefix_blurb,
                error_blurb)

        if next_value == None:
            break

        result.append(next_value)

    return result
###############################################################################

###############################################################################
# On entry, we assume that the next nonempty line returned by 'response_iter'
# will be the start of a discard report.  Parse the report, and return a
# TDiscardReport object representing its contents.  Parameter 'host' gives the
# canonicalized hostname representing where the report came from.  Parameter
# 'bruce_pid' gives the process ID of the bruce daemon that produced the report
# on the given host.  Parameter 'report_interval' gives the reporting interval
# (time between consecutive reports) in seconds.  Input parameter 'port' gives
# the port bruce listens on for HTTP status requests.  In the case where
# multiple instances of bruce run on a single host, this distinguishes the
# instances.  On return, 'response_iter' will be at the first nonempty line
# following the report.
###############################################################################
def ParseOneDiscardReport(host, port, response_iter, bruce_pid,
        report_interval):
    bruce_report_id = GetNextIntValueFromResponse(response_iter, 'report ID: ',
                                         'report ID', False)
    start_time = GetNextIntValueFromResponse(response_iter, 'start time: ',
                                          'start time', True)
    malformed_msg_count = GetNextIntValueFromResponse(response_iter,
            'malformed msg count: ', 'malformed msg count', False)
    unsupported_api_key_msg_count = GetNextIntValueFromResponse(response_iter,
            'unsupported API key msg count: ', 'unsupported API key msg count',
            False)
    unsupported_version_msg_count = GetNextIntValueFromResponse(response_iter,
            'unsupported version msg count: ', 'unsupported version msg count',
            False)
    bad_topic_msg_count = GetNextIntValueFromResponse(response_iter,
            'bad topic msg count: ', 'bad topic msg count', False)

    malformed_msg_list = [ ]
    unsupported_version_info_list = [ ]
    bad_topic_list = [ ]
    too_long_msg_list = [ ]
    discard_topic_info_list = [ ]
    dup_topic_info_list = [ ]

    # Loop never iterates more than once.  We use 'break' inside loop to
    # achieve the effect of "goto statement following loop".  Gotos aren't evil
    # if used in a disciplined manner.
    while True:
        line = response_iter.GetNextLine(True, True, True)

        if line == None:
            break

        s = 'recent malformed msg: '

        if line[:len(s)] == s:
            response_iter.RewindOneLine()
            malformed_msg_list = GetStringValueSequence(response_iter, s,
                    'recent malformed msg')
            line = response_iter.GetNextLine(True, True, True)

            if line == None:
                break

        s = 'unsupported msg version: '

        if line[:len(s)] == s:
            response_iter.RewindOneLine()
            unsupported_version_info_list = \
                    GetUnsupportedVersionInfoList(response_iter)
            line = response_iter.GetNextLine(True, True, True)

            if line == None:
                break

        s = 'recent bad topic: '

        if line[:len(s)] == s:
            response_iter.RewindOneLine()
            bad_topic_list = GetStringValueSequence(response_iter, s,
                    'recent bad topic')
            line = response_iter.GetNextLine(True, True, True)

            if line == None:
                break

        s = 'recent too long msg: '

        if line[:len(s)] == s:
            response_iter.RewindOneLine()
            too_long_msg_list = GetStringValueSequence(response_iter, s,
                    'recent too long msg')
            line = response_iter.GetNextLine(True, True, True)

            if line == None:
                break

        s = 'discard topic: '

        if line[:len(s)] == s:
            response_iter.RewindOneLine()
            discard_topic_info_list = GetTopicRangeAndCountList(response_iter,
                    s, 'discard topic')
            line = response_iter.GetNextLine(True, True, True)

            if line == None:
                break

        s = 'possible duplicate topic: '

        if line[:len(s)] == s:
            response_iter.RewindOneLine()
            dup_topic_info_list = GetTopicRangeAndCountList(response_iter,
                    s, 'possible duplicate topic')
            line = response_iter.GetNextLine(True, True, True)

        break

    if line != None:
        response_iter.RewindOneLine()

    return TDiscardReport(host, port, bruce_report_id, start_time,
                          start_time + report_interval, malformed_msg_count,
                          unsupported_api_key_msg_count,
                          unsupported_version_msg_count, bad_topic_msg_count,
                          bruce_pid, malformed_msg_list, too_long_msg_list,
                          unsupported_version_info_list, bad_topic_list,
                          discard_topic_info_list, dup_topic_info_list)
###############################################################################

###############################################################################
# Input parameter 'response' is a list of input lines representing the contents
# of a discard response obtained from bruce.  The response should contain two
# discard reports: the current partially completed report and the latest
# finished report.  Parse the entire response and return a 2-item tuple
# containing the discard reports represented as TDiscardReport objects.  The
# first tuple item will be the current partially completed report, and the
# second item will be the latest finished report.  Input parameter 'port' gives
# the port bruce listens on for HTTP status requests.  In the case where
# multiple instances of bruce run on a single host, this distinguishes the
# instances.
###############################################################################
def ParseDiscardResponse(response, host, port):
    response_iter = TResponseLineIterator(response)
    bruce_pid = GetNextIntValueFromResponse(response_iter, 'pid: ',
                                            'bruce pid', False)
    GetNextIntValueFromResponse(response_iter, 'now: ', '\'now\' value', True)
    line = response_iter.GetNextLine(True, True, True)
    s = 'version:'

    if line[:len(s)] != s:
        Die(EC_UNKNOWN, 'discard response is invalid: \'' + s + '\' expected')

    report_interval = GetNextIntValueFromResponse(response_iter,
            'report interval in seconds: ', 'report interval', False)

    line = response_iter.GetNextLine(True, True, True)

    if line == None:
        Die(EC_UNKNOWN, 'discard response is truncated')

    s = 'current (unfinished) reporting period:'

    if line[:len(s)] != s:
        Die(EC_UNKNOWN, 'discard response is invalid: \'' + s + '\' expected')

    unfinished_report = ParseOneDiscardReport(host, port, response_iter,
                                              bruce_pid, report_interval)
    line = response_iter.GetNextLine(True, True, True)

    if line == None:
        # This happens when bruce was started recently enough that the first
        # discard report is not yet finished.
        return (unfinished_report, None)

    s = 'latest finished reporting period:'

    if line[:len(s)] != s:
        Die(EC_UNKNOWN, 'discard response is invalid: \'' + s + '\' expected')

    finished_report = ParseOneDiscardReport(host, port, response_iter,
                                            bruce_pid, report_interval)
    line = response_iter.GetNextLine(True, True, True)

    if line != None:
        Die(EC_UNKNOWN, 'unexpected junk at end of discard response: ' + \
            line.rstrip('\n'))

    return (unfinished_report, finished_report)
###############################################################################

###############################################################################
# Analyze input parameter 'report' of type TDiscardReport, looking for
# discards.  Return EC_SUCCESS if no discards are found.  Otherwise return
# EC_CRITICAL.
###############################################################################
def AnalyzeDiscardReport(report):
    if report.MalformedMsgCount > 0:
        return EC_CRITICAL

    # Comment this out for now.  The PHP developers need to fix things so that
    # unknown topics are not sent to bruce.
    #
    #if report.BadTopicMsgCount > 0:
    #    return EC_CRITICAL

    if len(report.DiscardTopicInfoList) > 0:
        return EC_CRITICAL

    return EC_SUCCESS
###############################################################################

###############################################################################
# Convert integer value 'epoch_seconds', representing seconds since the epoch,
# to a string that looks like this:
#
#     2014 04 15 08 50 55
#
# and return the result.  The result represents a time specified in UTC.  The
# values are as follows:
#
#     year
#     month
#     day of month
#     hour
#     minute
#     second
###############################################################################
def EpochSecondsToTimeString(epoch_seconds):
    time_info = time.gmtime(epoch_seconds)
    result = str(time_info.tm_year).zfill(4) + ' '
    result += str(time_info.tm_mon).zfill(2) + ' '
    result += str(time_info.tm_mday).zfill(2) + ' '
    result += str(time_info.tm_hour).zfill(2) + ' '
    result += str(time_info.tm_min).zfill(2) + ' '
    result += str(time_info.tm_sec).zfill(2)
    return result
###############################################################################

###############################################################################
# Exception class indicating a problem encountered while writing a discard
# report to the database.
###############################################################################
class TDatabaseError(Exception):
    'database error exception class'
    def __init__(self, message):
        Exception.__init__(self, message)
###############################################################################

###############################################################################
# Input parameter 'cur' is a database cursor.  Input parameter 'report' is a
# discard report represented as a TDiscardReport object.  Write a row
# representing the report to the BRUCE_DATA_QUALITY_REPORT table.  On success,
# return the integer value from the ID column of the newly inserted row.  If
# a row for the given report was already in the table, return None.  On error,
# let cx_Oracle.Error exception propagate to caller.
###############################################################################
def InsertReportRow(cur, report):
    # Add report row to BRUCE_DATA_QUALITY_REPORT table.

    report_start = EpochSecondsToTimeString(report.ReportStart)
    report_end = EpochSecondsToTimeString(report.ReportEnd)
    rows = [ (report.Host, report.Port, report.BruceReportId, report_start,
              report_end, report.MalformedMsgCount,
              report.UnsupportedVersionMsgCount, report.BadTopicMsgCount,
              report.BrucePid)
           ]
    cur.setinputsizes(200, int, int, 64, 64, int, int, int, int)
    sql = 'insert into BRUCE_DATA_QUALITY_REPORT ' + \
          '    (HOST, BRUCE_PORT, BRUCE_REPORT_ID, REPORT_START, ' + \
          '     REPORT_END, MALFORMED_MSG_COUNT, ' + \
          '     UNSUPPORTED_VERSION_MSG_COUNT, BAD_TOPIC_MSG_COUNT, ' + \
          '     BRUCE_PID) ' + \
          '    values ' + \
          '    (:1, :2, :3, TO_DATE(:4, \'yyyy mm dd hh24 mi ss\'), ' + \
          '     TO_DATE(:5, \'yyyy mm dd hh24 mi ss\'), :6, :7, :8, :9)'

    try:
        # This code can probably be improved slightly, since we are inserting a
        # single row using an API designed to support inserts of multiple rows.
        cur.executemany(sql, rows)
    except cx_Oracle.DatabaseError, x:
        error, = x.args

        if error.code == 1:
            # ORA-00001: unique constraint violated.  This indicates that the
            # report is already in the database, which is typically due to a
            # recent previous run of this script.  Return None to indicate that
            # the report is already there.
            return None

        # Any other exception indicates something went wrong.
        raise

    # Get primary key of added report row.

    sql = 'select ID from BRUCE_DATA_QUALITY_REPORT where HOST = :host ' + \
          ' and BRUCE_PORT = :port and REPORT_START = ' + \
          'TO_DATE(:report_start, \'yyyy mm dd hh24 mi ss\')'
    sql_params = { 'host' : report.Host, 'port' : str(report.Port),
                   'report_start' : report_start }
    cur.execute(sql, sql_params)
    result_count = 0
    report_id = 0

    for result in cur:
        result_count += 1

        if result_count > 1:
            raise TDatabaseError('Got unexpected multiple results in ' + \
                                 'query for report ID')

        report_id = result[0]

    if result_count == 0:
        raise TDatabaseError('ID of added report row not found')

    return report_id
###############################################################################

###############################################################################
# Insert a set of rows into a table whose schema looks like (ID, STRING_VALUE),
# where ID is a foreign key into the BRUCE_DATA_QUALITY_REPORT table.  Input
# paramater 'string_list' provides a list of string values to use for the rows.
# Input parameter 'report_id' gives the report ID foreign key.  Input parameter
# 'table_spec' is a text blurb to incorporate into the SQL insert statement,
# which specifies the table and column names.  Input parameter
# 'string_max_size' specifies the maximum allowed size of the string values, as
# defined by the schema.  Input parameter 'cur' provides a database cursor.  On
# error, let cx_Oracle.Error exception propagate to caller.
###############################################################################
def InsertStringRows(cur, report_id, table_spec, string_max_size, string_list):
    if len(string_list) == 0:
        return

    rows = [ ]

    for item in string_list:
        t = (report_id, item)
        rows.append(t)

    cur.setinputsizes(int, string_max_size)
    sql = 'insert into ' + table_spec + ' values (:1, :2)'
    cur.executemany(sql, rows)
###############################################################################

###############################################################################
# Insert a set of rows into either table BRUCE_DISCARD_TOPIC or
# BRUCE_POSSIBLE_DUP_TOPIC, depending on the text blurb in input parameter
# 'table_spec', which gets incorporated into the SQL insert statement.
# The data for the rows is provided by input parameter topic_error_info_list,
# which is a list of TTopicErrorInfo objects.  Input parameter 'report_id'
# gives the report ID foreign key referencing table BRUCE_DATA_QUALITY_REPORT.
# Input parameter 'topic_max_size' specifies the maximum allowed size of the
# string values, as defined by the schema.  Input parameter 'cur' provides a
# database cursor.
###############################################################################
def InsertTopicErrorInfoRows(cur, report_id, table_spec, topic_max_size,
                             topic_error_info_list):
    if len(topic_error_info_list) == 0:
        return

    rows = [ ]

    for item in topic_error_info_list:
        t = (report_id, item.Topic, item.Timestamp1, item.Timestamp2,
             item.Count)
        rows.append(t)

    cur.setinputsizes(int, topic_max_size, int, int, int)
    sql = 'insert into ' + table_spec + ' values (:1, :2, :3, :4, :5)'
    cur.executemany(sql, rows)
###############################################################################

###############################################################################
# Input parameter 'report' is a discard report represented as a TDiscardReport
# object.  Input parameter 'con' is an active database connection.  Input
# parameter 'cur' is a database cursor for the given connection.  Write the
# report to the database.  Return on success, or raise exception on error.
# Exceptions raised: TDatabaseError, cx_Oracle.Warning, cx_Oracle.Error
###############################################################################
def DoPersistDiscardReport(con, cur, report):
    # Add report row to BRUCE_DATA_QUALITY_REPORT table.
    report_id = InsertReportRow(cur, report)

    if report_id == None:
        if Opts.Verbose:
            print 'Report already persisted'

        # Report is already in database, which is typically due to a recent
        # previous run of this script.  This is not an error.
        return

    if Opts.Verbose:
        print 'Creating report ID ' + str(report_id)

    # Add rows to BRUCE_MALFORMED_MSG table.
    InsertStringRows(cur, report_id, 'BRUCE_MALFORMED_MSG(ID, MSG)', 4000,
                     report.MalformedMsgList)

    # Add rows to BRUCE_TOO_LONG_MSG table.
    InsertStringRows(cur, report_id, 'BRUCE_TOO_LONG_MSG(ID, MSG)', 4000,
                     report.TooLongMsgList)

    if len(report.UnsupportedVersionInfoList) > 0:
        # Add rows to BRUCE_UNSUPP_VERSION_MSG table.

        rows = [ ]

        for item in report.UnsupportedVersionInfoList:
            t = (report_id, item.MsgVersion, item.MsgCount)
            rows.append(t)

        cur.setinputsizes(int, int, int)
        sql = 'insert into ' + \
              'BRUCE_UNSUPP_VERSION_MSG(ID, MSG_VERSION, MSG_COUNT) ' + \
          'values (:1, :2, :3)'
        cur.executemany(sql, rows)

    # Add rows to BRUCE_BAD_TOPIC table.
    InsertStringRows(cur, report_id, 'BRUCE_BAD_TOPIC(ID, TOPIC)', 100,
                     report.BadTopicList)

    # Add rows to BRUCE_DISCARD_TOPIC table.
    InsertTopicErrorInfoRows(cur, report_id,
            'BRUCE_DISCARD_TOPIC(ID, TOPIC, TIMESTAMP_1, TIMESTAMP_2, ' +
            'DISCARD_COUNT)', 100, report.DiscardTopicInfoList)

    # Add rows to BRUCE_POSSIBLE_DUP_TOPIC table.
    InsertTopicErrorInfoRows(cur, report_id,
            'BRUCE_POSSIBLE_DUP_TOPIC(ID, TOPIC, TIMESTAMP_1, TIMESTAMP_2, ' +
            'DUP_COUNT)', 100, report.DupTopicInfoList)

    # Commit transaction.
    con.commit()

    if Opts.Verbose:
        print 'Committed report ID ' + str(report_id)
###############################################################################

###############################################################################
# Exception thrown when operation times out.
###############################################################################
class TTimeoutException(Exception):
    'exception thrown when operation times out'
    def __init__(self): Exception.__init__(self)
###############################################################################

###############################################################################
#
###############################################################################
def AlarmHandler(signame, frame):
    raise TTimeoutException()
###############################################################################

###############################################################################
# Input parameter 'report' is a discard report represented as a TDiscardReport
# object.  Write the report to the database.  Return the empty string on
# success, or an error message on error.  The entire report is written as a
# single transaction, so the database update is all or nothing.  The given
# discard report may already be in the database if the last time this script
# ran is recent enough.  In this case, we return the empty string (indicating
# success) without any database update.  Input paramaters 'username' and
# 'password' provide the credentials for database access.
###############################################################################
def PersistDiscardReport(report, username, password):
    db_string = username + '/' + password + '@' + Opts.Dbhost + ':' + \
            str(Opts.Dbport) + '/' + Opts.Database
    con = None  # database connection
    cur = None  # database cursor
    signal.signal(signal.SIGALRM, AlarmHandler)
    signal.alarm(Opts.DatabaseTimeout)

    try:
        con = cx_Oracle.connect(db_string)
        cur = con.cursor()
        cur.bindarraysize = 256
        DoPersistDiscardReport(con, cur, report)
        signal.alarm(0)  # cancel alarm on success
        result = ''
    except TDatabaseError, x:
        # These errors are not reported by Oracle, but by logic within this
        # script.
        result = 'Persist error: ' + x.message
        print result
    except cx_Oracle.Warning, x:
        # Here we make the conservative assumption that in the case of a
        # warning, the database transaction failed.
        result = 'Database warning: ' + str(x)
        print result
    except cx_Oracle.Error, x:
        result = 'Database error: ' + str(x)
        print result
    except TTimeoutException:
        result = 'Database timeout'
        print result
    finally:
        if cur != None:
            cur.close()

        if con != None:
            con.close()

    if result != '':
        print 'Database transaction will be attempted again later'

    return result
###############################################################################

###############################################################################
# Open file specified by 'path' for reading.  Read all lines from file into a
# list of strings, preserving trailing newline characters, and return the list
# of lines.
###############################################################################
def GetDiscardResponseFromFile(path):
    response = [ ]

    try:
        infile = open(path, 'r')

        for line in infile:
            response.append(line)
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + path + ' for reading: ' + \
                e.strerror + '\n')
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + path + ' for reading: ' + \
                e.strerror + '\n')
    finally:
        infile.close()

    return response
###############################################################################

###############################################################################
# Read a saved discard response file.  These files get created when there is a
# failure saving a discard report to the database, so the database operation
# can be retried later.  The first line of the file contains the canonicalized
# hostname.  The second line contains an error message indicating why the
# database operation failed.  The remaining lines contain the discard response,
# as obtained from Bruce.
###############################################################################
def ReadSavedDiscardResponseFile(path):
    response = [ ]

    try:
        infile = open(path, 'r')
        line = infile.readline()
        canonicalized_host = line.rstrip('\n')
        line = infile.readline()  # discard error message

        for line in infile:
            response.append(line)
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + path + ' for reading: ' + \
                e.strerror + '\n')
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + path + ' for reading: ' + \
                e.strerror + '\n')
    finally:
        infile.close()

    return (canonicalized_host, response)
###############################################################################

###############################################################################
# Return EC_CRITICAL if the age in seconds of the oldest timestamp in
# 'timestamp_list' (an array of integers representing seconds since the epoch)
# exceeds Opts.MaxReportAge.  Otherwise return EC_SUCCESS.  It is assumed that
# 'timestamp_list' is sorted in ascending order.
###############################################################################
def CheckDiscardReportFileAges(timestamp_list):
    if len(timestamp_list) > 0:
        now = GetEpochSeconds()
        oldest = timestamp_list[0]

        if (oldest < now) and ((now - oldest) > Opts.MaxReportAge):
            print 'Oldest discard report file timestamp ' + str(oldest) + \
                    ' exceeds ' + str(Opts.MaxReportAge) + \
                    ' seconds: database appears to be down for extended ' + \
                    'period of time'
            return EC_WARNING

    return EC_SUCCESS
###############################################################################

###############################################################################
# Retry database save operation for saved discard report that we previously
# failed to save to database.  Return True on success or False on error.
###############################################################################
def RetryPersistDiscardReport(filename, timestamp, username, password):
    if Opts.Verbose:
        print 'Retrying persist of discard report ' + str(timestamp)

    (canonicalized_host, saved_response) = \
            ReadSavedDiscardResponseFile(filename)
    (unfinished, finished) = ParseDiscardResponse(saved_response,
            canonicalized_host, Opts.BruceStatusPort)

    if finished == None:
        print 'Failed to obtain finished report from saved discard ' + \
                'response ' + str(timestamp)
        return False

    err_msg = PersistDiscardReport(finished, username, password)

    if err_msg != '':
        if Opts.Verbose:
            print 'Persist discard report ' + str(timestamp) + ' retry failed'

        return False

    if Opts.Verbose:
        print 'Persist discard report ' + str(timestamp) + ' retry succeeded'

    return True
###############################################################################

###############################################################################
# This is called after we have successfully stored a discard report in the
# database.  Check for prior discard reports that we failed to persist.  These
# are stored in local files.  If any still exist, try again now to write them
# to the database.  The database we currently use for storing discard reports
# doesn't always have great uptime, so this is a workaround.
###############################################################################
def CheckPriorPersistFailures(work_path, username, password):
    # Contents of 'timestamp_list' are sorted in ascending order, so we go from
    # oldest to newest.
    timestamp_list = FindDiscardFileTimes(work_path)

    for timestamp in timestamp_list:
        filename = work_path + '/' + str(timestamp)

        if RetryPersistDiscardReport(filename, timestamp, username, password):
            # Success: Delete old discard report file.
            try:
                os.remove(filename)
            except OSError as e:
                print 'Failed to delete old discard report file [' + \
                        filename + ']: ' + e.strerror
                return EC_WARNING
            except IOError as e:
                print 'Failed to delete old discard report file [' + \
                        filename + ']: ' + e.strerror
                return EC_WARNING
        else:
            failed_timestamp_list = [ ]
            failed_timestamp_list.append(timestamp)
            return CheckDiscardReportFileAges(failed_timestamp_list)

    return EC_SUCCESS
###############################################################################

###############################################################################
# This is called after we have failed to persist a discard report.  Save the
# report temporarily to a local file, so we can try again later.
###############################################################################
def HandlePersistFailure(work_path, report_start, discard_response,
        canonicalized_host, err_msg):
    if Opts.Verbose:
        print 'Handling failure to persist discard report ' + str(report_start)

    file_list = FindDiscardFileTimes(work_path)

    for item in file_list:
        if item == report_start:
            if Opts.Verbose:
                print 'Discard report ' + str(report_start) + \
                        ' already saved to local file system'

            return CheckDiscardReportFileAges(FindDiscardFileTimes(work_path))

    filename = work_path + '/' + str(report_start)

    try:
        outfile = open(filename, 'w')
    except OSError as e:
        print 'Failed to create discard report file [' + filename + ']: ' + \
                e.strerror
        return EC_CRITICAL
    except IOError as e:
        print 'Failed to create discard report file [' + filename + ']: ' + \
                e.strerror
        return EC_CRITICAL

    # The code that parses the file we are creating expects the error message
    # to occupy a single line.  The error message should not contain any
    # newline characters, but it doesn't hurt to make sure.
    msg = err_msg.replace('\n', ' ')

    outfile.write(canonicalized_host + '\n')
    outfile.write(err_msg + '\n')

    for line in discard_response:
        outfile.write(line)

    outfile.close()

    if Opts.Verbose:
        print 'Discard report ' + str(report_start) + \
                ' has been saved to local file system'

    return CheckDiscardReportFileAges(FindDiscardFileTimes(work_path))
###############################################################################

###############################################################################
# Input parameter 'file_contents' is a string representation of the entire
# contents of the database credentials file.  It should contain JSON that looks
# something like this:
#
#     { "password": "quack", "username": "daffyduck" }
#
# Parse the JSON, extract the username and password, and return a 2-item tuple
# containing the username followed by the password, represented as ASCII
# strings.
###############################################################################
def ParseCredentialsFileContents(file_contents):
    try:
        parsed_json = json.loads(file_contents)
    except ValueError, x:
        Die(EC_CRITICAL, 'Failed to parse credentials file contents')

    if type(parsed_json) != types.DictType:
        Die(EC_CRITICAL, 'Credentials file data is in unexpected format')

    try:
        username_u = parsed_json['username']
        password_u = parsed_json['password']
    except KeyError, x:
        Die(EC_CRITICAL, 'Credentials file data is in unexpected format')

    if type(username_u) != types.UnicodeType or \
            type(password_u) != types.UnicodeType:
        Die(EC_CRITICAL, 'Credentials file data is in unexpected format')

    username = username_u.encode('ascii', 'ignore')
    password = password_u.encode('ascii', 'ignore')
    return (username, password)
###############################################################################

###############################################################################
# Read the credentials file and extract the username and password for database
# access.  Return a 2-item tuple of strings containing the username followed by
# the password.
###############################################################################
def GetCredentialsFromFile():
    file_contents = ''

    try:
        infile = open(Opts.Credentials, 'r')

        for line in infile:
            file_contents += line
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + Opts.Credentials + \
                ' for reading: ' + e.strerror + '\n')
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + Opts.Credentials + \
                ' for reading: ' + e.strerror + '\n')
    finally:
        infile.close()

    return ParseCredentialsFileContents(file_contents)
###############################################################################

###############################################################################
# Class for storing program options
#
# members:
#     Verbose: True enables verbose output.
#     WorkDir: A subdirectory beneath the Nagios user's home directory where
#         the script maintains all of its data files.
#     MaxReportAge: If any old discard report files exceed this age in seconds,
#         the situation is treated as Critical.  This handles cases where the
#         database is down for an unusually long time.
#     NagiosUser: The name of the nagios user.
#     BruceHost: The host running bruce that we should connect to for counter
#         data.
#     BruceStatusPort: The port to connect to when asking bruce for counter
#         data.
#     Testfile: A file to read discard info from (for testing).
#     Dbhost: Host for Oracle database.
#     Dbport: Port that Oracle database listens on.
#     Database: Specifies the database name to use for storing discard reports.
#     DatabaseTimeout: Specifies a timeout in seconds for storing a discard
#         report in the database.
#     Credentials: Credentials file for database access.
###############################################################################
class TProgramOptions(object):
    'program options class'
    def __init__(self, verbose, work_dir, nagios_server, max_report_age,
                 nagios_user, bruce_host, bruce_status_port, testfile, dbhost,
                 dbport, database, database_timeout, credentials):
        self.Verbose = verbose
        self.WorkDir = work_dir
        self.NagiosServer = nagios_server
        self.MaxReportAge = max_report_age
        self.NagiosUser = nagios_user
        self.BruceHost = bruce_host
        self.BruceStatusPort = bruce_status_port
        self.Testfile = testfile
        self.Dbhost = dbhost
        self.Dbport = dbport
        self.Database = database
        self.DatabaseTimeout = database_timeout
        self.Credentials = credentials
###############################################################################

###############################################################################
# Parse command line arguments provided by input list parameter 'args' and
# return a corresponding TProgramOptions object on success.  If there is a
# problem with the arguments, die with an error message.
###############################################################################
def ParseArgs(args):
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'vw:s:a:u:b:p:t:D:P:d:T:c:',
                ['verbose', 'work_dir=', 'nagios_server=', 'max_report_age=',
                 'nagios_user=', 'bruce_host=', 'bruce_status_port=',
                 'testfile=', 'dbhost=', 'dbport=', 'database=',
                 'database_timeout=', 'credentials='])
    except getopt.GetoptError as e:
        Die(EC_UNKNOWN, str(e))

    opt_verbose = False
    opt_work_dir = ''
    opt_nagios_server = ''
    opt_max_report_age = 28800
    opt_nagios_user = 'nrpe'
    opt_bruce_host = ''
    opt_bruce_status_port = 9090
    opt_testfile = ''
    opt_dbhost = ''
    opt_dbport = 1521
    opt_database = ''
    opt_database_timeout = 15
    opt_credentials = ''

    for o, a in opts:
        if o in ('-v', '--verbose'):
            opt_verbose = True
        elif o in ('-w', '--work_dir'):
            opt_work_dir = a
        elif o in ('-s', '--nagios_server'):
            opt_nagios_server = a
        elif o in ('-a', '--max_report_age'):
            try:
                opt_max_report_age = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_max_report_age < 0:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a nonnegative integer')
        elif o in ('-u', '--nagios_user'):
            opt_nagios_user = a
        elif o in ('-b', '--bruce_host'):
            opt_bruce_host = a
        elif o in ('-p', '--bruce_status_port'):
            try:
                opt_bruce_status_port = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if (opt_bruce_status_port < 1) or (opt_bruce_status_port > 65535):
                Die(EC_UNKNOWN,
                    'The ' + o +
                    ' option requires an integer between 1 and 65535')
        elif o in ('-t', '--testfile'):
            opt_testfile = a
        elif o in ('-D', '--dbhost'):
            opt_dbhost = a
        elif o in ('-P', '--dbport'):
            try:
                opt_dbport = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if (opt_dbport < 1) or (opt_dbport > 65535):
                Die(EC_UNKNOWN,
                    'The ' + o +
                    ' option requires an integer between 1 and 65535')
        elif o in ('-d', '--database'):
            opt_database = a
        elif o in ('-T', '--database_timeout'):
            try:
                opt_database_timeout = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_database_timeout < 1:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a positive integer')
        elif o in ('-c', '--credentials'):
            opt_credentials = a
        else:
            Die(EC_UNKNOWN, 'Unhandled command line option')

    if opt_work_dir == '':
        Die(EC_UNKNOWN, '-w option must be specified')

    if opt_nagios_server == '':
        Die(EC_UNKNOWN, '-s or --nagios_server option must be specified')

    if opt_bruce_host == '':
        Die(EC_UNKNOWN, '-b option must be specified')

    if opt_dbhost != '':
        if opt_database == '':
            Die(EC_UNKNOWN, '-d option must be specified if -D is specified')

        if opt_credentials == '':
            Die(EC_UNKNOWN, '-c option must be specified if -D is specified')

    return TProgramOptions(opt_verbose, opt_work_dir, opt_nagios_server,
                           opt_max_report_age, opt_nagios_user, opt_bruce_host,
                           opt_bruce_status_port, opt_testfile, opt_dbhost,
                           opt_dbport, opt_database, opt_database_timeout,
                           opt_credentials)
###############################################################################

###############################################################################
# main program
###############################################################################
def main():
    # 'Opts' is a global variable containing a TProgramOptions object with all
    # program options.
    global Opts
    Opts = ParseArgs(sys.argv[1:])

    work_path = GetNagiosDir() + '/' + Opts.WorkDir + '/' + Opts.NagiosServer
    MakePathExist(work_path)

    try:
        # Get canonicalized hostname.
        host = CanonicalizeHost(Opts.BruceHost)
    except socket.error, x:
        print 'Failed to canonicalize bruce host ' + Opts.BruceHost + ': ' + \
                str(x)
        sys.exit(EC_CRITICAL)

    if Opts.Testfile == '':
        # Get discard info from bruce.
        discard_response = GetDiscardResponse('http://' + host + ':' + \
                str(Opts.BruceStatusPort) + '/discards/compact')
    else:
        discard_response = GetDiscardResponseFromFile(Opts.Testfile)

    (unfinished_report, finished_report) = \
            ParseDiscardResponse(discard_response, host, Opts.BruceStatusPort)

    # See if reports contain any discards.
    if finished_report == None:
        nagios_code_from_reports = AnalyzeDiscardReport(unfinished_report)
    else:
        nagios_code_from_reports = max(AnalyzeDiscardReport(unfinished_report),
                          AnalyzeDiscardReport(finished_report))

    if Opts.Verbose:
        print 'current (unfinished) report:'
        print str(unfinished_report)

        if finished_report != None:
            print ''
            print 'latest finished report:'
            print str(finished_report)
            print ''

    if Opts.Dbhost != '' and finished_report != None:
        # Store latest finished discard report in database.
        (username, password) = GetCredentialsFromFile()
        persist_result = \
                PersistDiscardReport(finished_report, username, password)

        if persist_result == '':
            # Success.  See if there are any prior discard reports that we were
            # unable to persist.  If so, try again now.
            nagios_code_from_persist = \
                    CheckPriorPersistFailures(work_path, username, password)
        else:
            nagios_code_from_persist = HandlePersistFailure(work_path,
                finished_report.ReportStart, discard_response, host,
                persist_result)
    else:
        nagios_code_from_persist = EC_SUCCESS

    nagios_code = max(nagios_code_from_reports, nagios_code_from_persist)

    if nagios_code != EC_SUCCESS:
        if nagios_code_from_reports != EC_SUCCESS:
            print NagiosCodeToString(nagios_code_from_reports) + \
                    ': discards found'

        if nagios_code_from_persist != EC_SUCCESS:
            print NagiosCodeToString(nagios_code_from_persist) + \
                    ': failed to store discard report in database'

        print ''

        for line in discard_response:
            sys.stdout.write(line)

    # Nagios expects some sort of output even in the case of a successful
    # result.
    if nagios_code == EC_SUCCESS:
        print "Ok"

    sys.exit(nagios_code)
###############################################################################

try:
    main()
except Exception:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)

    # Write stack trace to standard output, since that's where Nagios expects
    # error output to go.
    for elem in lines:
        print elem

    sys.exit(EC_UNKNOWN)
