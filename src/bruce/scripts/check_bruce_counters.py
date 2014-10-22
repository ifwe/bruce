#!/usr/bin/env python

###############################################################################
# -----------------------------------------------------------------------------
# Copyright 2013-2014 if(we)
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
# This script requests counter reports from bruce and analyzes the counters.
# It is intended to be executed by Nagios, or manually to get more information
# on a problem reported by Nagios.
#
# Program arguments:
#    -m N
#    --manual N: (optional)
#        Specifies that we are running in manual mode.  In manual mode, instead
#        of getting counters from bruce and saving them to a counter file, we
#        read a previously created counter file and analyze it, giving verbose
#        output for each problem found.  When the script (not running in manual
#        mode) reports a problem to Nagios, a human being can later run it in
#        manual mode to analyze the resulting counter file and determine the
#        details of the problem(s) found.  When running in manual mode, we
#        don't delete old counter files.  The 'N' value gives a number of
#        seconds since the epoch specifying a counter file to be manually
#        analyzed.
#
#    -d DIR
#    --work_dir DIR: (required)
#        Specifies a directory under the Nagios user's home directory where the
#        script keeps all of its data files.  This directory will be created if
#        it does not already exist.
#
#    -i INTERVAL
#    --interval INTERVAL: (required)
#        The minimum number of seconds ago we will accept when looking for a
#        recent counter file to compare our metadata update count against.  The
#        most recent counter file whose age is at least the minimum will be
#        chosen.  If no such counter file exists, the metadata update count
#        test is skipped.
#
#    -H HISTORY
#    --history HISTORY: (required unless running in manual mode)
#        The number of minutes of history to preserve when deleting old counter
#        files.  A value of 0 means "preserve everything".
#
#    -s SERVER
#    --nagios_server SERVER: (required)
#        If not running in manual mode, this is a unique identifier for the
#        Nagios server that triggered the current execution of this script.  If
#        running in manual mode, this is a unique identifier for the Nagios
#        server that triggered the script execution that caused creation of the
#        counter file we are analyzing.
#
#    -w T
#    --socket_error_warn_threshold T: (required)
#        If the number of socket errors indicated by the counter output exceeds
#        this value, it is treated as Warning.
#
#    -c T
#    --socket_error_critical_threshold T: (required)
#        If the number of socket errors indicated by the counter output exceeds
#        this value, it is treated as Critical.
#
#    -W T
#    --msg_count_warn_threshold T: (required)
#        If the number of outstanding messages indicated by the counter output
#        exceeds this value, it is treated as Warning.
#
#    -C T
#    --msg_count_critical_threshold T: (required)
#        If the number of outstanding messages indicated by the counter output
#        exceeds this value, it is treated as Critical.
#
#    -b HOST
#    --bruce_host HOST: (required unless running in manual mode)
#        The host running bruce that we should connect to for counter data.
#
#    -p PORT
#    --bruce_status_port PORT: (optional, default = 9090)
#        The port to connect to when asking bruce for counter data.
#
#    -u USER
#    --nagios_user USER: (optional, default = 'nrpe')
#        The name of the nagios user.  The script will create a directory under
#        the nagios user's home directory (see '-d DIR' option above).
###############################################################################

import bisect
import errno
import getopt
import json
import os
import subprocess
import sys
import time

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
# Return the number of seconds since the epoch.
###############################################################################
def SecondsSinceEpoch():
    return int(time.time())
###############################################################################

###############################################################################
# Create the directory given by 'path' if it doesn't already exist.
###############################################################################
def MakeDirExist(path):
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
# Return true iff. we are running in manual mode.
###############################################################################
def RunningInManualMode():
    return (Opts.Manual >= 0)
###############################################################################

###############################################################################
# Print a problem message preceded by a string representation of its Nagios
# code.
###############################################################################
def ReportProblem(nagios_code, msg):
    print NagiosCodeToString(nagios_code) + ': ' + msg
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
# Return the value associated with string 'key' in dictionary 'counters', or
# die with an error message if no such key was found.
###############################################################################
def LookupCounter(counters, key):
    try:
        value = counters[key]
    except KeyError:
        Die(EC_UNKNOWN, 'Counter ' + key + ' not found')

    return value
###############################################################################

###############################################################################
# Parse a JSON counter report and return the result.
###############################################################################
def ParseCounterReport(json_input):
    try:
        result = json.loads(json_input)
    except ValueError:
        Die(EC_UNKNOWN, 'Failed to parse counter report')

    if type(result) is not dict:
        Die(EC_UNKNOWN, 'Counter report is not a dictionary')

    if 'now' not in result:
        Die(EC_UNKNOWN, 'Counter report missing "now" item')

    if type(result['now']) is not int:
        Die(EC_UNKNOWN, 'Item "now" in counter report is not an integer')

    if 'since' not in result:
        Die(EC_UNKNOWN, 'Counter report missing "since" item')

    if type(result['since']) is not int:
        Die(EC_UNKNOWN, 'Item "since" in counter report is not an integer')

    if 'pid' not in result:
        Die(EC_UNKNOWN, 'Counter report missing "pid" item')

    if type(result['pid']) is not int:
        Die(EC_UNKNOWN, 'Item "pid" in counter report is not an integer')

    if 'version' not in result:
        Die(EC_UNKNOWN, 'Counter report missing "version" item')

    if type(result['version']) is not unicode:
        Die(EC_UNKNOWN,
            'Item "version" in counter report is not a unicode string')

    if 'counters' not in result:
        Die(EC_UNKNOWN, 'Counter report missing "counters" item')

    if type(result['counters']) is not list:
        Die(EC_UNKNOWN, 'Item "counters" in counter report is not a list')

    counter_names = set()

    for item in result['counters']:
        if type(item) is not dict:
            Die(EC_UNKNOWN, 'Counter item is not a dictionary')

        if 'name' not in item:
            Die(EC_UNKNOWN, 'Counter item has no name')

        if type(item['name']) is not unicode:
            Die(EC_UNKNOWN, 'Counter item name is not a unicode string')

        if item['name'] in counter_names:
            Die(EC_UNKNOWN, 'Duplicate counter name [' + item['name'] + ']')

        counter_names.add(item['name'])

        if 'value' not in item:
            Die(EC_UNKNOWN, 'Counter item [' + item['name'] + '] has no value')

        if type(item['value']) is not int:
            Die(EC_UNKNOWN, 'Value of counter item [' + item['name'] + \
                    '] is not an integer')

        if 'location' not in item:
            Die(EC_UNKNOWN, 'Counter item [' + item['name'] + \
                    '] has no location')

        if type(item['location']) is not unicode:
            Die(EC_UNKNOWN, 'Location of counter item [' + item['name'] + \
                    '] is not a unicode string')

    return result
###############################################################################

###############################################################################
# Serialize a counter report to a JSON string and return the result.
###############################################################################
def SerializeCounterReport(report):
    return json.dumps(report, sort_keys=True, indent=4,
                separators=(',', ': ')) + '\n'
###############################################################################

###############################################################################
# Send a "get counters" HTTP request to bruce, parse the response, and return
# the parsed counter report.
###############################################################################
def GetCounters(url):
    try:
        response = urlopen(url)
    except URLError as e:
        # Treat this as Critical, since it indicates that bruce is probably not
        # running.
        Die(EC_CRITICAL, 'Failed to open counters URL: ' + str(e.reason))

    json_input = ''

    for line in response:
        json_input += line

    return ParseCounterReport(json_input)
###############################################################################

###############################################################################
# Create a file with counter data whose location is given by 'path'.
# 'counter_report' is a counter report containing the data to be written.
# Serialize the report to JSON and write it to the file.
###############################################################################
def CreateCounterFile(path, counter_report):
    json_report = SerializeCounterReport(counter_report)

    try:
        is_open = False
        outfile = open(path, 'w')
        is_open = True
        outfile.write(json_report)
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to create counter file ' + path + ': ' +
            e.strerror)
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to create counter file ' + path + ': ' +
            e.strerror)
    finally:
        if is_open:
            outfile.close()
###############################################################################

###############################################################################
# Read counter data from file given by 'path'.  Return a counter report
# containing the data.
###############################################################################
def ReadCounterFile(path):
    json_input = ''

    try:
        is_open = False
        infile = open(path, 'r')
        is_open = True

        for line in infile:
            json_input += line
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to open counter file ' + path +
            ' for reading: ' + e.strerror)
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to open counter file ' + path +
            ' for reading: ' + e.strerror)
    finally:
        if is_open:
            infile.close()

    return ParseCounterReport(json_input)
###############################################################################

###############################################################################
# Find all names of files in the directory given by 'path' that may be counter
# files based on their names, and that are older than the current time given by
# 'now'.  Return the filenames converted to integers (each representing seconds
# since the epoch) as a list.  The returned list will be sorted in ascending
# order.
###############################################################################
def FindOldCounterFileTimes(path, now):
    try:
        file_list = os.listdir(path)
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to list contents of directory ' + path + ': ' +
            e.strerror)
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to list contents of directory ' + path + ': ' +
            e.strerror)

    result = []

    for item in file_list:
        try:
            value = int(item)
        except ValueError:
            # The filename can't be converted to an integer, so it must not be
            # a counter file.
            continue

        if (value < now) and (value >= 0):
            result.append(value)

    result.sort()
    return result
###############################################################################

###############################################################################
# Read the counter file for the most recent previous counter report for the
# currently running instance of bruce.  Return a counter report containing the
# data, or None if no such data exists.
#
# parameters:
#     current_report: The current counter report.
#     work_path: The pathname of the directory containing the counter files.
#     old_counter_file_times: A list of integers sorted in ascending order.
#         Each value is a number of seconds since the epoch whose string
#         representation gives the name of a counter file in directory
#         'work_path'.
###############################################################################
def GetLastCounterReport(current_report, work_path, old_counter_file_times):
    file_count = len(old_counter_file_times)

    # There are no previous counter files, so we don't yet have any history.
    if file_count == 0:
        return None

    newest_timestamp = old_counter_file_times[file_count - 1]
    path = work_path + '/' + str(newest_timestamp)
    last_report = ReadCounterFile(path)

    # The most recent previous counter file contains data for a different
    # invocation of bruce (i.e. bruce restarted).  Therefore the data is not
    # applicable.
    if last_report['pid'] != current_report['pid']:
        return None

    if last_report['now'] != newest_timestamp:
        Die(EC_UNKNOWN, 'Previous counter report file ' +
            str(newest_timestamp) + ' contains timestamp ' +
            str(last_report['now']) + ' that differs from filename')

    return last_report
###############################################################################

###############################################################################
# Read the counter file for a previous counter report for the currently running
# instance of bruce.  The most recent file that was created at least a given
# number of seconds ago will be chosen.  Return a counter report containing the
# data, or None if no such data exists.
#
# parameters:
#     current_report: The current counter report.
#     work_path: The pathname of the directory containing the counter files.
#     old_counter_file_times: A list of integers sorted in ascending order.
#         Each value is a number of seconds since the epoch whose string
#         representation gives the name of a counter file in directory
#         'work_path'.
#     min_seconds_ago: The minimum number of seconds prior to the timestamp of
#         'current_report' that the chosen report's timestamp must be.
###############################################################################
def GetOldCounterReport(current_report, work_path, old_counter_file_times,
        min_seconds_ago):
    file_count = len(old_counter_file_times)

    if file_count == 0:
        return None

    max_ok_timestamp = current_report['now'] - min_seconds_ago

    # Use binary search to find the right timestamp.
    i = bisect.bisect_left(old_counter_file_times, max_ok_timestamp)

    if i == file_count:
        chosen_timestamp = old_counter_file_times[file_count - 1]
    elif i == 0:
        chosen_timestamp = old_counter_file_times[0]

        if chosen_timestamp > max_ok_timestamp:
            # All reports are too recent.
            return None
    else:
        chosen_timestamp = old_counter_file_times[i]

        if chosen_timestamp > max_ok_timestamp:
            chosen_timestamp = old_counter_file_times[i - 1]

    path = work_path + '/' + str(chosen_timestamp)
    old_report = ReadCounterFile(path)

    # The old counter file contains data for a different invocation of bruce
    # (i.e. bruce restarted).  Therefore the data is not applicable.
    if old_report['pid'] != current_report['pid']:
        return None

    if old_report['now'] != chosen_timestamp:
        Die(EC_UNKNOWN, 'Old counter report file ' + str(chosen_timestamp) +
            ' contains wrong timestamp')

    return old_report
###############################################################################

###############################################################################
# Input list 'in_counters' contains counter information from a counter report.
# Return a dictionary whose keys are the names of the counters in 'in_counters'
# and whose values are the corresponding counts.
###############################################################################
def CounterListToDict(in_counters):
    result = {}

    for item in in_counters:
        result[item['name']] = item['value']

    return result
###############################################################################

###############################################################################
# Take as input two lists, 'old_counters' and 'new_counters', of counter
# information from counter reports.  The input lists must have exactly the same
# sets of counter names.  If 'old_counters' is None, return a dictionary whose
# keys are the names of the counters from 'new_counters' and whose values are
# the corresponding counts.  Otherwise return a dictionary with keys identical
# to the counter names from 'old_couners' and 'new_counters'.  For each key K
# in the returned dictionary, suppose V_old is the value of the corresponding
# counter in 'old_counters' and V_new is the value of the corresponding counter
# in 'new_counters'.  The value associated with K is then V_new - V_old.  Exit
# with an error if the sets of counter names from the input lists are not
# identical or the computed value for any key in the result dictionary is
# negative.
###############################################################################
def ComputeCounterDeltas(old_counters, new_counters):
    new_counter_dict = CounterListToDict(new_counters)

    if old_counters == None:
        return new_counter_dict

    old_counter_dict = CounterListToDict(old_counters)
    old_keys = set(k for k in old_counter_dict.keys())
    new_keys = set(k for k in new_counter_dict.keys())
    s = old_keys - new_keys

    if s:
        Die(EC_UNKNOWN, 'Old counter file has unmatched keys: ' + str(s))

    s = new_keys - old_keys

    if s:
        Die(EC_UNKNOWN, 'New counter file has unmatched keys: ' + str(s))

    result = {}

    for k in new_keys:
        delta = new_counter_dict[k] - old_counter_dict[k]

        if delta < 0:
            Die(EC_UNKNOWN, 'Key [' + k + '] decreased by ' + str(-delta) + \
                    ' in new counters file')

        result[k] = delta

    return result
###############################################################################

###############################################################################
# Look up the counter whose name string is 'counter_name' in dictionary
# 'deltas'.  If its value is > 'max_ok_value' then report it as a problem with
# Nagios code 'nagios_code' and return max(nagios_code, old_nagios_code).
# Otherwise return 'old_nagios_code'.
###############################################################################
def CheckDelta(deltas, counter_name, max_ok_value, nagios_code,
        old_nagios_code):
    new_nagios_code = old_nagios_code
    count = LookupCounter(deltas, counter_name)

    if count > max_ok_value:
        new_nagios_code = max(new_nagios_code, nagios_code)
        ReportProblem(nagios_code, counter_name + '=' + str(count))

    return new_nagios_code
###############################################################################

###############################################################################
# Check for socket-related errors in dictionary 'deltas' and report a problem
# if the combined socket error count is high enough.  Return a nagios code
# equal to max(old_nagios_code, nagios code of reported problem) if a problem
# was reported.  Otherwise return 'old_nagios_code'.
###############################################################################
def CheckSocketErrorDeltas(deltas, old_nagios_code):
    new_nagios_code = old_nagios_code
    counter_names = [ 'ConnectFailOnTryGetMetadata',
                      'MetadataResponseRead1LostTcpConnection',
                      'MetadataResponseRead1TimedOut',
                      'MetadataResponseRead2LostTcpConnection',
                      'MetadataResponseRead2TimedOut',
                      'MetadataResponseRead2UnexpectedEnd',
                      'ReadMetadataResponse2Fail',
                      'ReceiverSocketBrokerClose',
                      'ReceiverSocketError',
                      'ReceiverSocketTimeout',
                      'SenderConnectFail',
                      'SenderSocketError',
                      'SenderSocketTimeout',
                      'SendMetadataRequestFail',
                      'SendMetadataRequestLostTcpConnection',
                      'SendMetadataRequestTimedOut'
                    ]
    nonzero_counter_names = []
    sum = 0

    for name in counter_names:
        count = LookupCounter(deltas, name)

        if count > 0:
            nonzero_counter_names.append(name)
            sum += count

    print_counters = False

    if sum > Opts.SocketErrorCriticalThreshold:
        new_nagios_code = max(new_nagios_code, EC_CRITICAL)
        print_counters = RunningInManualMode()
        ReportProblem(EC_CRITICAL, str(sum) + ' socket errors:')
    elif sum > Opts.SocketErrorWarnThreshold:
        new_nagios_code = max(new_nagios_code, EC_WARNING)
        print_counters = RunningInManualMode()
        ReportProblem(EC_WARNING, str(sum) + ' socket errors:')

    if print_counters:
        for name in nonzero_counter_names:
            print '    ' + name + '=' + str(deltas[name])

    return new_nagios_code
###############################################################################

###############################################################################
# The keys of input dictionary 'deltas' are counter names, and the values are
# differences between integer counts from the current counter report (obtained
# directly from bruce) and the previous counter report (obtained from a file).
# Check for problems, report any problems found, and return a Nagios code
# representing the maximum severity of any problems found.
###############################################################################
def AnalyzeDeltas(deltas):
    # A single instance of any of these is Critical.
    nagios_code = CheckDelta(deltas, 'MsgUnprocessedDestroy', 0, EC_CRITICAL,
                             EC_SUCCESS)
    nagios_code = CheckDelta(deltas, 'NoDiscardQuery', 0, EC_CRITICAL,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataHasEmptyTopicList', 0,
                             EC_CRITICAL, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataHasEmptyBrokerList', 0,
                             EC_CRITICAL, nagios_code)
    nagios_code = CheckDelta(deltas, 'BugDispatchBatchOutOfRangeIndex', 0,
                             EC_CRITICAL, nagios_code)
    nagios_code = CheckDelta(deltas, 'BugDispatchMsgOutOfRangeIndex', 0,
                             EC_CRITICAL, nagios_code)
    nagios_code = CheckDelta(deltas, 'TopicHasNoAvailablePartitions', 0,
                             EC_CRITICAL, nagios_code)
    nagios_code = CheckDelta(deltas, 'InitialGetMetadataFail', 0, EC_CRITICAL,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'GetMetadataFail', 0, EC_CRITICAL,
                             nagios_code)

    # Any number of instances of any of these is Warning.
    nagios_code = CheckDelta(deltas, 'MetadataResponseBadTopicNameLen', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseBadBrokerHostLen', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas,
            'MetadataResponseNegativeCaughtUpReplicaNodeId', 0, EC_WARNING,
            nagios_code)
    nagios_code = CheckDelta(deltas,
            'MetadataResponseNegativePartitionCaughtUpReplicaCount', 0,
            EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseNegativeReplicaNodeId',
                             0, EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas,
            'MetadataResponseNegativePartitionReplicaCount', 0, EC_WARNING,
            nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseInvalidLeaderNodeId', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseNegativePartitionId', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseNegativePartitionCount',
                             0, EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseNegativeTopicCount', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseBadBrokerPort', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseNegativeBrokerNodeId', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseNegativeBrokerCount', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'MetadataResponseHasExtraJunk', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'BadMetadataResponseSize', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BadMetadataContent', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BadMetadataResponse', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'ProduceResponseBadPartitionCount', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'ProduceResponseBadTopicNameLength', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'ProduceResponseBadTopicCount', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'BadKafkaResponseSize', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BugGetAckWaitQueueOutOfRangeIndex', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'ProduceResponseUnexpectedTopic', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'ProduceResponseUnexpectedPartition', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'ProduceResponseShortPartitionList', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'ProduceResponseShortTopicList', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'CorrelationIdMismatch', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BadProduceResponseSize', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BadProduceResponse', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BugAllTopicsEmpty', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BugMsgListMultipleTopics', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BugMsgSetEmpty', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'BugMultiPartitionGroupEmpty', 0,
                             EC_WARNING, nagios_code)
    nagios_code = CheckDelta(deltas, 'BugProduceRequestEmpty', 0, EC_WARNING,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'MsgSetCompressionError', 0, EC_WARNING,
                             nagios_code)

    # Check counters indicating socket-related errors.  These are summed
    # together and compared against thresholds for Warning and Critical.
    nagios_code = CheckSocketErrorDeltas(deltas, nagios_code)

    # An instance of any of these indicates that something is wrong inside
    # bruce's HTTP status monitoring mechanism, which means that the integrity
    # of the information received by the monitoring scripts may be compromised.
    nagios_code = CheckDelta(deltas, 'MongooseUrlDecodeError', 1, EC_UNKNOWN,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'MongooseStdException', 1, EC_UNKNOWN,
                             nagios_code)
    nagios_code = CheckDelta(deltas, 'MongooseUnknownException', 1,
                             EC_UNKNOWN, nagios_code)

    return nagios_code
###############################################################################

###############################################################################
# Compare the 'MsgCreate' and 'MsgDestroy' counter values from input list
# 'counters' of counters from counter report.  Report a problem if one is found
# and return the appropriate Nagios code.
###############################################################################
def CheckOutstandingMsgCount(counters):
    counter_dict = CounterListToDict(counters)
    msg_create_count = LookupCounter(counter_dict, 'MsgCreate')
    msg_destroy_count = LookupCounter(counter_dict, 'MsgDestroy')

    if msg_destroy_count > msg_create_count:
        ReportProblem(EC_CRITICAL, 'MsgDestroy counter value ' +
                      str(msg_destroy_count) +
                      ' is greater than MsgCreate counter value ' +
                      str(msg_create_count))
        return EC_CRITICAL

    msg_count = msg_create_count - msg_destroy_count

    if msg_count > Opts.MsgCountCriticalThreshold:
        ReportProblem(EC_CRITICAL, 'MsgCreate - MsgDestroy is ' +
                      str(msg_count))
        return EC_CRITICAL

    if msg_count > Opts.MsgCountWarnThreshold:
        ReportProblem(EC_WARNING, 'MsgCreate - MsgDestroy is ' +
                      str(msg_count))
        return EC_WARNING

    return EC_SUCCESS
###############################################################################

###############################################################################
# Compare counter info lists 'current_counters' and 'old_counters' and report
# a problem if counter 'MetadataUpdated' has not increased.  Return the
# appropriate Nagios code.
###############################################################################
def CheckMetadataUpdates(current_counters, old_counters):
    old_counter_dict = CounterListToDict(old_counters)
    new_counter_dict = CounterListToDict(current_counters)
    counter_name = 'GetMetadataSuccess'
    old_count = LookupCounter(old_counter_dict, counter_name)
    new_count = LookupCounter(new_counter_dict, counter_name)

    if new_count > old_count:
        return EC_SUCCESS

    ReportProblem(EC_WARNING, counter_name + ' not increasing: old value ' +
                  str(old_count) + ' new value ' + str(new_count))
    return EC_CRITICAL
###############################################################################

###############################################################################
# Delete any counter files whose ages exceed the configured history period.
###############################################################################
def DeleteOldCounterFiles(work_path, now, old_counter_file_times):
    # A value of 0 means "don't delete anything".
    if Opts.History == 0:
        return

    for epoch_seconds in old_counter_file_times:
        # Opts.History specifies a value in minutes.
        if (epoch_seconds > now) or \
                ((now - epoch_seconds) <= (60 * Opts.History)):
            break

        path = work_path + '/' + str(epoch_seconds)

        try:
            os.remove(path)
        except OSError as e:
            Die(EC_UNKNOWN, 'Failed to delete counter file ' + path + ': ' +
                e.strerror)
        except IOError as e:
            Die(EC_UNKNOWN, 'Failed to delete counter file ' + path + ': ' +
                e.strerror)
###############################################################################

###############################################################################
# Class for storing program options
#
# members:
#     Manual: If we are not running in manual mode, this is -1.  Otherwise,
#         it gives a number of seconds since the epoch specifying a counter
#         file to be manually analyzed.
#     WorkDir: A subdirectory beneath the Nagios user's home directory where
#         the script maintains all of its counter data.
#     Interval: The minimum number of seconds ago we will accept when looking
#         for a recent counter file to compare our metadata update count
#         against.
#     History: The number of minutes of history to preserve when deleting old
#         counter files.
#     NagiosServer: If not running in manual mode, this is a unique identifier
#         for the Nagios server that triggered the current execution of this
#         script.  If running in manual mode, this is a unique identifier for
#         the Nagios server that triggered the script execution that caused
#         creation of the counter file we are analyzing.
#     SocketErrorWarnThreshold: If the number of socket errors indicated by the
#         counter output exceeds this value, it is treated as Warning.
#     SocketErrorCriticalThreshold: If the number of socket errors indicated by
#         the counter output exceeds this value, it is treated as Critical.
#     MsgCountWarnThreshold: If the number of outstanding messages indicated by
#         the counter output exceeds this value, it is treated as Warning.
#     MsgCountCriticalThreshold: If the number of outstanding messages
#         indicated by the counter output exceeds this value, it is treated as
#         Critical.
#     BruceHost: The host running bruce that we should connect to for counter
#         data.
#     BruceStatusPort: The port to connect to when asking bruce for counter
#         data.
#     NagiosUser: The name of the nagios user.
###############################################################################
class TProgramOptions(object):
    'program options class'
    def __init__(self, manual, work_dir, interval, history, nagios_server,
                 socket_error_warn_threshold,
                 socket_error_critical_threshold, msg_count_warn_threshold,
                 msg_count_critical_threshold, bruce_host, bruce_status_port,
                 nagios_user):
        self.Manual = manual
        self.WorkDir = work_dir
        self.Interval = interval
        self.History = history
        self.NagiosServer = nagios_server
        self.SocketErrorWarnThreshold = socket_error_warn_threshold
        self.SocketErrorCriticalThreshold = socket_error_critical_threshold
        self.MsgCountWarnThreshold = msg_count_warn_threshold
        self.MsgCountCriticalThreshold = msg_count_critical_threshold
        self.BruceHost = bruce_host
        self.BruceStatusPort = bruce_status_port
        self.NagiosUser = nagios_user
###############################################################################

###############################################################################
# Parse command line arguments provided by input list parameter 'args' and
# return a corresponding TProgramOptions object on success.  If there is a
# problem with the arguments, die with an error message.
###############################################################################
def ParseArgs(args):
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'm:d:i:H:s:w:c:W:C:b:p:u:',
                ['manual', 'work_dir=', 'interval=', 'history=',
                 'nagios_server=', 'socket_error_warn_threshold=',
                 'socket_error_critical_threshold=',
                 'msg_count_warn_threshold=', 'msg_count_critical_threshold=',
                 'bruce_host=', 'bruce_status_port=', 'nagios_user='])
    except getopt.GetoptError as e:
        Die(EC_UNKNOWN, str(e))

    opt_manual = -1
    opt_work_dir = ''
    opt_interval = -1
    opt_history = -1
    opt_nagios_server = ''
    opt_socket_error_warn_threshold = -1
    opt_socket_error_critical_threshold = -1
    opt_msg_count_warn_threshold = -1
    opt_msg_count_critical_threshold = -1
    opt_bruce_host = ''
    opt_bruce_status_port = 9090
    opt_nagios_user = 'nrpe'

    for o, a in opts:
        if o in ('-m', '--manual'):
            try:
                opt_manual = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_manual < 0:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a nonnegative integer')
        elif o in ('-d', '--work_dir'):
            opt_work_dir = a
        elif o in ('-i', '--interval'):
            try:
                opt_interval = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_interval < 1:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a positive integer')
        elif o in ('-H', '--history'):
            try:
                opt_history = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_history < 0:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a nonnegative integer')
        elif o in ('-s', '--nagios_server'):
            opt_nagios_server = a
        elif o in ('-w', '--socket_error_warn_threshold'):
            try:
                opt_socket_error_warn_threshold = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_socket_error_warn_threshold < 0:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a nonnegative integer')
        elif o in ('-c', '--socket_error_critical_threshold'):
            try:
                opt_socket_error_critical_threshold = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_socket_error_critical_threshold < 0:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a nonnegative integer')
        elif o in ('-W', '--msg_count_warn_threshold'):
            try:
                opt_msg_count_warn_threshold = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_msg_count_warn_threshold < 0:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a nonnegative integer')
        elif o in ('-C', '--msg_count_critical_threshold'):
            try:
                opt_msg_count_critical_threshold = int(a)
            except ValueError:
                Die(EC_UNKNOWN, 'The ' + o + ' option requires an integer')

            if opt_msg_count_critical_threshold < 0:
                Die(EC_UNKNOWN,
                    'The ' + o + ' option requires a nonnegative integer')
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
        elif o in ('-u', '--nagios_user'):
            opt_nagios_user = a
        else:
            Die(EC_UNKNOWN, 'Unhandled command line option')

    if opt_work_dir == '':
        Die(EC_UNKNOWN, '-d or --work_dir option must be specified')

    if opt_interval == -1:
        Die(EC_UNKNOWN, '-i or --interval option must be specified')

    if opt_history == -1:
        if opt_manual == -1:
            Die(EC_UNKNOWN, '-H or --history option must be specified')
        else:
            opt_history = 0

    if opt_nagios_server == '':
        Die(EC_UNKNOWN, '-s or --nagios_server option must be specified')

    if opt_socket_error_warn_threshold == -1:
        Die(EC_UNKNOWN, '-w or --socket_error_warn_threshold option must be '
            'specified')

    if opt_socket_error_critical_threshold == -1:
        Die(EC_UNKNOWN, '-c or --socket_error_critical_threshold option '
            'must be specified')

    if opt_msg_count_warn_threshold == -1:
        Die(EC_UNKNOWN, '-W or --msg_count_warn_threshold option must be '
            'specified')

    if opt_msg_count_critical_threshold == -1:
        Die(EC_UNKNOWN, '-C or --msg_count_critical_threshold option must '
            'be specified')

    if (opt_bruce_host == '') and (opt_manual == -1):
        Die(EC_UNKNOWN, '-b or --bruce_host option must be specified')

    return TProgramOptions(opt_manual, opt_work_dir, opt_interval,
                           opt_history, opt_nagios_server,
                           opt_socket_error_warn_threshold,
                           opt_socket_error_critical_threshold,
                           opt_msg_count_warn_threshold,
                           opt_msg_count_critical_threshold, opt_bruce_host,
                           opt_bruce_status_port, opt_nagios_user)
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
    MakeDirExist(work_path)

    if RunningInManualMode():
        now = Opts.Manual
        counter_report = ReadCounterFile(work_path + '/' + str(now))
    else:
        counter_report = GetCounters('http://' + Opts.BruceHost + ':' + \
                str(Opts.BruceStatusPort) + '/counters/json')
        now = counter_report['now']
        CreateCounterFile(work_path + '/' + str(now), counter_report)

    old_counter_file_times = FindOldCounterFileTimes(work_path, now)
    last_report = GetLastCounterReport(counter_report, work_path,
                                   old_counter_file_times)

    if last_report == None:
        last_counters = None
    else:
        last_counters = last_report['counters']

    deltas = ComputeCounterDeltas(last_counters, counter_report['counters'])
    nagios_code = AnalyzeDeltas(deltas)
    nagios_code = max(nagios_code,
                      CheckOutstandingMsgCount(counter_report['counters']))
    older_report = GetOldCounterReport(counter_report, work_path,
                                       old_counter_file_times, Opts.Interval)

    if older_report != None:
        nagios_code = max(nagios_code,
                          CheckMetadataUpdates(counter_report['counters'],
                                               older_report['counters']))

    # Nagios expects some sort of output even in the case of a successful
    # result.
    if nagios_code == EC_SUCCESS:
        print "Ok"

    # Manual mode is used by a human being to view the details of problems
    # previously reported by this script while being executed by Nagios.  In
    # this case, deleting old counter files is not a desired behavior.
    if RunningInManualMode():
        print 'Nagios code: ' + NagiosCodeToString(nagios_code)
    else:
        DeleteOldCounterFiles(work_path, now, old_counter_file_times)

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
