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
#        'web001.ifwe.co'.  Likewise, the IP address for web001 will be
#        converted to 'web001.ifwe.co'.
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
#
#    -g
#    --ignore_bad_topics:
#        Do not report errors due to bad topic discards.
#
#    -i TOPIC_FILE
#    --ignore_topics_file TOPIC_FILE
#        Specifies a file containing discard topics to ignore when reporting
#        errors.  Discards for these topics are still stored in Oracle, but
#        will not cause reporting of Nagios errors.  The file is a simple text
#        file with one topic per line.
###############################################################################

import cx_Oracle
import errno
import getopt
import json
import os
import select
import signal
import socket
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

    result = []

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
# 'web001', this function will return 'web001.ifwe.co'.  Likewise, if the IP
# address of web001 is given as input, this function returns
# 'web001.ifwe.co'.  Throw a subclass of socket.error if there is a failure
# during name or address lookup.
###############################################################################
def CanonicalizeHost(host):
    if IsValidIpv4Address(host) or IsValidIpv6Address(host):
        return socket.gethostbyaddr(host)[0]

    return socket.gethostbyaddr(socket.gethostbyname(host))[0]
###############################################################################

###############################################################################
# Send a "get discards" HTTP request to bruce and return the response as a
# string.
###############################################################################
def GetDiscardResponse(url):
    try:
        response = urlopen(url)
    except URLError as e:
        # Treat this as Critical, since it indicates that bruce is probably not
        # running.
        Die(EC_CRITICAL, 'Failed to open discards URL: ' + str(e.reason))

    json_input = ''

    for line in response:
        json_input += line

    return json_input
###############################################################################

###############################################################################
# Validate a list of unicode strings within a discard report.  Silently return
# on success.  Die on error.
###############################################################################
def ValidateUnicodeListItem(report, item_name):
    if item_name not in report:
        Die(EC_UNKNOWN, 'Discard report missing "' + item_name + '" item')

    the_list = report[item_name]

    if type(the_list) is not list:
        Die(EC_UNKNOWN, 'Item "' + item_name + \
                '" in discard report is not a list')

    for item in the_list:
        if type(item) is not unicode:
            Die(EC_UNKNOWN, 'List "' + item_name + \
                    '" in discard report has item that is not unicode string')
###############################################################################

###############################################################################
# Validate a 'discard_topic' or 'possible_duplicate_topic' section within a
# discard report.  Silently return on success.  Die on error.
###############################################################################
def ValidateTopicIntervalList(report, list_name):
    if list_name not in report:
        Die(EC_UNKNOWN, 'Discard report missing "' + list_name + '" item')

    the_list = report[list_name]

    if type(the_list) is not list:
        Die(EC_UNKNOWN, 'Item "' + list_name + \
                '" in discard report is not a list')

    for item in the_list:
        if type(item) is not dict:
            Die(EC_UNKNOWN, 'List "' + list_name + \
                    '" in discard report has item that is not a dictionary')

        if 'topic' not in item:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report is missing topic')

        if type(item['topic']) is not unicode:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report has topic that is not unicode string')

        if 'min_timestamp' not in item:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report is missing min_timestamp')

        if type(item['min_timestamp']) is not int:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report has noninteger min_timestamp')

        if 'max_timestamp' not in item:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report is missing max_timestamp')

        if type(item['max_timestamp']) is not int:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report has noninteger max_timestamp')

        if 'count' not in item:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report is missing count')

        if type(item['count']) is not int:
            Die(EC_UNKNOWN, 'Item of list "' + list_name + '" in ' + \
                    'discard report has noninteger count')
###############################################################################

###############################################################################
# Validate a single discard report within a parsed discard response.  Silently
# return on success.  Die on error.
###############################################################################
def ValidateDiscardReport(report):
    if 'id' not in report:
        Die(EC_UNKNOWN, 'Discard report missing "id" item')

    if type(report['id']) is not int:
        Die(EC_UNKNOWN, 'Item "id" in discard report is not an integer')

    if 'start_time' not in report:
        Die(EC_UNKNOWN, 'Discard report missing "start_time" item')

    if type(report['start_time']) is not int:
        Die(EC_UNKNOWN,
                'Item "start_time" in discard report is not an integer')

    if 'malformed_msg_count' not in report:
        Die(EC_UNKNOWN, 'Discard report missing "malformed_msg_count" item')

    if type(report['malformed_msg_count']) is not int:
        Die(EC_UNKNOWN, 'Item "malformed_msg_count" in discard report is ' + \
                'not an integer')

    if 'unsupported_api_key_msg_count' not in report:
        Die(EC_UNKNOWN, 'Discard report missing ' + \
                '"unsupported_api_key_msg_count" item')

    if type(report['unsupported_api_key_msg_count']) is not int:
        Die(EC_UNKNOWN, 'Item "unsupported_api_key_msg_count" in discard ' + \
                'report is not an integer')

    if 'unsupported_version_msg_count' not in report:
        Die(EC_UNKNOWN, 'Discard report missing ' + \
                '"unsupported_version_msg_count" item')

    if type(report['unsupported_version_msg_count']) is not int:
        Die(EC_UNKNOWN, 'Item "unsupported_version_msg_count" in discard ' + \
                'report is not an integer')

    if 'bad_topic_msg_count' not in report:
        Die(EC_UNKNOWN, 'Discard report missing "bad_topic_msg_count" item')

    if type(report['bad_topic_msg_count']) is not int:
        Die(EC_UNKNOWN, 'Item "bad_topic_msg_count" in discard report is ' + \
                'not an integer')

    ValidateUnicodeListItem(report, 'recent_malformed')

    if 'unsupported_msg_version' not in report:
        Die(EC_UNKNOWN,
                'Discard report missing "unsupported_msg_version" item')

    if type(report['unsupported_msg_version']) is not list:
        Die(EC_UNKNOWN, 'Item "unsupported_msg_version" in discard report ' + \
                'is not a list')

    for item in report['unsupported_msg_version']:
        if type(item) is not dict:
            Die(EC_UNKNOWN, 'List "unsupported_msg_version" in discard ' + \
                    'report has item that is not a dictionary')

        if 'version' not in item:
            Die(EC_UNKNOWN, 'Item of list "unsupported_msg_version" in ' + \
                    'discard report is missing version')

        if type(item['version']) is not int:
            Die(EC_UNKNOWN, 'Item of list "unsupported_msg_version" in ' + \
                    'discard report has noninteger version')

        if 'count' not in item:
            Die(EC_UNKNOWN, 'Item of list "unsupported_msg_version" in ' + \
                    'discard report is missing count')

        if type(item['count']) is not int:
            Die(EC_UNKNOWN, 'Item of list "unsupported_msg_version" in ' + \
                    'discard report has noninteger count')

    ValidateUnicodeListItem(report, 'recent_bad_topic')
    ValidateUnicodeListItem(report, 'recent_too_long_msg')

    if type(report['rate_limit_discard']) is not list:
        Die(EC_UNKNOWN, 'Item "rate_limit_discard" in discard report ' + \
                'is not a list')

    for item in report['rate_limit_discard']:
        if type(item) is not dict:
            Die(EC_UNKNOWN, 'List "rate_limit_discard" in discard report ' + \
                    'has item that is not a dictionary')

        if 'topic' not in item:
            Die(EC_UNKNOWN, 'Item of list "rate_limit_discard" in discard ' + \
                    'report is missing version')

        if type(item['topic']) is not unicode:
            Die(EC_UNKNOWN, 'Item of list "rate_limit_discard" in discard ' + \
                    'report has topic that is not unicode string')

        if 'count' not in item:
            Die(EC_UNKNOWN, 'Item of list "rate_limit_discard" in discard ' + \
                    'report is missing count')

        if type(item['count']) is not int:
            Die(EC_UNKNOWN, 'Item of list "rate_limit_discard" in discard ' + \
                    'report has noninteger count')

    ValidateTopicIntervalList(report, 'discard_topic')
    ValidateTopicIntervalList(report, 'possible_duplicate_topic')
###############################################################################

###############################################################################
# Parse a JSON discard response and return the parsed result.  Die on error.
# If we got the response directly from Bruce, then 'host' and 'port' will
# provide the canonicalized hostname where Bruce is running, and the port Bruce
# is listening on.  Otherwise, the response was obtained from a file that got
# created due to a previous database failure.  In this case, 'host' and 'port'
# will be None.
###############################################################################
def ParseDiscardResponse(json_input, host, port):
    try:
        result = json.loads(json_input)
    except ValueError:
        Die(EC_UNKNOWN, 'Failed to parse discard report')

    if type(result) is not dict:
        Die(EC_UNKNOWN, 'Discard response is not a dictionary')

    if 'now' not in result:
        Die(EC_UNKNOWN, 'Discard response missing "now" item')

    if type(result['now']) is not int:
        Die(EC_UNKNOWN, 'Item "now" in discard response is not an integer')

    if 'pid' not in result:
        Die(EC_UNKNOWN, 'Discard response missing "pid" item')

    if type(result['pid']) is not int:
        Die(EC_UNKNOWN, 'Item "pid" in discard response is not an integer')

    if 'version' not in result:
        Die(EC_UNKNOWN, 'Discard response missing "version" item')

    if type(result['version']) is not unicode:
        Die(EC_UNKNOWN,
            'Item "version" in discard response is not a unicode string')

    if 'interval' not in result:
        Die(EC_UNKNOWN, 'Discard response missing "interval" item')

    if type(result['interval']) is not int:
        Die(EC_UNKNOWN,
                'Item "interval" in discard response is not an integer')

    if 'unfinished_report' not in result:
        Die(EC_UNKNOWN, 'Discard response missing "unfinished_report" item')

    if type(result['unfinished_report']) is not dict:
        Die(EC_UNKNOWN, 'Item "unfinished_report" in discard response is ' + \
                'not a dictionary')

    ValidateDiscardReport(result['unfinished_report'])

    if host is None:
        if 'host' not in result['unfinished_report']:
            Die(EC_UNKNOWN, '"host" missing from saved discard response')
    else:
        result['unfinished_report']['host'] = host

    if port is None:
        if 'port' not in result['unfinished_report']:
            Die(EC_UNKNOWN, '"port" missing from saved discard response')
    else:
        result['unfinished_report']['port'] = port

    result['unfinished_report']['end_time'] = \
            result['unfinished_report']['start_time'] + result['interval']
    result['unfinished_report']['pid'] = result['pid']

    if 'finished_report' in result:
        if type(result['finished_report']) is not dict:
            Die(EC_UNKNOWN, 'Item "finished_report" in discard response ' + \
                    'is not a dictionary')

        ValidateDiscardReport(result['finished_report'])

        if host is None:
            if 'host' not in result['finished_report']:
                Die(EC_UNKNOWN, '"host" missing from saved discard response')
        else:
            result['finished_report']['host'] = host

        if port is None:
            if 'port' not in result['finished_report']:
                Die(EC_UNKNOWN, '"port" missing from saved discard response')
        else:
            result['finished_report']['port'] = port

        result['finished_report']['end_time'] = \
                result['finished_report']['start_time'] + result['interval']
        result['finished_report']['pid'] = result['pid']

    return result
###############################################################################

###############################################################################
# Serialize previously parsed JSON content back to JSON and return the result.
###############################################################################
def SerializeToJson(structured_data):
    return json.dumps(structured_data, sort_keys=True, indent=4,
                separators=(',', ': ')) + '\n'
###############################################################################

###############################################################################
# Analyze input parameter 'report', looking for discards.  Ignore any topics in
# input set 'ignore_topics'.  Return EC_SUCCESS if no discards are found.
# Otherwise return EC_CRITICAL.
###############################################################################
def AnalyzeDiscardReport(report, ignore_topics):
    if report['malformed_msg_count'] > 0:
        return EC_CRITICAL

    if (not Opts.IgnoreBadTopics) and report['bad_topic_msg_count'] > 0:
        return EC_CRITICAL

    for topic_item in report['discard_topic']:
        if topic_item['topic'] not in ignore_topics:
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
# discard report.  Write a row representing the report to the
# BRUCE_DATA_QUALITY_REPORT table.  On success, return the integer value from
# the ID column of the newly inserted row.  If a row for the given report was
# already in the table, return None.  On error, let cx_Oracle.Error exception
# propagate to caller.
###############################################################################
def InsertReportRow(cur, report):
    # Add report row to BRUCE_DATA_QUALITY_REPORT table.

    report_start = EpochSecondsToTimeString(report['start_time'])
    report_end = EpochSecondsToTimeString(report['end_time'])
    rows = [ (ToAscii(report['host']), report['port'], report['id'],
              report_start, report_end, report['malformed_msg_count'],
              report['unsupported_version_msg_count'],
              report['bad_topic_msg_count'],
              report['pid'])
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
    sql_params = { 'host' : ToAscii(report['host']),
                   'port' : str(report['port']),
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
    if not string_list:
        return

    rows = []

    for item in string_list:
        t = (report_id, ToAscii(item))
        rows.append(t)

    cur.setinputsizes(int, string_max_size)
    sql = 'insert into ' + table_spec + ' values (:1, :2)'
    cur.executemany(sql, rows)
###############################################################################

###############################################################################
# Insert a set of rows into table BRUCE_RATE_LIMIT_DISCARD.  Input parameter
# 'cur' provides a database cursor, and 'report_id' provides the discard report
# ID, which is a foreign key into table BRUCE_DATA_QUALITY_REPORT.
# 'topic_max_size' provides the maximum length of a topic string, and
# 'rate_limit_discard_info_list' provides a possibly empty list of information
# on discards due to Bruce's message rate limiting mechanism.
###############################################################################
def InsertRateLimitDiscardRows(cur, report_id, topic_max_size,
                               rate_limit_discard_info_list):
    if not rate_limit_discard_info_list:
        return

    rows = []

    for item in rate_limit_discard_info_list:
        t = (report_id, ToAscii(item['topic']), item['count'])
        rows.append(t)

    cur.setinputsizes(int, topic_max_size, int)
    sql = 'insert into BRUCE_RATE_LIMIT_DISCARD(ID, TOPIC, MSG_COUNT) ' + \
            'values(:1, :2, :3)'
    cur.executemany(sql, rows)
###############################################################################

###############################################################################
# Insert a set of rows into either table BRUCE_DISCARD_TOPIC or
# BRUCE_POSSIBLE_DUP_TOPIC, depending on the text blurb in input parameter
# 'table_spec', which gets incorporated into the SQL insert statement.  The
# data for the rows is provided by input parameter topic_error_info_list.
# Input parameter 'report_id' gives the report ID foreign key referencing table
# BRUCE_DATA_QUALITY_REPORT.  Input parameter 'topic_max_size' specifies the
# maximum allowed size of the string values, as defined by the schema.  Input
# parameter 'cur' provides a database cursor.
###############################################################################
def InsertTopicErrorInfoRows(cur, report_id, table_spec, topic_max_size,
                             topic_error_info_list):
    if not topic_error_info_list:
        return

    rows = []

    for item in topic_error_info_list:
        t = (report_id, ToAscii(item['topic']), item['min_timestamp'],
             item['max_timestamp'], item['count'])
        rows.append(t)

    cur.setinputsizes(int, topic_max_size, int, int, int)
    sql = 'insert into ' + table_spec + ' values (:1, :2, :3, :4, :5)'
    cur.executemany(sql, rows)
###############################################################################

###############################################################################
# Input parameter 'report' is a parsed discard report.  Input parameter 'con'
# is an active database connection.  Input parameter 'cur' is a database cursor
# for the given connection.  Write the report to the database.  Return on
# success, or raise exception on error.
# Exceptions raised: TDatabaseError, cx_Oracle.Warning, cx_Oracle.Error
###############################################################################
def DoPersistDiscardReportImpl(con, cur, report):
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
                     report['recent_malformed'])

    # Add rows to BRUCE_TOO_LONG_MSG table.
    InsertStringRows(cur, report_id, 'BRUCE_TOO_LONG_MSG(ID, MSG)', 4000,
                     report['recent_too_long_msg'])

    if report['unsupported_msg_version']:
        # Add rows to BRUCE_UNSUPP_VERSION_MSG table.

        rows = []

        for item in report['unsupported_msg_version']:
            t = (report_id, item['version'], item['count'])
            rows.append(t)

        cur.setinputsizes(int, int, int)
        sql = 'insert into ' + \
              'BRUCE_UNSUPP_VERSION_MSG(ID, MSG_VERSION, MSG_COUNT) ' + \
          'values (:1, :2, :3)'
        cur.executemany(sql, rows)

    # Add rows to BRUCE_BAD_TOPIC table.
    InsertStringRows(cur, report_id, 'BRUCE_BAD_TOPIC(ID, TOPIC)', 100,
                     report['recent_bad_topic'])

    # Add rows to BRUCE_RATE_LIMIT_DISCARD table.
    InsertRateLimitDiscardRows(cur, report_id, 100,
            report['rate_limit_discard'])

    # Add rows to BRUCE_DISCARD_TOPIC table.
    InsertTopicErrorInfoRows(cur, report_id,
            'BRUCE_DISCARD_TOPIC(ID, TOPIC, TIMESTAMP_1, TIMESTAMP_2, ' +
            'DISCARD_COUNT)', 100, report['discard_topic'])

    # Add rows to BRUCE_POSSIBLE_DUP_TOPIC table.
    InsertTopicErrorInfoRows(cur, report_id,
            'BRUCE_POSSIBLE_DUP_TOPIC(ID, TOPIC, TIMESTAMP_1, TIMESTAMP_2, ' +
            'DUP_COUNT)', 100, report['possible_duplicate_topic'])

    # Commit transaction.
    con.commit()

    if Opts.Verbose:
        print 'Committed report ID ' + str(report_id)
###############################################################################

###############################################################################
# Input parameter 'report' is a parsed discard report.  Write the report to the
# database.  Return the empty string on success, or an error message on error.
# The entire report is written as a single transaction, so the database update
# is all or nothing.  The given discard report may already be in the database
# if the last time this script ran is recent enough.  In this case, we return
# the empty string (indicating success) without any database update.  Input
# paramaters 'username' and 'password' provide the credentials for database
# access.
###############################################################################
def PersistDiscardReportImpl(report, username, password):
    db_string = username + '/' + password + '@' + Opts.Dbhost + ':' + \
            str(Opts.Dbport) + '/' + Opts.Database
    con = None  # database connection
    cur = None  # database cursor

    try:
        con = cx_Oracle.connect(db_string)
        cur = con.cursor()
        cur.bindarraysize = 256
        DoPersistDiscardReportImpl(con, cur, report)
        signal.alarm(0)  # cancel alarm on success
        result = ''
    except TDatabaseError, x:
        # These errors are not reported by Oracle, but by logic within this
        # script.
        result = 'Persist error: ' + x.message
        print result
    except cx_Oracle.Error, x:
        result = 'Database error: ' + str(x)
        print result
    except cx_Oracle.Warning, x:
        # Here we make the conservative assumption that in the case of a
        # warning, the database transaction failed.
        result = 'Database warning: ' + str(x)
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
# 'persist_fn' is a lambda that persists a discard report to Oracle.  It
# returns the empty string on success or an error message on failure.  fork() a
# child process and let the child do the database operation.  If 'persist_fn'
# returns an error string, then return the error string.  If the child does not
# finish within 'timeout_ms' milliseconds then return an error message
# indicating timeout.  If 'persist_fn' returns the empty string, then return
# the empty string (indicating success).
#
# In a previous implementation, SIGALRM was used to implement a timeout for the
# database operation, but this did not work well in practice.  Probably the
# cx_Oracle library or some other piece of code was masking SIGALRM and then
# getting stuck somewhere.  Hopefully the approach below will be a more
# reliable mechanism for implementing the timeout.
###############################################################################
def RunPersistFnAsChild(persist_fn, timeout_ms):
    global pipe_to_parent
    read_fd, write_fd = os.pipe()
    pid = os.fork()

    if pid:
        # parent executes here
        os.close(write_fd)
        poller = select.poll()
        poller.register(read_fd, select.POLLIN | select.POLLHUP)
        events = poller.poll(timeout_ms)

        if not events:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

            return 'Timed out waiting for child to perform database operation'

        if events[0][1] & select.POLLIN:
            read_end = os.fdopen(read_fd)

            # return error string from pipe
            return read_end.read()
    else:
        # child executes here
        os.close(read_fd)
        write_end = os.fdopen(write_fd, 'w')

        # Write the empty string on success, or an error message on error.
        write_end.write(persist_fn())

        try:
            write_end.close()
        except IOError:
            pass

        sys.exit(0)

    # empty error string indicates child was successful
    return ''
###############################################################################

###############################################################################
def PersistDiscardReport(report, username, password):
    my_persist_fn = \
            lambda : PersistDiscardReportImpl(report, username, password)
    return RunPersistFnAsChild(my_persist_fn, Opts.DatabaseTimeout * 1000)
###############################################################################

###############################################################################
# Open file specified by 'path' for reading.  Read entire file contents into a
# string and return the result.
###############################################################################
def ReadFileContents(path):
    json_response = ''

    try:
        is_open = False
        infile = open(path, 'r')
        is_open = True

        for line in infile:
            json_response += line
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + path + ' for reading: ' + \
                e.strerror + '\n')
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + path + ' for reading: ' + \
                e.strerror + '\n')
    finally:
        if is_open:
            infile.close()

    return json_response
###############################################################################

###############################################################################
# Return EC_CRITICAL if the age in seconds of the oldest timestamp in
# 'timestamp_list' (an array of integers representing seconds since the epoch)
# exceeds Opts.MaxReportAge.  Otherwise return EC_SUCCESS.  It is assumed that
# 'timestamp_list' is sorted in ascending order.
###############################################################################
def CheckDiscardReportFileAges(timestamp_list):
    if timestamp_list:
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
# Retry database write operation for discard report that we previously failed
# to save to database.  Return True on success or False on error.
###############################################################################
def RetryPersistDiscardReport(filename, timestamp, username, password):
    if Opts.Verbose:
        print 'Retrying persist of discard report ' + str(timestamp)

    response = ParseDiscardResponse(ReadFileContents(filename), None, None)

    if 'finished_report' not in response:
        print 'Failed to obtain finished report from saved discard ' + \
                'response ' + str(timestamp)
        return False

    err_msg = PersistDiscardReport(response['finished_report'], username,
            password)

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
# is sometimes down, so this is a workaround.
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
            failed_timestamp_list = []
            failed_timestamp_list.append(timestamp)
            return CheckDiscardReportFileAges(failed_timestamp_list)

    return EC_SUCCESS
###############################################################################

###############################################################################
# This is called after we have failed to persist a discard report.  Save the
# report temporarily to a local file, so we can try again later.
###############################################################################
def HandlePersistFailure(work_path, report_start, response, err_msg):
    if Opts.Verbose:
        print 'Handling failure to persist discard report ' + str(report_start)

    file_list = FindDiscardFileTimes(work_path)

    for item in file_list:
        if item == report_start:
            if Opts.Verbose:
                print 'Discard report ' + str(report_start) + \
                        ' already saved to local file system'

            return CheckDiscardReportFileAges(file_list)

    filename = work_path + '/' + str(report_start)
    response['err_msg'] = err_msg
    json_response = SerializeToJson(response)

    try:
        is_open = False
        outfile = open(filename, 'w')
        is_open = True
        outfile.write(json_response)
    except OSError as e:
        print 'Failed to create discard report file [' + filename + ']: ' + \
                e.strerror
        return EC_CRITICAL
    except IOError as e:
        print 'Failed to create discard report file [' + filename + ']: ' + \
                e.strerror
        return EC_CRITICAL
    finally:
        if is_open:
            outfile.close()

    if Opts.Verbose:
        print 'Discard report ' + str(report_start) + \
                ' has been saved to local file system'

    return CheckDiscardReportFileAges(file_list)
###############################################################################

###############################################################################
# Input parameter 'unicode_str' is a unicode string.  Convert its contents to
# ASCII and return the result as a normal string.
###############################################################################
def ToAscii(unicode_str):
    return unicode_str.encode('ascii', 'ignore')
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

    if type(parsed_json) is not dict:
        Die(EC_CRITICAL, 'Credentials file data is in unexpected format')

    try:
        username_u = parsed_json['username']
        password_u = parsed_json['password']
    except KeyError, x:
        Die(EC_CRITICAL, 'Credentials file data is in unexpected format')

    if type(username_u) is not unicode or type(password_u) is not unicode:
        Die(EC_CRITICAL, 'Credentials file data is in unexpected format')

    return (ToAscii(username_u), ToAscii(password_u))
###############################################################################

###############################################################################
# If 'ignore_topics_file' is a nonempty string, open the file and read it line
# by line.  Each line is interpreted as a topic.  Return a set containing all
# topics from the file.  If 'ignore_topics_file' is the empty string, return an
# empty set.
###############################################################################
def BuildIgnoreTopicSet(ignore_topics_file):
    topics = set([])

    if ignore_topics_file == '':
        return topics

    try:
        is_open = False
        infile = open(ignore_topics_file, 'r')
        is_open = True

        for line in infile:
            topics.add(line.rstrip('\n'))
    except OSError as e:
        Die(EC_UNKNOWN, 'Failed to file ' + ignore_topics_file +
            ' with topics to ignore: ' + e.strerror)
    except IOError as e:
        Die(EC_UNKNOWN, 'Failed to open file ' + ignore_topics_file +
            ' with topics to ignore: ' + e.strerror)
    finally:
        if is_open:
            infile.close()

    return topics
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
#     IgnoreBadTopics: Don't report errors for bad topics.
#     IgnoreTopicsFile: File containing discard topics to ignore when reporting
#         Nagios errors.  File format is text with one topic per line.
#         Discards for ignored topics are still stored in Oracle.
###############################################################################
class TProgramOptions(object):
    'program options class'
    def __init__(self, verbose, work_dir, nagios_server, max_report_age,
                 nagios_user, bruce_host, bruce_status_port, testfile, dbhost,
                 dbport, database, database_timeout, credentials,
                 ignore_bad_topics, ignore_topics_file):
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
        self.IgnoreBadTopics = ignore_bad_topics
        self.IgnoreTopicsFile = ignore_topics_file
###############################################################################

###############################################################################
# Parse command line arguments provided by input list parameter 'args' and
# return a corresponding TProgramOptions object on success.  If there is a
# problem with the arguments, die with an error message.
###############################################################################
def ParseArgs(args):
    try:
        opts, args = getopt.getopt(sys.argv[1:],
                'vw:s:a:u:b:p:t:D:P:d:T:c:gi:',
                ['verbose', 'work_dir=', 'nagios_server=', 'max_report_age=',
                 'nagios_user=', 'bruce_host=', 'bruce_status_port=',
                 'testfile=', 'dbhost=', 'dbport=', 'database=',
                 'database_timeout=', 'credentials=', 'ignore_bad_topics',
                 'ignore_topics_file'])
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
    opt_ignore_bad_topics = False
    opt_ignore_topics_file = ''

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
        elif o in ('-g', '--ignore_bad_topics'):
            opt_ignore_bad_topics = True
        elif o in ('-i', '--ignore_topics_file'):
            opt_ignore_topics_file = a
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
                           opt_credentials, opt_ignore_bad_topics,
                           opt_ignore_topics_file)
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

    try:
        # Get canonicalized hostname.
        host = CanonicalizeHost(Opts.BruceHost)
    except socket.error, x:
        print 'Failed to canonicalize bruce host ' + Opts.BruceHost + ': ' + \
                str(x)
        sys.exit(EC_CRITICAL)

    if Opts.Testfile == '':
        # Get discard info from bruce.
        json_response = GetDiscardResponse('http://' + host + ':' + \
                str(Opts.BruceStatusPort) + '/discards/json')
    else:
        json_response = ReadFileContents(Opts.Testfile)

    response = ParseDiscardResponse(json_response, host, Opts.BruceStatusPort)
    unfinished_report = response['unfinished_report']
    finished_report = None

    if 'finished_report' in response:
        finished_report = response['finished_report']

    ignore_topics = BuildIgnoreTopicSet(Opts.IgnoreTopicsFile)

    # See if reports contain any discards.
    if finished_report == None:
        nagios_code_from_reports = \
                AnalyzeDiscardReport(unfinished_report, ignore_topics)
    else:
        nagios_code_from_reports = \
                max(AnalyzeDiscardReport(unfinished_report, ignore_topics),
                    AnalyzeDiscardReport(finished_report, ignore_topics))

    if Opts.Verbose:
        print SerializeToJson(response)

    if Opts.Dbhost != '' and finished_report != None:
        # Store latest finished discard report in database.
        (username, password) = \
                ParseCredentialsFileContents(
                        ReadFileContents(Opts.Credentials))
        persist_result = \
                PersistDiscardReport(finished_report, username, password)

        if persist_result == '':
            # Success.  See if there are any prior discard reports that we were
            # unable to persist.  If so, try again now.
            nagios_code_from_persist = \
                    CheckPriorPersistFailures(work_path, username, password)
        else:
            nagios_code_from_persist = HandlePersistFailure(work_path,
                finished_report['start_time'], response, persist_result)
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
        sys.stdout.write(json_response)

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
