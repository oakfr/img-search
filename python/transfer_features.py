#!/usr/bin/env python

""" Transfer features from AWS to HDFS """

import os
import sys
import datetime
import subprocess
import pipes
import time
import shutil
from util import util_send_email, util_timestamp_file_to_json, util_run_cmd, util_get_platform
import re
from pid import PidFile

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def _log(msg):
    dt_string = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    filename = os.path.basename(__file__)
    msg_string = '[%s]\t%s\t[%s]\t%s' % (
        util_get_platform(), dt_string, filename, msg)

    print(msg_string)

    with open('/home/o.koch/image-search/_%s.log' % filename, 'a') as fp:
        fp.write('%s\n' % msg_string)
        fp.close()

    # log error messages (but ignore HDFS warnings)
    if msg.find('ERROR') is not -1 and msg.find('WARN retry.RetryInvocationHandler') == -1:
        with open('/home/o.koch/image-search/_%s.log.error' % os.path.basename(__file__), 'a') as fp:
            fp.write('%s\n' % msg_string)
            fp.close()


def _take_lock():
    filename = '/home/o.koch/image-search/.hathi.lock'
    assert (not os.path.isfile(filename))
    with open(filename, 'w') as fp:
        fp.write('0')


def _is_locked():
    filename = '/home/o.koch/image-search/.hathi.lock'
    return os.path.isfile(filename)


def _release_lock():
    filename = '/home/o.koch/image-search/.hathi.lock'
    assert (os.path.isfile(filename))
    os.remove(filename)


def exists_remote(host, path):
    proc = subprocess.Popen(['ssh', host, 'test -f %s' % pipes.quote(path)])
    proc.wait()
    return proc.returncode == 0


def exists_hdfs(path):
    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-d', path])
    proc.wait()
    return proc.returncode == 0


def move_aws_directory(host, src, dest):
    proc = subprocess.Popen(['ssh', host, 'mv %s %s' %
                             (pipes.quote(src), pipes.quote(dest))])
    proc.wait()


def find_first_timestamp_remote(host, path):
    proc = subprocess.Popen(['ssh', host, 'ls -td -- %s/*/.success.akela | tail -n 1' % pipes.quote(path)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    result = proc.stdout.readlines()
    if result == []:
        error = proc.stderr.readlines()
        _log('%s' % error)
        return None
    return os.path.join(path, result[0].strip())


def list_remote_dirs(host, path):
    proc = subprocess.Popen(['ssh', host, 'ls -td -- %s/*/.success.akela' % pipes.quote(path)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    result = proc.stdout.readlines()
    if result == []:
        error = proc.stderr.readlines()
        _log('%s' % error)
        return None
    dirs = []
    for filename in result:
        dirs.append(os.path.dirname(filename.strip()))
    return dirs


def main():

    host = 'ubuntu@176.34.228.64'

    # get latest directory on AWS
    input_dirs = list_remote_dirs(host, '/opt/output-per-partner')
    if input_dirs is None:
        _log('Did not find directories in /opt/output-per-partner. Exiting.')
        return

    _log('Processing %d dirs from AWS' % len(input_dirs))

    for input_dir in input_dirs:
        process_dir(input_dir)


def process_dir(input_dir):

    host = 'ubuntu@176.34.228.64'
    local_dir = '/home/o.koch/image-search/output-per-partner'
    remote_success_file = '/opt/output-per-partner/.success'
    local_success_file = '/home/o.koch/image-search/_SUCCESS'
    timestamp_filename = '/home/o.koch/image-search/lastPartnerTimestamp_computed.txt'
    job_timestamp_filename = '/home/o.koch/image-search/.hathi.lastrun.timestamp'

    _log('Found following dir to process : %s' % input_dir)

    timestamp_aws = os.path.basename(input_dir)

    # skip directory if does not belong to your platform
    if util_get_platform() == 'pa4' and (int(timestamp_aws) % 2 == 0):
        _log('This is a directory for AM5.  Skipping...')
        return
    if util_get_platform() == 'am5' and (int(timestamp_aws) % 2 == 1):
        _log('This is a directory for PA4.  Skipping...')
        return

    hdfs_root = '/user/o.koch/cnn'
    hdfs_dir = '%s/%s' % (hdfs_root, timestamp_aws)

    # quit if directory already exists on HDFS
    if exists_hdfs(hdfs_dir):
        _log('Directory %s already exists on HDFS.  Skipping...' % hdfs_dir)
        return

    # create directory on hdfs
    cmd = 'hadoop fs -mkdir %s' % hdfs_dir
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)
        return

    # create local directory
    if not os.path.isdir(local_dir):
        os.makedirs(local_dir)

    # copy files to gateway
    cmd = 'scp -r %s:%s %s' % (host, input_dir, local_dir)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)
        return

    # load last timestamp for each partner
    last_partnersTimestamp = {}
    if os.path.isfile(timestamp_filename):
        with open(timestamp_filename, 'r') as fp:
            lines = fp.readlines()
            for l in lines:
                l = l.strip().split(' ')
                partnerid = int(l[0])
                timestamp = int(l[1])
                last_partnersTimestamp[partnerid] = timestamp

    # copy files to HDFS
    local_dir_output = os.path.join(local_dir, timestamp_aws)
    assert (os.path.isdir(local_dir_output))

    n_errors = 0

    for root, dirs, files in os.walk(local_dir_output):
        for filen in files:
            if not os.path.basename(filen).endswith('.gz'):
                continue
            filename = os.path.join(root, filen)

            # extract partner id from filename
            partnerid = int(re.findall('[0-9]+', filename)[-1])
            last_partnersTimestamp[partnerid] = int(timestamp_aws)

            hdfs_dir_full = '%s/%d' % (hdfs_dir, partnerid)

            # create subdir on HDFS
            cmd = 'hadoop fs -mkdir %s' % hdfs_dir_full
            cmd_out, cmd_err, rc = util_run_cmd(cmd)
            if rc != 0:
                _log('*** ERROR *** %s' % cmd_err)
                n_errors += 1
                break

            # transfer file to HDFS
            cmd = 'hadoop fs -put %s %s' % (filename, hdfs_dir_full)
            _log(cmd)
            cmd_out, cmd_err, rc = util_run_cmd(cmd)
            if rc != 0:
                _log('*** ERROR *** %s' % cmd_err)
                n_errors += 1
                break

    if n_errors > 0:
        _log('*** ERROR *** Encountered errors during copy. Exiting.')
        return

    _log('Done sending data to HDFS')

    # remove local dir to save space
    shutil.rmtree(local_dir_output)

    # write timestamps to file
    fp = open(timestamp_filename, 'w')
    for k, v in last_partnersTimestamp.iteritems():
        fp.write('%d %d\n' % (k, v))
    fp.close()

    _log('Done updating timestamps')

    # build json file
    json_file = os.path.join(local_dir, 'outputByPartner')
    total_size_bytes = util_timestamp_file_to_json(
        timestamp_filename, json_file)

    _log('Done creating JSON file')

    # remove remote json file
    cmd = 'hadoop fs -rm %s/outputByPartner' % hdfs_root
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)

    # send new json file
    cmd = 'hadoop fs -put %s %s' % (json_file, hdfs_root)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)

    # send success file
    with open(local_success_file, 'w') as fp:
        fp.write('0')

    cmd = 'hadoop fs -put %s %s' % (local_success_file, hdfs_dir)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)

    sent_mbytes = round(1.0*total_size_bytes/1000000)

    # move AWS input directory to bin
    #bin_input_dir = '/opt/old.output'
    #_log ('Moving AWS dir %s to %s' % (input_dir, bin_input_dir))
    #move_aws_directory (host, input_dir, bin_input_dir)
    #_log ('Done.')

    # write job timestamp to file
    with open(job_timestamp_filename, 'w') as gp:
        gp.write('%d' % int(time.time()))
        gp.close()

    _log('Summary : Sent %d MB to HDFS' % sent_mbytes)


if __name__ == "__main__":

    # header for the log file
    _log('======================================================================')

    # if _is_locked ():
    #    _log ('Hathi instance already running.  Exiting...')
    #    sys.exit(1)

    # _take_lock()

    with PidFile():

        # ESTOP check
        estop_filename = '/home/o.koch/image-search/ESTOP'
        if os.path.isfile(estop_filename):
            _log('ESTOP ENGAGED.  EXITING.')
            sys.exit(1)

        # main program
        main()

        # print last run timestamp to file
        with open('/home/o.koch/image-search/.hathi.timestamp', 'w') as fp:
            fp.write('%d' % int(time.time()))
            fp.close()

    # _release_lock()
