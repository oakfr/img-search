#!/usr/bin/env python

""" This program performs sanity checks on the Hadoop gateway. """

import os
import sys
import time
import json
import datetime
from util import util_file_size, util_run_cmd, util_send_email, util_get_platform
import subprocess
from pid import PidFile
import pipes

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


def delay_sec_to_string(d):
    if d < 60:
        return '%d secs' % d
    if d < 3600:
        return '%d mins' % round(1.0*d/60)
    return '%1.1f hours' % (1.0 * d / 3600)


def exists_dir_hdfs(path):
    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-d', path])
    proc.wait()
    return proc.returncode == 0


def exists_file_hdfs(path):
    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', path],
                            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    #result = proc.stdout.readlines()
    # if result == []:
    #    error = proc.stderr.readlines()
    #    _log ('***ERROR *** %s' % error)
    #    return True # do not raise false alarm if you cannot reach HDFS
    return proc.returncode == 0


def send_jar_command(channel, metric, value):
    jar_cmd = 'yarn jar /home/o.koch/ref/recocomputer/releases/1455705605_1.0-SNAPSHOT/lib/criteo-hadoop-recocomputer.jar com.criteo.hadoop.recocomputer.utils.logging.GraphiteMetricsLogger'
    cmd = '%s %s %s:%s' % (jar_cmd, channel, metric, value)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)


def count_aws_dirs_to_process(host, path):
    proc = subprocess.Popen(['ssh', host, 'ls -l %s/*/.success.baloo | wc -l' % pipes.quote(path)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    result = proc.stdout.readlines()
    if result == []:
        error = proc.stderr.readlines()
        _log('%s' % error)
        return None
    return int(result[0].strip())


def check_partners():

    hdfs_input_json_filename = '/user/recocomputer/bestofs/imagedl/outputByPartner'
    hdfs_output_json_filename = '/user/o.koch/cnn/outputByPartner'
    input_json_filename = '/home/o.koch/image-search/.input.outputByPartner.argus'
    output_json_filename = '/home/o.koch/image-search/.output.outputByPartner.argus'
    timestamp_filename = '/home/o.koch/image-search/lastPartnerTimestamp.txt'
    hdfs_root = '/user/o.koch/cnn/'
    hathi_lastrun_filename = '/home/o.koch/image-search/.hathi.lastrun.timestamp'
    baloo_lastrun_filename = '/home/o.koch/image-search/.baloo.lastrun.timestamp'
    host = 'ubuntu@176.34.228.64'

    content = ''
    good_to_go = True

    # count dirs to process
    n_dirs = count_aws_dirs_to_process(host, '/opt/input')
    if n_dirs is not None:
        _log('%d dirs left to process on AWS' % n_dirs)
        content += '%d dirs left to process on AWS\n' % n_dirs

    # compute job delays
    hathi_lastrun_timestamp = 0
    baloo_lastrun_timestamp = 0
    if os.path.isfile(hathi_lastrun_filename):
        with open(hathi_lastrun_filename, 'r') as gp:
            hathi_lastrun_timestamp = int(gp.read())
    if os.path.isfile(baloo_lastrun_filename):
        with open(baloo_lastrun_filename, 'r') as gp:
            baloo_lastrun_timestamp = int(gp.read())

    ref_timestamp = int(time.time())
    hathi_delay = ref_timestamp - hathi_lastrun_timestamp
    baloo_delay = ref_timestamp - baloo_lastrun_timestamp

    _log('Send-to-AWS   delay : %s' % delay_sec_to_string(baloo_delay))
    _log('Recv-from-AWS delay : %s' % delay_sec_to_string(hathi_delay))

    content += 'Send-to-AWS   delay : %s\n' % delay_sec_to_string(baloo_delay)
    content += 'Recv-from-AWS delay : %s\n' % delay_sec_to_string(hathi_delay)

    # remove local files
    if os.path.isfile(input_json_filename):
        os.remove(input_json_filename)
    if os.path.isfile(output_json_filename):
        os.remove(output_json_filename)

    # fetch input outputByPartner on HDFS
    cmd = 'hadoop fs -get %s %s' % (hdfs_input_json_filename,
                                    input_json_filename)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)
        content += '*** ERROR *** %s\n' % cmd_err
        good_to_go = False

    if util_file_size(input_json_filename) == 0:
        _log('*** ERROR *** did not find outputByPartner at this location on HDFS : %s' %
             hdfs_input_json_filename)
        content += '*** ERROR *** did not find outputByPartner at this location on HDFS : %s\n' % hdfs_input_json_filename
        good_to_go = False

    # fetch output outputByPartner on HDFS
    cmd = 'hadoop fs -get %s %s' % (hdfs_output_json_filename,
                                    output_json_filename)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)
        content += '*** ERROR *** %s\n' % cmd_err
        good_to_go = False

    if util_file_size(output_json_filename) == 0:
        _log('*** ERROR *** did not find outputByPartner at this location on HDFS : %s' %
             hdfs_output_json_filename)
        content += '*** ERROR *** did not find outputByPartner at this location on HDFS : %s\n' % hdfs_output_json_filename
        good_to_go = False

    if not good_to_go:
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

    # parse input data
    input_data = None
    with open(input_json_filename, 'r') as fp:
        json_data = fp.read()
        try:
            input_data = json.loads(json_data)
        except:
            _log('*** ERROR *** Failed to read JSON file %s.  Exiting.' %
                 input_json_filename)
            content += '*** ERROR *** Failed to read JSON file %s.  Exiting.\n' % input_json_filename
            good_to_go = False

    # parse output data
    output_data = None
    with open(output_json_filename, 'r') as fp:
        json_data = fp.read()
        try:
            output_data = json.loads(json_data)
        except:
            _log('*** ERROR *** Failed to read JSON file %s.  Exiting.' %
                 output_json_filename)
            content += '*** ERROR *** Failed to read JSON file %s.  Exiting.\n' % output_json_filename
            good_to_go = False

    if not good_to_go:
        return

    assert (input_data is not None)
    assert (output_data is not None)

    computed_size = 0
    remaining_size = 0
    nb_skipped_partners = 0
    nb_total_partners = 0

    # compute amount of data to process
    for item in input_data:
        partner_id = int(item)
        partner_timestamp = int(input_data[item]['jobTimeStamp'])
        nb_total_partners += 1

        if partner_id in last_partnersTimestamp and last_partnersTimestamp[partner_id] == partner_timestamp:
            computed_size += input_data[item]['outputSize']
        else:
            remaining_size += input_data[item]['outputSize']
            nb_skipped_partners += 1

    computed_size_gb = 1.0 * computed_size / 1000000000
    remaining_size_gb = 1.0 * remaining_size / 1000000000

    _log('Computed size  : %4.1f GB' % computed_size_gb)
    _log('Remaining size : %4.1f GB' % remaining_size_gb)
    _log('# skipped partners : %d out of %d total' %
         (nb_skipped_partners, nb_total_partners))

    content += 'Computed size  : %4.1f GB\n' % computed_size_gb
    content += 'Remaining size : %4.1f GB\n' % remaining_size_gb
    content += '# skipped partners : %d out of %d total\n' % (
        nb_skipped_partners, nb_total_partners)

    # check for missing partners in output
    for item in input_data:
        if item not in output_data:
            _log('*** WARNING *** Partner %s missing in output.' % item)
            content += '*** WARNING *** Partner %s missing in output.\n' % item

    # check that all files exist on HDFS
    n_files = 0
    n_files_success = 0

    check_hdfs = True
    if check_hdfs:
        for item in input_data:
            if item not in output_data:
                continue

            # check that files exist on HDFS
            output_folder = output_data[item]['outputFolder']
            output_files = output_data[item]['files']
            for filename in output_files:
                hdfs_path = os.path.join(hdfs_root, output_folder, filename)
                n_files += 1
                print('checking %s' % hdfs_path)
                if not exists_file_hdfs(hdfs_path):
                    _log(
                        '*** ERROR *** File %s does not exist on HDFS but is listed in outputByPartner.' % hdfs_path)
                    content += '*** ERROR *** File %s does not exist on HDFS but is listed in outputByPartner.\n' % hdfs_path
                else:
                    n_files_success += 1

        _log('%d/%d files checked successfully on HDFS.' %
             (n_files_success, n_files))
        content += '%d/%d files checked successfully on HDFS.\n' % (
            n_files_success, n_files)

    # alert?
    warning = False
    alert = False
    if n_files_success != n_files:
        alert = True
        content = '*** Warning! Some files seem to be missing on HDFS ***\n' + content

    if (baloo_delay > 12 * 3600) or (hathi_delay > 12 * 3600):
        alert = True
        content = '*** Warning! Some jobs are more than 12 hours old ***\n' + content

    elif (baloo_delay > 6 * 3600) or (hathi_delay > 6 * 3600):
        warning = True
        content = '*** Warning! Some jobs are more than 6 hours old ***\n' + content

    if alert:
        title = '[prod][%s][aws-tiger] Summary -- Alert' % util_get_platform()
    elif warning:
        title = '[prod][%s][aws-tiger] Summary -- Warning' % util_get_platform()
    else:
        title = '[prod][%s][aws-tiger] Summary -- OK' % util_get_platform()

    _log('Sending email with following title : %s' % title)

    # build email
    email_file = '/tmp/.email.argus.%d' % int(time.time())
    with open(email_file, 'w') as fp:
        fp.write(content)

    util_send_email('rd.recocomputer_jobs@lists.criteo.net',
                    title, email_file, 1000)
    util_send_email('o.koch@criteo.com', title, email_file, 1000)

    os.remove(email_file)


if __name__ == "__main__":

    with PidFile():

        # ESTOP check
        estop_filename = '/home/o.koch/image-search/ESTOP'
        if os.path.isfile(estop_filename):
            _log('ESTOP ENGAGED.  EXITING.')
            sys.exit(1)

        check_partners()
