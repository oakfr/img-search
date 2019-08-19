#!/usr/bin/env python

""" Launch graphite commands for monitoring. """

import os
import sys
import time
import json
import datetime
from util import util_file_size, util_run_cmd, util_send_email, util_get_platform
import subprocess
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
    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', path])
    proc.wait()
    return proc.returncode == 0


def send_jar_command(channel, metric, value):
    jar_cmd = 'yarn jar /home/o.koch/ref/recocomputer/releases/1455705605_1.0-SNAPSHOT/lib/criteo-hadoop-recocomputer.jar com.criteo.hadoop.recocomputer.utils.logging.GraphiteMetricsLogger'
    cmd = '%s %s %s:%s' % (jar_cmd, channel, metric, value)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)


def check_partners():

    hdfs_input_json_filename = '/user/recocomputer/bestofs/imagedl/outputByPartner'
    hdfs_output_json_filename = '/user/o.koch/cnn/outputByPartner'
    input_json_filename = '/home/o.koch/image-search/.input.outputByPartner.graphite'
    output_json_filename = '/home/o.koch/image-search/.output.outputByPartner.graphite'
    timestamp_filename = '/home/o.koch/image-search/lastPartnerTimestamp.txt'
    hdfs_root = '/user/o.koch/cnn/'
    hathi_lastrun_filename = '/home/o.koch/image-search/.hathi.lastrun.timestamp'
    baloo_lastrun_filename = '/home/o.koch/image-search/.baloo.lastrun.timestamp'

    content = ''
    good_to_go = True

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

    # send graphite commands (delay)
    channel = 'delay'
    metric = 'cnn_send'
    value = round(1.0 * baloo_delay / 3600, 1)
    send_jar_command(channel, metric, value)
    metric = 'cnn_recv'
    value = round(1.0 * hathi_delay / 3600, 1)
    send_jar_command(channel, metric, value)

    # send graphite commands (computing rate)
    channel = 'imagedlCnn'
    metric = 'ComputedSize'
    value = round(computed_size_gb, 1)
    send_jar_command(channel, metric, value)
    channel = 'imagedlCnn'
    metric = 'TotalSize'
    value = round(remaining_size_gb + computed_size_gb, 1)
    send_jar_command(channel, metric, value)

    # skipped partners
    channel = 'imagedlCnn'
    metric = 'NbProcessedPartners'
    value = nb_total_partners - nb_skipped_partners
    send_jar_command(channel, metric, value)
    channel = 'imagedlCnn'
    metric = 'NbTotalPartners'
    value = nb_total_partners
    send_jar_command(channel, metric, value)


if __name__ == "__main__":

    with PidFile():

        # ESTOP check
        estop_filename = '/home/o.koch/image-search/ESTOP'
        if os.path.isfile(estop_filename):
            _log('ESTOP ENGAGED.  EXITING.')
            sys.exit(1)

        check_partners()
