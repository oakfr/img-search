#!/usr/bin/env python

""" Downloads images from HDFS and ships them to AWS for processing. """


import subprocess
import os
import sys
import json
import datetime
import time
from util import util_send_email, util_run_cmd, util_get_platform
import multiprocessing
import shutil
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
    filename = '/home/o.koch/image-search/.baloo.lock'
    assert (not os.path.isfile(filename))
    with open(filename, 'w') as fp:
        fp.write('0')


def _is_locked():
    filename = '/home/o.koch/image-search/.baloo.lock'
    return os.path.isfile(filename)


def _release_lock():
    filename = '/home/o.koch/image-search/.baloo.lock'
    assert (os.path.isfile(filename))
    os.remove(filename)


def run_commands(proc_id, cmd_list, dry_run, return_dict):
    n_cmds = len(cmd_list)
    return_dict[proc_id] = 0

    for (cmd, cmd_count) in zip(cmd_list, range(n_cmds)):

        progress_pc = 100.0 * cmd_count / n_cmds
        _log('[proc %d] [%d/%d = %2.2f%%] %s' %
             (proc_id, cmd_count, n_cmds, progress_pc, cmd[0]))
        _log('[proc %d] [%d/%d = %2.2f%%] %s' %
             (proc_id, cmd_count, n_cmds, progress_pc, cmd[1]))
        partner_id = cmd[2]
        if not dry_run:
            # this is a copy from HDFS
            cmd_out, cmd_err, rc = util_run_cmd(cmd[0])
            if rc != 0:
                _log('*** ERROR *** %s' % cmd_err)
                return_dict[proc_id] = -1
                break
            # this is a copy to AWS
            cmd_out, cmd_err, rc = util_run_cmd(cmd[1])
            if rc != 0:
                _log('*** ERROR *** %s' % cmd_err)
                return_dict[proc_id] = -1
                break


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def aws_dir_exists(path):
    proc = subprocess.Popen(['ssh', host, 'test -d %s' % pipes.quote(path)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    return proc.returncode == 0


def main():

    dry_run = True
    dry_run = False

    # header for the log file
    _log('======================================================================')

    remote_root = '/opt/input'
    json_filename = '/home/o.koch/image-search/outputByPartner'
    timestamp_filename = '/home/o.koch/image-search/lastPartnerTimestamp.txt'
    gpu_ip = '54.73.223.224'
    root_dir = '/user/recocomputer/bestofs/imagedl/'
    local_dir = '/home/o.koch/image-search/input'
    job_timestamp_filename = '/home/o.koch/image-search/.baloo.lastrun.timestamp'

    # build timestamp
    # timestamp is odd on PA4 and even on AM5
    ref_time = int(time.time())
    if util_get_platform() == 'pa4':
        if ref_time % 2 == 0:
            ref_time += 1
    elif util_get_platform() == 'am5':
        if ref_time % 2 == 1:
            ref_time += 1
    else:
        _log('***ERROR*** Unrecognized platform %s' % util_get_platform())
        assert(False)

    dt_string = '%d' % ref_time
    remote_dir = '%s/%s' % (remote_root, dt_string)

    # remove old JSON file
    if os.path.isfile(json_filename):
        os.remove(json_filename)

    # clean up local directory
    if not dry_run:
        if os.path.isdir(local_dir):
            _log('Removing %s...' % local_dir)
            shutil.rmtree(local_dir)
            _log('done.')
        os.makedirs(local_dir)

    # remove local file
    if os.path.isfile(json_filename):
        os.remove(json_filename)

    # fetch outputByPartner on HDFS
    # leave if file is not on HDFS
    cmd = 'hadoop fs -get %s/outputByPartner %s' % (root_dir, json_filename)
    _log(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        _log('*** ERROR *** %s' % cmd_err)
        return

    # parse JSON data
    with open(json_filename, 'r') as fp:
        json_data = fp.read()
        try:
            data = json.loads(json_data)
        except:
            _log('*** ERROR *** Failed to read JSON file %s.  File might be empty.  Exiting.' % json_filename)
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

    # sort partners by age
    partners_by_age = []
    for item in data:
        partner_id = int(item)
        partner_timestamp = int(data[item]['jobTimeStamp'])
        item_age = partner_timestamp
        if partner_id in last_partnersTimestamp:
            item_age -= last_partnersTimestamp[partner_id]
        else:
            print('partner %s not found' % item)
        partners_by_age.append((item, item_age))

    partners_by_age.sort(key=lambda x: x[1], reverse=True)

    n_transferred_files = 0
    n_transferred_partners = 0
    n_proc = 2
    n_files_limit = 200

    cmd_list = []

    # parse json file
    for item in [x[0] for x in partners_by_age]:
        partner_id = int(item)
        partner_timestamp = int(data[item]['jobTimeStamp'])

        if partner_id != 13045:
            continue

        # cap number of files to transfer
        if n_transferred_files > n_files_limit:
            _log('*** Reached file limit (%d files) ****  Partner ID %d and the following ones will be skipped.' %
                 (n_transferred_files, partner_id))
            break

        if partner_id in last_partnersTimestamp and last_partnersTimestamp[partner_id] >= partner_timestamp:
            # _log('Skipping partner %d.  No new data to process. Current timestamp : %d.  Last timestamp :%d.  Current - last = %d.' % (partner_id, \
            #        partner_timestamp, last_partnersTimestamp[partner_id], partner_timestamp - last_partnersTimestamp[partner_id]))
            continue

        last_partnersTimestamp[partner_id] = partner_timestamp
        n_transferred_partners += 1
        _log('Processing partner %d with timestamp %d' %
             (partner_id, partner_timestamp))

        # get file
        output_folder = data[item]['outputFolder']
        files = data[item]['files']
        for file in files:
            target = os.path.join(root_dir, output_folder, file)
            local_file = os.path.join(
                local_dir, '%d-%s.bin' % (partner_id, file))

            # copy from HDFS
            cmd_1 = 'hadoop fs -get %s %s' % (target, local_file)

            # send to AWS
            cmd_2 = 'scp %s ubuntu@%s:%s' % (local_file, gpu_ip, remote_dir)

            cmd_list.append((cmd_1, cmd_2, partner_id))

            n_transferred_files += 1

    # stop here if nothing to do
    if n_transferred_files == 0:
        _log('No files were planned for transfer.  Stopping here.')
        return

    # create remote dir on AWS (for first file only)
    cmd = 'ssh ubuntu@%s \'mkdir %s\'' % (gpu_ip, remote_dir)
    _log(cmd)
    if not dry_run:
        cmd_out, cmd_err, rc = util_run_cmd(cmd)
        if rc != 0:
            _log('*** ERROR *** %s' % cmd_err)
            return

    # split commands among processes
    cmd_lists = [[] for c in range(n_proc)]
    for (cmd, c) in zip(cmd_list, range(len(cmd_list))):
        cmd_lists[c % n_proc].append(cmd)

    # run commands
    manager = multiprocessing.Manager()
    return_dict = manager.dict()

    jobs = []
    c = 0
    for cmd_list in cmd_lists:
        process = multiprocessing.Process(target=run_commands, args=[
                                          c, cmd_list, dry_run, return_dict])
        process.start()
        jobs.append(process)
        c += 1

    # wait for jobs to finish
    for job in jobs:
        job.join()

    # if any of the job failed, exit
    for k in return_dict.values():
        if k != 0:
            _log('*** ERROR *** One of the baloo children failed.  Exiting.')
            # remove local data
            assert (os.path.isdir(local_dir))
            _log('Removing %s...' % local_dir)
            shutil.rmtree(local_dir)
            _log('done.')
            return

    # write timestamps to file
    if not dry_run:
        fp = open(timestamp_filename, 'w')
        for k, v in last_partnersTimestamp.iteritems():
            fp.write('%d %d\n' % (k, v))
        fp.close()

    # create sucess file on AWS
    local_success_file = os.path.join(local_dir, '.success.baloo')
    with open(local_success_file, 'w') as gp:
        gp.write('%s' % dt_string)
    cmd = 'scp %s ubuntu@%s:%s' % (local_success_file, gpu_ip, remote_dir)
    _log(cmd)
    if not dry_run:
        cmd_out, cmd_err, rc = util_run_cmd(cmd)
        if rc != 0:
            _log('*** ERROR *** %s' % cmd_err)

    # remove local dir
    if not dry_run:
        assert (os.path.isdir(local_dir))
        _log('Removing %s...' % local_dir)
        shutil.rmtree(local_dir)
        _log('done.')

    # write job timestamp to file
    with open(job_timestamp_filename, 'w') as gp:
        gp.write('%d' % int(time.time()))
        gp.close()

    _log('Summary : transferred %d files (%d partners) to AWS' %
         (n_transferred_files, n_transferred_partners))


if __name__ == "__main__":

    # header for the log file
    _log('======================================================================')

    # if _is_locked ():
    #    _log ('Baloo instance already running.  Exiting...')
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
        with open('/home/o.koch/image-search/.baloo.timestamp', 'w') as fp:
            fp.write('%d' % int(time.time()))
            fp.close()

    # _release_lock()
