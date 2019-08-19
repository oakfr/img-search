import os
import sys
import random
import time
import json
import subprocess
import hadoop_filestatus_utils

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def util_count_lines(filename):
    if not os.path.isfile(filename):
        print('Error : file %s does not exist' % filename)
        return 0
    with open(filename, 'r', encoding="utf8") as f:
        return sum(1 for _ in f)
    return 0


def util_chunkify(lst, n):
    return [lst[i::n] for i in range(n)]


def util_get_partnerid_from_file(filename):
    if sys.version_info[0] > 2:
        fp = open(filename, 'r', encoding="utf8")
    else:
        import codecs
        fp = codecs.open(filename, 'r', encoding='utf-8')
    line = fp.readline()
    d = line.rstrip().split('\t')
    partnerid = int(d[13])
    return partnerid


def util_file_size(filename):
    assert (os.path.isfile(filename))
    statinfo = os.stat(filename)
    return statinfo.st_size


def util_run_cmd(cmd):
    cmd = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cmd_out, cmd_err = cmd.communicate()
    return_code = cmd.returncode
    return (cmd_out.strip(), cmd_err.strip(), return_code)


def util_feature_filename(data_dir, feature_file_basename, partnerid, feature_type, hog_resize, file_id):
    if feature_type == 'hog':
        return os.path.join(data_dir, 'features', '%s-%d-%s-%d-%d.bin' % (feature_file_basename, partnerid, feature_type, hog_resize, file_id))
    else:
        return os.path.join(data_dir, 'features', '%s-%d-%s-%d.bin' % (feature_file_basename, partnerid, feature_type, file_id))


def util_split_file(input_file, n_files, sampling_ratio):

    if not os.path.isfile(input_file):
        print('*** Warning***  Input file %s does not exist.  Returning None.' % input_file)
        return None

    # open files
    basename = os.path.splitext(os.path.basename(input_file))[0]

    out_files = ['.temp_%s_%04d.txt' % (basename, k) for k in range(n_files)]

    fps = [open(out_file, 'w') for out_file in out_files]
    gp = open(input_file, 'r')

    # round-robin over output files
    curr_file = 0
    line_count = [0] * n_files

    for line in gp.readlines():
        if random.random() > sampling_ratio:
            continue
        fps[curr_file].write(line)
        line_count[curr_file] += 1
        curr_file = (curr_file+1) % n_files

    print(line_count)

    # close files
    gp.close()
    for fp in fps:
        fp.close()

    return out_files


def util_send_email(dest, title, textfile, nlines):

    tmp_file = '/tmp/email_%d.txt' % int(time.time())

    #cmd = 'tac %s | head -%d > %s; mail -s \"%s\" %s < %s' % (textfile, nlines, tmp_file, title, dest, tmp_file)
    cmd = 'mail -s \"%s\" %s < %s' % (title, dest, textfile)
    print(cmd)
    cmd_out, cmd_err, rc = util_run_cmd(cmd)
    if rc != 0:
        print('*** ERROR *** %s' % cmd_err)

    #os.remove (tmp_file)


def util_hdfs_ls(hdfsPath):
    HDFS_CMD_PREFIX = 'hadoop fs -'
    cmdOutput = subprocess.Popen(
        [HDFS_CMD_PREFIX + "ls " + hdfsPath], shell=True, stdout=subprocess.PIPE).stdout.readlines()
    result = parseHdfsLsOutput(cmdOutput)
    return result


def parseHdfsLsOutput(cmdOutput):
    result = []
    for line in cmdOutput:
        line = line.strip()
        splittedLine = filter(None, line.split(' '))
        if len(splittedLine) != 8:
            continue
        result.append(hadoop_filestatus_utils.FileStatus(*splittedLine))
    return result


def util_timestamp_file_to_json(timestamp_file, json_file):

    assert (os.path.isfile(timestamp_file))

    data = {}

    total_size_bytes = 0

    with open(timestamp_file, 'r') as fp:
        lines = fp.readlines()
        for l in lines:
            l = l.strip().split(' ')
            partnerid = int(l[0])
            timestamp = int(l[1])
            key = '%d' % partnerid
            data[key] = {}
            data[key]['files'] = ['cnn_features.txt.gz']
            data[key]['jobTimeStamp'] = timestamp
            data[key]['outputFolder'] = '%d/%d' % (timestamp, partnerid)

            hdfs_file = '/user/o.koch/cnn/%d/%d/cnn_features.txt.gz' % (
                timestamp, partnerid)
            filestatus = util_hdfs_ls(hdfs_file)

            data[key]['outputSize'] = filestatus[0].filesize

            total_size_bytes += filestatus[0].filesize
            #print('util done with partnerid %d (size %d)' % (partnerid, filestatus[0].filesize))

    json_data = json.dumps(data)
    with open(json_file, 'w') as fp:
        json.dump(data, fp, sort_keys=True, indent=2)
        fp.close()

    return total_size_bytes


def util_get_platform():
    if os.environ['HOSTNAME'] == 'e4-1d-2d-1d-00-a0':
        return 'pa4'
    elif os.environ['HOSTNAME'] == '6c-3b-e5-a8-71-c0':
        return 'am5'
    return None


if __name__ == "__main__":

    #    util_send_email('o.koch@criteo.com', '[aws-tiger][baloo]', '_baloo.py.log', 100)

    util_timestamp_file_to_json(
        '/home/o.koch/image-search/lastPartnerTimestamp_computed.txt')
