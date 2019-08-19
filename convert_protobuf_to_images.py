#!/usr/bin/env python

""" Read protobufs sent from HDFS and converts them to JPG images for further processing. """

import os
import sys
import image_pb2
from google.protobuf.internal import encoder
import varint
import cv2
from cv2 import cv
import numpy as np
import shutil
import argparse
import ConfigParser
import re
import datetime
import multiprocessing
from pid import PidFile
import urllib
import mmh3

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
    msg_string = '%s\t[%s]\t%s' % (dt_string, filename, msg)

    print(msg_string)

    with open('/home/ubuntu/image-search/_%s.log' % filename, 'a') as fp:
        fp.write('%s\n' % msg_string)
        fp.close()

    # log error messages (but ignore HDFS warnings)
    if msg.find('ERROR') is not -1 and msg.find('WARN retry.RetryInvocationHandler') == -1:
        with open('/home/o.koch/image-search/_%s.error.log' % os.path.basename(__file__), 'a') as fp:
            fp.write('%s\n' % msg_string)
            fp.close()


def _take_lock():
    filename = '/home/ubuntu/image-search/.mowgli.lock'
    assert (not os.path.isfile(filename))
    with open(filename, 'w') as fp:
        fp.write('0')


def _is_locked():
    filename = '/home/ubuntu/image-search/.mowgli.lock'
    return os.path.isfile(filename)


def _release_lock():
    filename = '/home/ubuntu/image-search/.mowgli.lock'
    assert (os.path.isfile(filename))
    os.remove(filename)


def hash_external(external_item_id):
    hashed_external = mmh3.hash64(
        recursiveUrlDecode(external_item_id.lower()), 42, True)
    hashed_external = hashed_external[0] ^ hashed_external[1]
    return hashed_external


def process_file(proc_id, output_dir, filename, check):

    with open(filename, 'rb') as fp:
        data = fp.read()
    n_bytes = len(data)

    decoder = varint.decodeVarint32

    next_pos, pos = 0, 0
    while 1:
        #_log('[%02d] processing %s' % (proc_id, filename))
        msg = image_pb2.ProductImage()
        next_pos, pos = decoder(data, pos)
        msg.ParseFromString(data[pos:pos + next_pos])

        if not check:

            # build an opencv image
            nparr = np.fromstring(msg.image, np.uint8)
            img_np = cv2.imdecode(nparr, cv2.CV_LOAD_IMAGE_COLOR)
            #img_ipl = cv.CreateImageHeader((img_np.shape[1], img_np.shape[0]), cv.IPL_DEPTH_8U, 3)
            #cv.SetData(img_ipl, img_np.tostring(), img_np.dtype.itemsize * 3 * img_np.shape[1])

            # write image
#            img_filename = os.path.join(output_dir,'%d__%d.jpg' % (msg.partner_id, msg.internal_id))
            hashed_external = hash_external(msg.external_item_id)

            img_filename = os.path.join(
                output_dir, '%d__%s.jpg' % (msg.partner_id, hashed_external))
            cv2.imwrite(img_filename, img_np)

            #_log ('[%02d] wrote image %s' % (proc_id, img_filename))

        pos += next_pos

        if pos == n_bytes:
            break


def recursiveUrlDecode(externalIdFromImageDl):
    decodedUrl = urllib.unquote(externalIdFromImageDl)
    if decodedUrl != externalIdFromImageDl:
        return recursiveUrlDecode(decodedUrl)
    return decodedUrl


# read data from input directory
def protobuf_to_images(check):

    input_dir = '/opt/input'
    backup_dir = '/opt/old.input'
    #local_timestamp_file = '/home/ubuntu/image-search/.mowgli.timestamp'

    # empty output directory
#    if not check:
#        _log('empty-ing output dir %s' % output_dir)
#        if os.path.isdir(output_dir):
#            shutil.rmtree (output_dir)
#        os.makedirs (output_dir)

    # open first directory to compute
    dirs = os.listdir(input_dir)
    m_input_dir = None
    for mdir in dirs:
        if os.path.isdir(os.path.join(input_dir, mdir)) and os.path.isfile(os.path.join(input_dir, mdir, '.success.baloo')):
            m_input_dir = os.path.join(input_dir, mdir)
            break

    if m_input_dir is None:
        _log('Did not find a new directory to compute. Exiting.')
        return

    input_dir = m_input_dir

    if True:
        output_dir = '/opt/_img'
    else:
        # build output dir
        output_dir = os.path.join('/opt/_img', os.path.basename(input_dir))
        if os.path.isdir(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)

    # sanity check
    assert (os.path.isdir(input_dir))
    assert (os.path.isdir(output_dir))

    _log('Processing input directory %s --> %s' % (input_dir, output_dir))

    # list files
    filenames = []

    for root, dirs, files in os.walk(input_dir):
        for filen in files:
            if not os.path.basename(filen).endswith('.bin'):
                continue
            filename = os.path.join(root, filen)
            filenames.append(filename)

    n_files = len(filenames)

    # process files
    n_proc = 32
    file_lists = [[] for c in range(n_proc)]

    for (filename, filecount) in zip(filenames, range(len(filenames))):
        file_lists[filecount % n_proc].append(filename)

    _log(' '.join(['%s' % len(a) for a in file_lists]))

    jobs = []
    for proc in range(n_proc):
        process = multiprocessing.Process(target=parallel_run, args=[
                                          proc, output_dir, file_lists[proc], check])
        process.start()
        jobs.append(process)

    # wait for all
    for job in jobs:
        job.join()

    # save input dir to old directory
    _log('Moving dir %s to backup directory %s' % (input_dir, backup_dir))
    shutil.move(input_dir, backup_dir)

    _log('Summary : processed %d files --> %s' % (n_files, output_dir))

    # write success file to output dir
    success_file = os.path.join(output_dir, '.success.mowgli')
    with open(success_file, 'w') as fp:
        fp.write('0')


def parallel_run(proc_id, output_dir, file_list, check):
    for filename in file_list:
        process_file(proc_id, output_dir, filename, check)


def main():

    # header for the log file
    _log('======================================================================')
    _log('======================= MOWGLI  RUN =================================')
    _log('======================================================================')

    # read config file
    config = ConfigParser.ConfigParser()
    config.readfp(open('/home/ubuntu/image-search/defaults.ini'))

    # parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', dest='check', action='store_true',
                        help='sanity check only', default=False)
    args = parser.parse_args()

    protobuf_to_images(args.check)


def unit_test():

    mid = "260882700"
    hash_id = hash_external(mid)
    print(hash_id)


if __name__ == "__main__":

    with PidFile():
        main()
