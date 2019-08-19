#!/usr/bin/env python

""" Aggregates features per partner, for further retrieval from HDFS. """

import argparse
import datetime
import glob
import os
import sys
import shutil
import time
import numpy as np
import multiprocessing
from shutil import copyfileobj
from pid import PidFile
#import bz2
import gzip
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Agg')

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
        with open('/home/ubuntu/image-search/_%s.error.log' % os.path.basename(__file__), 'a') as fp:
            fp.write('%s\n' % msg_string)
            fp.close()


def _take_lock():
    filename = '/home/ubuntu/image-search/.akela.lock'
    assert (not os.path.isfile(filename))
    with open(filename, 'w') as fp:
        fp.write('0')


def _is_locked():
    filename = '/home/ubuntu/image-search/.akela.lock'
    return os.path.isfile(filename)


def _release_lock():
    filename = '/home/ubuntu/image-search/.akela.lock'
    assert (os.path.isfile(filename))
    os.remove(filename)


def compress_files(files):

    for c in files:
        a = c[0]
        b = c[1]
        _log('compressing file %s --> %s...' % (a, b))

        with open(a, 'r') as input:
            with gzip.open(b, 'wb') as output:
                output.write(input.read())

        # remove uncompressed file
        os.remove(a)


def list_partners(files):

    partner_ids = []

    for file in files:

        _log('processing file %s' % file)

        with open(file, 'r') as fp:

            for line in fp.readlines():

                d = line.strip().split(' ')
                partner_id = int(d[1])

                partner_ids.append(partner_id)

    return list(set(partner_ids))

# to_temp is used to dump output in a directory that is not watched by the gateway
#


def split_per_partners(dt_string, partner_ids, feature_filenames, do_stats, to_temp):

    if to_temp:
        success_file = '/opt/tmp.output-per-partner/.success'
    else:
        success_file = '/opt/output-per-partner/.success'

    # remove success file
    if os.path.isfile(success_file):
        os.remove(success_file)

    # get timestamp
#    dt_string = '%d' % int(time.time())
    if to_temp:
        output_dir = '/opt/tmp.output-per-partner/%s' % dt_string
    else:
        output_dir = '/opt/output-per-partner/%s' % dt_string

    # remove all output files
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # open files
    fps = {}
    for partner_id in partner_ids:
        filename = '%s/cnn_features_partner_%d.txt' % (output_dir, partner_id)
        fps[partner_id] = open(filename, 'w')
        _log('opened file for partner id %d' % partner_id)

    if do_stats:
        # average non-zero values
        nbins = 1000
        nonzero_values = []
        feat_hist = np.zeros(4096)     # histogram of non-zero indices
        feat_val_hist = np.zeros(nbins)  # histogram of vector values
        min_val = 1E8
        max_val = -1E8

    # for debugging - set to -1 to disable
    max_files_to_process = -1
    n_greater_int16 = 0
    n_greater_int16_count = 0

    # splitting data
    for (file, filecount) in zip(feature_filenames, range(len(feature_filenames))):

        _log('processing file %s (%d out of %d)' %
             (file, filecount+1, len(feature_filenames)))

        with open(file, 'r') as op:
            for line in op.readlines():

                d = line.strip().split(' ')

                # the first integer is used by bagheera and can be discarded
                partner_id = int(d[1])
                internal_id = int(d[2])

                if partner_id not in fps:
                    _log('***ERROR*** partnerid %d not listed.' % partner_id)

                assert (partner_id in fps)

                # fps[partner_id].write(line)
                vals = [float(y) for y in d[3:]]

                feat = np.array(vals)

                if np.any(np.isnan(feat)):
                    _log('*** ERROR *** nan values encountered in file %s' % file)
                    continue

                nzf = np.where(feat > .00000001)[0]
                if nzf.size > 0:
                    n_greater_int16 += 1.0 * \
                        np.sum(feat[nzf] > 65536.0/1000000) / nzf.size
                    n_greater_int16_count += 1

                # write data to file (if not doing stats)
                if not do_stats:
                    fps[partner_id].write('%d %d ' % (partner_id, internal_id))

                    for k in vals:
                        fps[partner_id].write('%d ' % int(round(k*1000000)))

                    fps[partner_id].write('\n')

                if do_stats:
                    # feature vector
                    nonzero_values.append(100.0*nzf.size/feat.size)
                    feat_hist[nzf] += 1
                    for k in np.around(feat[nzf]*nbins).astype(int):
                        feat_val_hist[k] += 1
                    #feat_val_hist[np.around(feat[nzf]*nbins).astype(int)] += 1
                    min_val = min([min_val, np.amin(feat[nzf])])
                    max_val = max([max_val, np.amax(feat[nzf])])

        if max_files_to_process != -1 and filecount+1 == max_files_to_process:
            break

    # close files
    for fp in fps.values():
        fp.close()

    # compress files (parallelized)
    n_proc = 32
    files_to_compress = [[] for c in range(n_proc)]
    for (partner_id, count) in zip(partner_ids, range(len(partner_ids))):
        filename = '%s/cnn_features_partner_%d.txt' % (output_dir, partner_id)
        os.makedirs('%s/%d' % (output_dir, partner_id))
        zip_filename = '%s/%d/cnn_features.txt.gz' % (output_dir, partner_id)
        files_to_compress[count % n_proc].append((filename, zip_filename))

    jobs = []

    for c in range(n_proc):

        process = multiprocessing.Process(
            target=compress_files, args=[files_to_compress[c]])
        process.start()
        jobs.append(process)

    for proc in jobs:
        proc.join()

    # write success file
    success_file = os.path.join(output_dir, '.success.akela')
    with open(success_file, 'w') as fp:
        fp.write('0')

    _log('Ratio of values over 2^16 : %.2f %%' %
         (100.0 * n_greater_int16 / n_greater_int16_count))

    if do_stats:
        # average non-zero values
        nonzero_values = np.array(nonzero_values)
        avg_nonzero_values = np.mean(nonzero_values)
        med_nonzero_values = np.median(nonzero_values)
        max_nonzero_values = np.amax(nonzero_values)
        _80p_nonzero_values = np.percentile(nonzero_values, 80)
        _90p_nonzero_values = np.percentile(nonzero_values, 90)

        # plot histogram
        fig = plt.figure()
        print('%d %d' % (len(feat_val_hist), nbins))
        plt.bar(np.arange(len(feat_val_hist)), feat_val_hist, color='green')
        plt.savefig('feature_distribution-all.png')
        fig = plt.figure()
        n_cap = round(1.0*nbins/5)
        plt.bar((np.arange(len(feat_val_hist)))[
                :n_cap], feat_val_hist[:n_cap], color='green')
        plt.savefig('feature_distribution-top20pc.png')
        # print(feat_val_hist)

        _log('Min value : %.6f   Max value : %.6f' % (min_val, max_val))

        _log('Non-zero values : [mean] %.4f  [median] %.4f  [max] %.4f  [80%%] %.6f  [90%%] %.6f'
             % (avg_nonzero_values, med_nonzero_values, max_nonzero_values, _80p_nonzero_values, _90p_nonzero_values))

        for k in [10, 20, 30, 40]:
            x = np.percentile(feat_hist, k)
            xc = np.sum(feat_hist > x)
            _log('%.2f %% of values over %d-%% percentile' %
                 (100.0*xc/feat_hist.size, k))


def main():

    # parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--stats', dest='do_stats', action='store_true',
                        help='run stats over feature vectors', default=False)
    parser.add_argument('--temp', dest='to_temp', action='store_true',
                        help='store data in a temporary output dir', default=False)
    args = parser.parse_args()

    # find input_dir
    input_dir = '/opt/output'
    dirs = os.listdir(input_dir)
    m_input_dir = None
    for mdir in dirs:
        if os.path.isdir(os.path.join(input_dir, mdir)) and os.path.isfile(os.path.join(input_dir, mdir, '.success.bagheera')):
            m_input_dir = os.path.join(input_dir, mdir)
            break

    input_dir = m_input_dir

    if input_dir is None:
        _log('*** ERROR *** did not find a proper directory to process.')
        return

    files = glob.glob('%s/*.txt' % input_dir)

    dt_string = os.path.basename(input_dir)

    if len(files) == 0:
        _log('No files to process.  Exiting')
        sys.exit(1)

    _log('listing partners...')

    partners = list_partners(files)

    _log('%d partners.' % len(partners))

    _log('splitting per partner...')

    split_per_partners(dt_string, partners, files, args.do_stats, args.to_temp)

    _log('Removing input directory %s' % input_dir)
    assert (os.path.isdir(input_dir))
    shutil.rmtree(input_dir)

    _log('Summary : processed %d partners.' % len(partners))


if __name__ == "__main__":

    # header for the log file
    _log('======================================================================')
    _log('======================= AKELA   RUN =================================')
    _log('======================================================================')

    with PidFile():
        main()
