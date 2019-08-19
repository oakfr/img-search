#!/usr/bin/env python

"""
 Aggregates match statistics for image features
 it takes as input a directory containing an arbitrary number of files in the following format:

 file name X.Y.Z.bin where X = partnerid, Y = feature type, Z = feature size
 each file contains a dictionary <internal id, array of internal ids> output by knn.py

 this script outputs the precision per feature type and feature size, assuming the
 largest feature size represents the ground truth
 """

import numpy as np
import glob
import os
import random
import pickle
import time


__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def process_partner(input_dir, partnerid, feature_type):

    files = glob.glob("%s/%d.%s.*.bin" % (input_dir, partnerid, feature_type))

    # find maximum feature size (that will be ground truth)
    max_fsize = 0
    for filename in files:
        d = os.path.basename(filename).split(".")
        feature_size = int(d[2])
        if feature_size > max_fsize:
            max_fsize = feature_size
        print(os.path.basename(filename))

    print(max_fsize)

    # read reference data
    ref_filename = '%s/%d.%s.%d.bin' % (input_dir,
                                        partnerid, feature_type, max_fsize)
    ref_data = pickle.load(open(ref_filename, 'r'))

    # parse all other datasets
    for filename in files:
        print('%s %s' % (ref_filename, filename))
        d = os.path.basename(filename).split(".")
        feature_size = int(d[2])
        if feature_size == max_fsize:
            continue
        data = pickle.load(open(filename, 'r'))
        stat_filename = '%s/%s.%d.txt' % (input_dir,
                                          feature_type, feature_size)
        fp = open(stat_filename, 'a')

        for (k, v) in data.iteritems():
            if k not in ref_data:
                print('warning: internal %d is in target but not in ref, type=%s, size = %d, ref size=%d' % (
                    k, feature_type, feature_size, max_fsize))
                continue
            v_ref = ref_data[k]
            assert(len(v) == len(v_ref))
            inter = np.intersect1d(v, v_ref)
            recall = 1.0 * len(inter) / len(v)
            fp.write('%.6f\n' % recall)
        fp.close()

# aggregate statistics for each <feature type, feature size> pair
#


def process_feature_type(input_dir, feature_type):

    files = glob.glob("%s/%s.*.txt" % (input_dir, feature_type))

    print('Feature type %s (median, mean, std, count)' % feature_type)
    print('============================')

    for filename in files:
        feature_size = int(os.path.basename(filename).split('.')[1])
        fp = open(filename, 'r')
        d = fp.readlines()
        x = np.array([float(y) for y in d])
        fp.close()

        print('%3d\t%2.4f\t%2.4f\t%2.4f\t%10d' %
              (feature_size, np.median(x), np.mean(x), np.std(x), x.size))


# build a dummy dataset
#
def build_dummy():
    output_dir = 'output'
    rrange = range(1000, 1200)
    random.seed(time.time())

    for partner_id in [234, 2342, 443]:
        for feature_type in ['hog', 'cnn']:
            for feature_size in [2, 4, 16, 256, 1024]:
                filename = '%s/%d.%s.%d.bin' % (output_dir,
                                                partner_id, feature_type, feature_size)
                # random dictionary
                dicts = {}
                for internal_id in rrange:
                    v = np.array(random.sample(rrange, 20))
                    dicts[internal_id] = v
                pickle.dump(dicts, open(filename, 'w'))
                print(filename)


def main():

    # skip if pipe is running
    if os.path.isfile('/opt/input/_PIPE_RUNNING'):
        print('Main pipe is running.  Sorry...')
        return

    input_dir = '/home/ubuntu/image-search/matches/matrix/'

    # build dummy dataset
    #build_dummy ()
    # return

    # list partners and types
    partners_and_types = []

    files = glob.glob("%s/*.bin" % input_dir)
    for filename in files:
        d = os.path.basename(filename).split(".")
        partner_id = int(d[0])
        feature_type = d[1]
        partners_and_types.append((partner_id, feature_type))

    partners_and_types = list(set(partners_and_types))

    print(partners_and_types)

    # process partners and types
    for partner_type in partners_and_types:
        process_partner(input_dir, partner_type[0], partner_type[1])

    # aggregate statistics over feature type
    types = list(set([y[1] for y in partners_and_types]))
    for feature_type in types:
        process_feature_type(input_dir, feature_type)


if __name__ == "__main__":
    main()
