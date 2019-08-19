#!/usr/bin/env python
"""
 This script computes the top-N matches for a given partner

 usage : knn.py --input /opt/output-per-partner/cnn_partner_1232.txt
"""
import pickle
import ConfigParser
import os
import sys
import gzip
from itertools import product, combinations
import numpy as np
import multiprocessing
import time
import cv2
import random
import argparse
import glob
import math
from util import util_get_partnerid_from_file, util_feature_filename, util_split_file

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def main():

    # parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input_file', type=str, required=True)
    parser.add_argument('--shrink', dest='shrink',
                        type=int, required=False, default=-1)
    args = parser.parse_args()

    # only accept gzip files
    assert (os.path.basename(args.input_file).endswith('.gz'))

    # unzip file
    print('unzipping %s...' % args.input_file)
    temp_filename = '/tmp/tmp.features.txt'
    with gzip.open(args.input_file, 'rb') as input:
        with open(temp_filename, 'w') as output:
            output.write(input.read())

    print('Splitting into files...')
    files = util_split_file(temp_filename, 32, 1)
    print('Done.')

    # main run
    run(files, args.shrink)

    # remove files
    for file in files:
        os.remove(file)


def run(files, shrink):

    nx = 1
    nf = len(files)

    # compute matches by pairs
    pairs = [(i, j) for i in range(nx) for j in range(nf)]

    n_pairs = len(pairs)
    n_proc = 32
    topn = 10
    sampling_ratio = 1

    n_batches = int(math.ceil(1.0 * n_pairs / n_proc))

    print('%d files, %d pairs, %d batches' % (nf, n_pairs, n_batches))

    matches = {}

    k = 0
    for b in range(n_batches):

        jobs = []
        manager = multiprocessing.Manager()
        return_dict = manager.dict()

        for c in range(32):

            if k >= len(pairs):
                break

            # compute matches
            ii = pairs[k][0]
            ij = pairs[k][1]
            process = multiprocessing.Process(target=compute_matches, args=[
                                              ii, ij, files[ii], files[ij], topn, shrink, return_dict])
            process.start()
            jobs.append(process)

            k += 1

        for job in jobs:
            job.join()

        # store data
        for (key, val) in return_dict.items():
            matches[key] = val.copy()

    topn_matrix = build_topn(matches, files, nx, nf, topn, sampling_ratio)


def read_features_from_file(filename, shrink):

    print('reading features from %s...' % filename)
    feats = []
    partner_ids = []
    internal_ids = []
    k = 0

    with open(filename, 'r') as fp:
        for line in fp.readlines():
            data = line.strip().split(' ')
            partner_id = data[0]
            internal_id = data[1]
            feat = np.array([float(y)/1000000 for y in data[2:]])

            if shrink > 0:
                feat = shrink_vector(feat, shrink)

            feats.append(feat)
            partner_ids.append(partner_id)
            internal_ids.append(internal_id)
            k += 1
            # if k>20:
            #    break

    return (feats, partner_ids, internal_ids)


def dist(a, b):
    assert(len(a) == len(b))
    return np.sum(np.abs(a-b)) / len(a)


def compute_matches(id1, id2, file1, file2, topn, shrink, return_dict):

    print('computing matches for (%d,%d)...' % (id1, id2))

    # read data
    (feats_1, partner_ids_1, internal_ids_1) = read_features_from_file(file1, shrink)
    (feats_2, partner_ids_2, internal_ids_2) = read_features_from_file(file2, shrink)

    n_features_1 = len(feats_1)
    n_features_2 = len(feats_2)

    # brute-force l1 distance
    dx = np.zeros((n_features_1, topn, 6))

    print('[%d,%d] L1 matching...' % (id1, id2))

    for i in range(n_features_1):

        dd = np.array([dist(feats_1[i], feats_2[j])
                       for j in range(n_features_2)])
        dp2 = np.array([int(x) for x in partner_ids_2])
        di2 = np.array([int(x) for x in internal_ids_2])

        ds = np.argsort(dd)[:topn]

        dx[i, :, 0] = ds      # indices
        dx[i, :, 1] = dd[ds]  # distances
        dx[i, :, 2] = int(partner_ids_1[i])
        dx[i, :, 3] = int(internal_ids_1[i])
        dx[i, :, 4] = dp2[ds]
        dx[i, :, 5] = di2[ds]

    print('computing matches for (%d,%d)... done!' % (id1, id2))
    return_dict[(id1, id2)] = dx


# build image filename from feature filename
def get_image_name_from_ids(partner_id, internal_id):
    return '/opt/_img/%d__%d.jpg' % (partner_id, internal_id)


def build_topn(matches, feature_files, nx, nf, topn, sampling_ratio):

    topn_matrix = {}

    for id1 in range(nx):

        #    print('----------------------------')
        #    print(id1)
        #    print(nf)
        #    print(matches[(0,0)])
        #    print(matches[(0,0)][:,:,0])
        #    dumm=[matches[(id1,x)][:,:,0] for x in np.arange(nf)]
        inds = np.hstack(tuple([matches[(id1, x)][:, :, 0]
                                for x in np.arange(nf)]))
        dist = np.hstack(tuple([matches[(id1, x)][:, :, 1]
                                for x in np.arange(nf)]))
        dp1s = np.hstack(tuple([matches[(id1, x)][:, :, 2]
                                for x in np.arange(nf)]))
        di1s = np.hstack(tuple([matches[(id1, x)][:, :, 3]
                                for x in np.arange(nf)]))
        dp2s = np.hstack(tuple([matches[(id1, x)][:, :, 4]
                                for x in np.arange(nf)]))
        di2s = np.hstack(tuple([matches[(id1, x)][:, :, 5]
                                for x in np.arange(nf)]))

        indx = np.argsort(dist, axis=1)[:, :topn]

        for i in range(indx.shape[0]):

            skip_it = random.random() > sampling_ratio:

            out_img = None

            for j in range(indx.shape[1]):

                k = indx[i, j]
                partner_id_1 = dp1s[i, k]
                internal_id_1 = di1s[i, k]
                partner_id_2 = dp2s[i, k]
                internal_id_2 = di2s[i, k]

                assert (partner_id_1 == partner_id_2)

                if internal_id_1 not in topn_matrix:
                    topn_matrix[internal_id_1] = [internal_id_2]
                else:
                    topn_matrix[internal_id_1].append(internal_id_2)

                #ids = int(inds[i,k])

                # write image
                if skip_it:
                    continue

                img1_name = get_image_name_from_ids(
                    partner_id_1, internal_id_1)
                img2_name = get_image_name_from_ids(
                    partner_id_2, internal_id_2)

                img1 = cv2.resize(cv2.imread(img1_name), (256, 256))
                img2 = cv2.resize(cv2.imread(img2_name), (256, 256))

                if out_img is None:
                    out_img = img1

                out_img = np.concatenate((out_img, img2), axis=1)

            if out_img is not None:
                out_filename = '/home/ubuntu/image-search/matches/%d_%d.jpg' % (
                    partner_id_1, internal_id_1)
                cv2.imwrite(out_filename, out_img)

    return topn_matrix


def shrink_vector(h, newlen):

    # resample
    lenh = len(h)
    if newlen != lenh:
        K = np.zeros((newlen, 1))
        r = math.floor(1.0*lenh/newlen)
        for k in range(newlen):
            K[k] = np.mean(h[k*r:(k+1)*r])
        newh = K

        # normalize
        hl = np.linalg.norm(newh)
        if hl > 1E-8:
            newh = newh / hl
        return newh

    return h


if __name__ == "__main__":

    main()
