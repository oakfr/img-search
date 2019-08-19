import os
import glob
import shutil
import cv2
import numpy as np

""" Stack output of a knn search into a single output. """

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def stack_by_feature_type():

    input_dir = '/home/ubuntu/image-search/matches'

    # list images
    img_list = glob.glob(os.path.join(input_dir, '*.jpg'))

    # reset output dir
    output_dir = os.path.join(input_dir, 'stack')

    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # list unique pairs <partnerid,internalid>
    # image name format is partnerid_internalid_featuretype_featuresize.jpg
    #
    partner_internals = []
    for img in img_list:
        d = os.path.basename(img).split('_')
        partnerid = int(d[0])
        internalid = int(d[1])
        partner_internals.append((partnerid, internalid))
    partner_internals = list(set(partner_internals))

    print('Found %d pairs <partner id, internal id>' % len(partner_internals))

    # find matches available for each feature type
    for partner_internal in partner_internals:

        partnerid = partner_internal[0]
        internalid = partner_internal[1]

        img_hog_names = glob.glob(os.path.join(
            input_dir, '%d_%d_hog_*.jpg' % (partnerid, internalid)))
        img_cnn_names = glob.glob(os.path.join(
            input_dir, '%d_%d_cnn_*.jpg' % (partnerid, internalid)))
        img_ran_names = glob.glob(os.path.join(
            input_dir, '%d_%d_ran_*.jpg' % (partnerid, internalid)))

        if (len(img_hog_names) == 0) or (len(img_cnn_names) == 0) or (len(img_ran_names) == 0):
            continue

        hog_size_list = []
        cnn_size_list = []
        ran_size_list = []

        for img_hog_name in img_hog_names:
            hog_size = os.path.splitext(os.path.basename(img_hog_name))[
                0].split('_')[3]
            hog_size_list.append(hog_size)
        for img_cnn_name in img_cnn_names:
            cnn_size = os.path.splitext(os.path.basename(img_cnn_name))[
                0].split('_')[3]
            cnn_size_list.append(cnn_size)
        for img_ran_name in img_ran_names:
            ran_size = os.path.splitext(os.path.basename(img_ran_name))[
                0].split('_')[3]
            ran_size_list.append(ran_size)
        for hog_size in hog_size_list:
            if (hog_size in cnn_size_list) and (hog_size in ran_size_list):
                img_hog_name = os.path.join(
                    input_dir, '%d_%d_hog_%s.jpg' % (partnerid, internalid, hog_size))
                img_cnn_name = os.path.join(
                    input_dir, '%d_%d_cnn_%s.jpg' % (partnerid, internalid, hog_size))
                img_ran_name = os.path.join(
                    input_dir, '%d_%d_ran_%s.jpg' % (partnerid, internalid, hog_size))
            elif (hog_size == '9216' and '4096' in cnn_size_list):
                img_hog_name = os.path.join(
                    input_dir, '%d_%d_hog_%s.jpg' % (partnerid, internalid, '9216'))
                img_cnn_name = os.path.join(
                    input_dir, '%d_%d_cnn_%s.jpg' % (partnerid, internalid, '4096'))
                img_ran_name = os.path.join(
                    input_dir, '%d_%d_ran_%s.jpg' % (partnerid, internalid, '4096'))
            else:
                continue

            print('processing %d,%d size %s' %
                  (partnerid, internalid, hog_size))

            assert (os.path.isfile(img_hog_name))
            assert (os.path.isfile(img_cnn_name))
            assert (os.path.isfile(img_ran_name))

            img_hog = cv2.imread(img_hog_name)
            img_cnn = cv2.imread(img_cnn_name)
            img_ran = cv2.imread(img_ran_name)

            out_img = np.concatenate((img_hog, img_cnn, img_ran), axis=0)

            out_filename = os.path.join(
                output_dir, '%d_%d_hog_cnn_ran_%s.jpg' % (partnerid, internalid, hog_size))
            print('generating %s' % out_filename)

            cv2.imwrite(out_filename, out_img)


def stack_by_feature_size():

    input_dir = '/home/ubuntu/image-search/matches'

    # list images
    img_list = glob.glob(os.path.join(input_dir, '*.jpg'))

    # reset output dir
    output_dir = os.path.join(input_dir, 'stack')

    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # list unique pairs <partnerid,internalid>
    # image name format is partnerid_internalid_featuretype_featuresize.jpg
    #
    partner_internals = []
    for img in img_list:
        d = os.path.basename(img).split('_')
        partnerid = int(d[0])
        internalid = int(d[1])
        partner_internals.append((partnerid, internalid))
    partner_internals = list(set(partner_internals))

    print('Found %d pairs <partner id, internal id>' % len(partner_internals))

    # find matches available for each feature type
    for partner_internal in partner_internals:

        partnerid = partner_internal[0]
        internalid = partner_internal[1]

        img_hog_names = glob.glob(os.path.join(
            input_dir, '%d_%d_hog_*.jpg' % (partnerid, internalid)))
        img_cnn_names = glob.glob(os.path.join(
            input_dir, '%d_%d_cnn_*.jpg' % (partnerid, internalid)))

        if (len(img_hog_names) == 0) or (len(img_cnn_names) == 0):
            continue

        hog_size_list = []
        cnn_size_list = []

        for img_hog_name in img_hog_names:
            hog_size = os.path.splitext(os.path.basename(img_hog_name))[
                0].split('_')[3]
            hog_size_list.append(int(hog_size))
        for img_cnn_name in img_cnn_names:
            cnn_size = os.path.splitext(os.path.basename(img_cnn_name))[
                0].split('_')[3]
            cnn_size_list.append(int(cnn_size))

        # stack HOG images
        out_img = None
        for hog_size in sorted(hog_size_list, reverse=False):

            img_hog_name = os.path.join(
                input_dir, '%d_%d_hog_%04d.jpg' % (partnerid, internalid, hog_size))

            print('processing %d,%d size %s' %
                  (partnerid, internalid, hog_size))

            assert (os.path.isfile(img_hog_name))

            img_hog = cv2.imread(img_hog_name)

            if out_img is None:
                out_img = img_hog
            else:
                out_img = np.concatenate((out_img, img_hog), axis=0)

        out_filename = os.path.join(
            output_dir, '%d_%d_hog_by_size.jpg' % (partnerid, internalid))

        print('generating %s' % out_filename)

        cv2.imwrite(out_filename, out_img)

        # stack CNN images
        out_img = None
        for cnn_size in sorted(cnn_size_list, reverse=False):

            img_cnn_name = os.path.join(
                input_dir, '%d_%d_cnn_%04d.jpg' % (partnerid, internalid, cnn_size))

            print('processing %d,%d size %s' %
                  (partnerid, internalid, cnn_size))

            assert (os.path.isfile(img_cnn_name))

            img_cnn = cv2.imread(img_cnn_name)

            if out_img is None:
                out_img = img_cnn
            else:
                out_img = np.concatenate((out_img, img_cnn), axis=0)

        out_filename = os.path.join(
            output_dir, '%d_%d_cnn_by_size.jpg' % (partnerid, internalid))

        print('generating %s' % out_filename)

        cv2.imwrite(out_filename, out_img)


if __name__ == "__main__":

    #    stack_by_feature_type ()

    stack_by_feature_size()
