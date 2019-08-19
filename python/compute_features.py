#!/usr/bin/env python

""" Computes image features.  Uses overfeat by default (slow).  Use --caffe to make it use Caffe on GPU. """


import os
import sys
import subprocess
import progressbar as pb
import argparse
import json
import numpy as np
import pickle
from util import util_feature_filename, util_get_partnerid_from_file
import ConfigParser
import math
import glob
import shutil
import random
import multiprocessing
import time
import datetime
import re
import cv2
from pid import PidFile
sys.path.insert(0, '/home/ubuntu/caffe/python')
import caffe
import matplotlib
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
        with open('/home/o.koch/image-search/_%s.error.log' % os.path.basename(__file__), 'a') as fp:
            fp.write('%s\n' % msg_string)
            fp.close()


def _take_lock():
    filename = '/home/ubuntu/image-search/.bagheera.lock'
    assert (not os.path.isfile(filename))
    with open(filename, 'w') as fp:
        fp.write('0')


def _is_locked():
    filename = '/home/ubuntu/image-search/.bagheera.lock'
    return os.path.isfile(filename)


def _release_lock():
    filename = '/home/ubuntu/image-search/.bagheera.lock'
    assert (os.path.isfile(filename))
    os.remove(filename)


def count_files(dir):
    result = 0
    for root, dirs, files in os.walk(dir):
        result += len(files)
    return result


def resize2_images(input_directory, resized_directory):
    # resize images
    target_size = 231
    nb_files = count_files(input_directory)
    _log('Resizing %d images...' % nb_files)
    #widgets = ['Resizing: ', pb.Counter(), ", ", pb.Percentage(), ' ', pb.Bar(marker=pb.RotatingMarker()), ' ', pb.ETA()]
    #pbar = pb.ProgressBar(widgets=widgets, maxval=count_files(input_directory)).start()
    for root, dirs, files in os.walk(input_directory):
        for file in files:
            if not os.path.basename(file).endswith('.jpg'):
                continue
            dest = os.path.join(resized_directory,
                                file[0], file[1], file[2], file)
            if not os.path.exists(os.path.dirname(dest)):
                os.makedirs(os.path.dirname(dest))
            #pbar.update(pbar.currval + 1)
            command = "convert %s -resize %dx%d^ -gravity Center -crop %dx%s+0+0 +repage %s" % (
                os.path.join(root, file), target_size, target_size, target_size, target_size, dest)
            subprocess.Popen([command], shell=True,
                             stdout=subprocess.PIPE).stdout.readlines()
    # pbar.finish()


def resize_images(img_files, output_directory):

    target_size = 231

    for (a, b) in img_files:
        dest = os.path.join(output_directory, a)
        command = "convert %s -resize %dx%d^ -gravity Center -crop %dx%s+0+0 +repage %s" % (
            b, target_size, target_size, target_size, target_size, dest)
        subprocess.Popen([command], shell=True,
                         stdout=subprocess.PIPE).stdout.readlines()


def caffe_build_net(proc_id):
    caffe_root = '/home/ubuntu/caffe/'
    if not os.path.isfile(caffe_root + 'models/bvlc_reference_caffenet/bvlc_reference_caffenet.caffemodel'):
        _log("You need to download the caffe model first...")
        _log("Type : ./scripts/download_model_binary.py ../models/bvlc_reference_caffenet in CAFFE_ROOT")
        return

    # caffe.set_mode_cpu()
    # GPU mode
    _log('Setting caffe in GPU mode with device ID %d' % proc_id)

    caffe.set_mode_gpu()
    caffe.set_device(proc_id)

    net = caffe.Net(caffe_root + 'models/bvlc_reference_caffenet/deploy.prototxt',
                    caffe_root + 'models/bvlc_reference_caffenet/bvlc_reference_caffenet.caffemodel',
                    caffe.TEST)

    # input preprocessing: 'data' is the name of the input blob == net.inputs[0]
    transformer = caffe.io.Transformer({'data': net.blobs['data'].data.shape})
    transformer.set_transpose('data', (2, 0, 1))
    transformer.set_mean('data', np.load(
        caffe_root + 'python/caffe/imagenet/ilsvrc_2012_mean.npy').mean(1).mean(1))  # mean pixel
    # the reference model operates on images in [0,255] range instead of [0,1]
    transformer.set_raw_scale('data', 255)
    # the reference model has channels in BGR order instead of RGB
    transformer.set_channel_swap('data', (2, 1, 0))

    _log('Caffe net initialization done')

    return (net, transformer)


def benchmark_caffe():

    (net, transformer) = caffe_build_net(0)

    caffe_root = '/home/ubuntu/caffe/'

    n_images = 800

    _log('init the data...')
    t0 = time.clock()
    # set net to batch size
    net.blobs['data'].reshape(n_images, 3, 227, 227)
    _log('setting the data...')
    #net.blobs['data'].data[...] = transformer.preprocess('data', caffe.io.load_image(caffe_root + 'examples/images/cat.jpg'))
    net.blobs['data'].data[...] = transformer.preprocess(
        'data', caffe.io.load_image('/opt/_img/3231__15697.jpg'))

    _log('Running forward...')
    t1 = time.clock()
    out = net.forward()
    elapsed_time_0 = time.clock() - t0
    elapsed_time_1 = time.clock() - t1
    _log('Elapsed time: %.6f sec. / %.6f sec.' %
         (elapsed_time_1, elapsed_time_0))
    _log('Elapsed time per image: %.2f ms (%.2f ms with data transfer)' %
         ((1000*elapsed_time_1/n_images), (1000*elapsed_time_0/n_images)))

    _log("Predicted class for image 0 is #{}.".format(out['prob'][0].argmax()))

    _log([(k, v.data.shape) for k, v in net.blobs.iteritems()])

    # this is the output feature vector
    feat = net.blobs['fc7'].data[0].flat

    y = np.array([x for x in feat])
    z = y > 0

    _log(np.amax(y))
    _log(np.sum(z))
    _log(y.size)


def compute_hog(im, hog_resize):
    winSize = (64, 64)
    blockSize = (16, 16)
    blockStride = (16, 16)
    cellSize = (8, 8)
    nbins = 9
    derivAperture = 1
    winSigma = 4.
    histogramNormType = 0
    L2HysThreshold = 2.0000000000000001e-01
    gammaCorrection = 0
    nlevels = 64
    hog = cv2.HOGDescriptor(winSize, blockSize, blockStride, cellSize, nbins, derivAperture, winSigma,
                            histogramNormType, L2HysThreshold, gammaCorrection, nlevels)
    winStride = (64, 64)
    padding = (0, 0)
    locations = ()
    h = hog.compute(im, winStride, padding, locations)

    # resample
    lenh = len(h)
    if hog_resize != -1 and hog_resize < lenh:
        K = np.zeros((hog_resize, 1))
        r = math.floor(1.0*lenh/hog_resize)
        for k in range(hog_resize):
            K[k] = np.mean(h[k*r:(k+1)*r])
        h = K

    # normalize
    hl = np.linalg.norm(h)
    if hl > 1E-8:
        h = h / hl

    return h


def compute_features_hog(image_files, feature_directory):

    image_files = [x[1] for x in image_files]

    for filename in image_files:

        if not os.path.basename(filename).endswith('.jpg'):
            continue

        image_file = filename

        # read and resize
        im_r_size = 256
        im = cv2.resize(cv2.imread(image_file), (im_r_size, im_r_size))

        feat = compute_hog(im, -1)

        output_file = os.path.join(
            feature_directory, os.path.basename(image_file) + '.features')

        with open(output_file, 'w') as gp:
            gp.write('%d 1 1\n' % len(feat))  # same format as overfeat
            for a in range(len(feat)):
                gp.write('%.6f ' % feat[a])


def compute_features_caffe(net, transformer, image_files, feature_directory):

    caffe_root = '/home/ubuntu/caffe/'

    # run one pass forward
    #net.blobs['data'].data[...] = transformer.preprocess('data', caffe.io.load_image(caffe_root + 'examples/images/cat.jpg'))

    image_files = [x[1] for x in image_files]

    n_images = len(image_files)

    # set net to batch size
    _log('init the data with %d images...' % n_images)
    net.blobs['data'].reshape(n_images, 3, 227, 227)

    # write data to net
    _log('setting the data...')
    for (image_file, k) in zip(image_files, range(n_images)):
        net.blobs['data'].data[k, :, :, :] = transformer.preprocess(
            'data', caffe.io.load_image(image_file))

    _log('Running forward...')
    t0 = time.clock()
    out = net.forward()
    elapsed_time = time.clock() - t0
    _log('Elapsed time: %.6f sec.' % elapsed_time)
    _log('Elapsed time per image: %.2f ms' % (1000*elapsed_time/n_images))

    # _log("Predicted class for image 0 is #{}.".format(out['prob'][0].argmax()))

    #_log([(k, v.data.shape) for k, v in net.blobs.iteritems()])

    for (image_file, k) in zip(image_files, range(n_images)):
        output_file = os.path.join(
            feature_directory, os.path.basename(image_file) + '.features')
        feat = net.blobs['fc7'].data[k].flat
        with open(output_file, 'w') as gp:
            gp.write('%d 1 1\n' % len(feat))  # same format as overfeat
            for a in range(len(feat)):
                gp.write('%.6f ' % feat[a])


def compute_features_overfeat(input_directory, feature_directory, overfeat_lib, overfeat_batch):

    feature_extraction_cmd = "LD_LIBRARY_PATH=%s %s -i %s -o %s" % (
        overfeat_lib, overfeat_batch, input_directory, feature_directory)
    with open(os.devnull, "w") as fnull:
        _log(feature_extraction_cmd)
        subprocess.call(feature_extraction_cmd, shell=True,
                        stdout=fnull, stderr=fnull)


def get_batch_number_from_dirname(dirname):
    x = re.findall('[0-9]+', dirname)
    assert (x is not None)
    return int(x[0])


def resize_and_compute(proc_id, img_files, net, transformer, use_hog):

    # do not change the format of these directories.  They are used somewhere else in the code
    # e.g. to retrieve the batch number (batch_id) of a given image/feature file
    feature_directory = '/opt/_features_%d' % proc_id

    # compute features
    if os.path.isdir(feature_directory):
        _log('removing dir %s...' % feature_directory)
        shutil.rmtree(feature_directory)
    os.makedirs(feature_directory)

    if use_hog:
        compute_features_hog(img_files, feature_directory)
    else:
        compute_features_caffe(net, transformer, img_files, feature_directory)

    return feature_directory


def stack_features(feature_dir, out_filename):

    with open(out_filename, 'w') as go:
        for root, dirs, files in os.walk(feature_dir):
            for file in files:
                if not os.path.basename(file).endswith('.features'):
                    continue
                batch_id = get_batch_number_from_dirname(root)
                # splitext twice because overfeat stores features in a file called foo.jpg.features
                d = os.path.splitext(os.path.splitext(file)[0])[0].split('__')
                partner_id = int(d[0])
                internal_id = int(d[1])
                fullname = os.path.join(root, file)
                with open(fullname, 'r') as fp:
                    x = fp.readline().strip()  # skip first line (which contains metadata)
                    x = fp.readline().strip()  # this line contains the golden vector
                    # _log(x)
                    #z = x.split(' ')
                    # for a in z:
                    #    _log(a)
                    #    _log(float(a))
                    x = [float(y) for y in x.split(' ')]

                    # normalize vector before saving it
                    x = x / np.linalg.norm(x)
                go.write('%d %d %d ' % (batch_id, partner_id, internal_id))
                for a in range(len(x)):
                    go.write('%.6f ' % x[a])
                go.write('\n')


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def resize_and_compute_head(proc_unit_id, output_dir, img_files, use_hog):

    # no more than N images per batch on caffee due to GPU mem limits
    n_images_per_proc_unit = 500

    if not use_hog:
        # create nets
        (net, transformer) = caffe_build_net(proc_unit_id)
    else:
        net = None
        transformer = None

    # run batches
    file_sets = chunks(img_files, n_images_per_proc_unit)

    n_chunks = math.ceil(1.0 * len(img_files) / n_images_per_proc_unit)

    count = 0

    for file_set in file_sets:

        t0 = time.clock()

        feature_directory = resize_and_compute(
            proc_unit_id, file_set, net, transformer, use_hog)

        # stack all features in a single file
        out_filename = os.path.join(
            output_dir, 'cnn_features_%d_%d.txt' % (proc_unit_id, count))
        stack_features(feature_directory, out_filename)

        shutil.rmtree(feature_directory)

        count += 1

        elapsed_time = 1000 * (time.clock() - t0) / n_images_per_proc_unit

        progress = 100.0 * count / n_chunks
        _log('[%02d] progress = %2.2f %% (%.2f ms per image)' %
             (proc_unit_id, progress, elapsed_time))


def main():

    # read config file
    config = ConfigParser.ConfigParser()
    config.readfp(open('/home/ubuntu/image-search/defaults.ini'))
    overfeat_batch = config.get('Overfeat', 'overfeat_batch')
    overfeat_lib = config.get('Overfeat', 'overfeat_lib')
    feature_file_basename = config.get('Features', 'feature_file_basename')

    parser = argparse.ArgumentParser()
    parser.add_argument('--benchmark', dest='benchmark',
                        action='store_true', help='benchmark caffee', default=False)
    parser.add_argument('--caffe', dest='use_caffe', action='store_true',
                        help='run with caffe instead of overfeat', default=False)
    parser.add_argument('--hog', dest='use_hog', action='store_true',
                        help='compute HOG features', default=False)
    parser.add_argument('--input', dest='input_dir', type=str, required=False)
    args = parser.parse_args()

    assert (not (args.use_caffe and args.use_hog))

    # run a benchmark of caffe
    if args.benchmark:
        benchmark_caffe()
        return

    # override input dir
    if args.input_dir:
        input_dir = args.input_dir
    else:
        # find input_dir
        input_dir = '/opt/_img'
        dirs = os.listdir(input_dir)
        m_input_dir = None
        for mdir in dirs:
            if os.path.isdir(os.path.join(input_dir, mdir)) and os.path.isfile(os.path.join(input_dir, mdir, '.success.mowgli')):
                m_input_dir = os.path.join(input_dir, mdir)
                break

        if m_input_dir is None:
            _log('*** ERROR *** did not find a proper directory to process.')
            return

        input_dir = m_input_dir

    # list files
    _log('listing files in %s' % input_dir)
    img_files = []
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext == '.jpg':
                img_files.append((filename, os.path.join(root, filename)))
    n_images = len(img_files)

    # exit after having cleaned up output!
    if n_images == 0:
        _log('No images to process. Exiting')
        return

    # create output dir
    output_dir = os.path.join('/opt/output', os.path.basename(input_dir))
    if os.path.isdir(output_dir):
        _log('removing dir %s...' % output_dir)
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    n_proc_units = 32
    if args.use_caffe:
        n_proc_units = 4

    _log('# proc = %d, # images = %d' % (n_proc_units, n_images))

    # distribute in round-robin
    _log('distributing images...')
    img_files_gp = {}
    for k in range(n_proc_units):
        img_files_gp[k] = []
    for ((a, b), c) in zip(img_files, range(n_images)):
        img_files_gp[c % n_proc_units].append((a, b))

    jobs = []

    for c in range(n_proc_units):

        process = multiprocessing.Process(target=resize_and_compute_head, args=[
                                          c, output_dir, img_files_gp[c], args.use_hog])
        process.start()
        jobs.append(process)

    for proc in jobs:
        proc.join()

    if not args.input_dir:
        # delete input dir unless it was overriden by user
        backup_dir = '/opt/old.img'
        _log('Moving dir %s to backup directory %s' % (input_dir, backup_dir))
        shutil.move(input_dir, backup_dir)

    # write success file to output dir
    success_file = os.path.join(output_dir, '.success.bagheera')
    with open(success_file, 'w') as fp:
        fp.write('0')

    _log('Summary : processed %d images -> %s' % (n_images, output_dir))


if __name__ == "__main__":

    # header for the log file
    _log('======================================================================')
    _log('======================= BAGHEERA RUN =================================')
    _log('======================================================================')

    with PidFile():
        main()
