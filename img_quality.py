#!/usr/bin/env python

""" Compute image quality features. """

import os
import cv2
import cv2.cv as cv
import numpy as np
import numpy.ma as ma
import math
import shutil
from random import shuffle
import glob
from scipy import signal
from scipy import misc

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def has_white_background(imgray):
    """ return true if image has a white background """
    h, w = imgray.shape[:2]
    border = np.zeros((h, w), dtype='uint8')
    border[:10, :] = 1
    border[-10:, :] = 1
    border[:, :10] = 1
    border[:, -10:] = 1
    border_pixels = imgray[np.nonzero(border)]
    num_white_border_pixels = np.size(np.nonzero(border_pixels > 253))
    white_rate = 1.0 * num_white_border_pixels / np.size(border_pixels)
    return white_rate > .7


def segment_image(img_name, im, imgray):
    """ http://stackoverflow.com/questions/9469244/opencv-python-bindings-for-grabcut-algorithm """

    h, w = im.shape[:2]
    seg_type = -1
    itercount = 10
    mask = np.zeros((h, w), dtype='uint8')

    border_size = int(1.0*w/10)

    if border_size > 0:

        if not has_white_background(imgray):
            # found less than X white pixels, use rectangle approach
            seg_type = 0
            rect = (border_size, 0, w-2*border_size, h)
            tmp1 = np.zeros((1, 13 * 5))
            tmp2 = np.zeros((1, 13 * 5))
            try:
                cv2.grabCut(im, mask, rect, tmp1, tmp2,
                            itercount, mode=cv2.GC_INIT_WITH_RECT)
            except:
                print('WARNING : opencv grabcut failed.  Skipping segmentation.')
                seg_type = -1
        else:
            # found white pixels, use mask
            seg_type = 1
            mask[imgray < 240] = 1
            tmp1 = np.zeros((1, 13 * 5))
            tmp2 = np.zeros((1, 13 * 5))
            cv2.grabCut(im, mask, None, tmp1, tmp2,
                        itercount, mode=cv2.GC_INIT_WITH_MASK)
        # 0=background,1=foreground,2=likely background,3=likely foreground
        mask[mask == 2] = 0
        mask[mask == 3] = 1

    # sanity check
    if np.sum(mask) < 10:
        print('WARNING : segmentation failed for image %s (%d x %d). seg type %d, border size %d' % (
            img_name, w, h, seg_type, border_size))
        return (-1, mask)

    return (seg_type, 1-mask)


def list_partners(files):

    partner_ids = []
    for file in files:
        with open(file, 'r') as fp:
            for line in fp.readlines():
                d = line.strip().split(',')
                partner_id = int(d[0])
                partner_ids.append(partner_id)
    return list(set(partner_ids))


def colorfulness(im):
    """ computes the colorfulness of an image assuming BGR color space """
    rg = im[:, :, 2]-im[:, :, 1]
    yb = .5 * (im[:, :, 2] + im[:, :, 1]) - im[:, :, 0]
    try:
        sigma_rgyb = math.sqrt(rg.std()**2 + yb.std()**2)
    except:
        print('uh oh...')
        print(im.shape)
        print(im.compressed())
        print(np.sum(im.mask))
    mu_rgyb = math.sqrt(rg.mean()**2 + yb.mean()**2)
    return sigma_rgyb + .3 * mu_rgyb


def crispyness(im):
    # compute laplacian
    laplacian = np.array([[0, 1, 0], [1, -4, 1], [0, 1, 0]], dtype=np.float32)
    grad = signal.convolve2d(im, laplacian, boundary='symm', mode='same')
    return (grad, np.mean(np.abs(grad)))


def count_channels(im):
    if len(im.shape) == 2:
        return 1
    elif len(im.shape) == 3:
        return im.shape[2]
    return -1


class ImageQuality:

    def __init__(self, img_dir, img_file):
        self.im = None
        self.imbg = None
        self.img_dir = img_dir
        self.img_file = img_file
        self.partnerid = int(img_file.split('__')[0])
        self.colorfulness = None
        self.score = None
        self.grad = None
        self.seg_type = None
        self.params_valid = False
        self.score_valid = False

    def compute_params(self):
        im = cv2.imread(os.path.join(self.img_dir, self.img_file))
        if im is None:
            return
        n_channels = count_channels(im)
        if n_channels != 3:
            return

        imgray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)

        self.im = im
        h, w = im.shape[:2]
        bg_mask = np.zeros((h+2, w+2), np.uint8)
        lo = 2
        up = 2
        # flood fill from corners
        #self.imbg = im.copy()
        #cv2.floodFill (self.imbg, bg_mask,(0,0), (0,)*3, (lo,)*3, (up,)*3,flags=8)
        #cv2.floodFill (self.imbg, bg_mask,(w-1,h-1), (0,)*3, (lo,)*3, (up,)*3,flags=8)
        # fix mask size
        #bg_mask = bg_mask[1:-1,1:-1]

        # segment image
        #(self.seg_type,bg_mask) = segment_image (self.img_file, im,imgray)

        # stupid segmentation
        self.seg_type = 0
        bg_mask = 1.0*(imgray > 253)
        bg_mask = bg_mask.astype('uint8')

        if self.seg_type < 0:
            # save image to failed directory
            failed_dir = '/opt/data-sales/failed'
            if not os.path.isdir(failed_dir):
                os.makedirs(failed_dir)
            shutil.copy(os.path.join(self.img_dir, self.img_file), failed_dir)
            return
        self.imbg = np.multiply(self.im, cv2.cvtColor(
            1-bg_mask, cv2.COLOR_GRAY2RGB))

        # masked background/foreground images
        bg_ma = ma.masked_array(imgray, mask=1-bg_mask)
        fg_ma = ma.masked_array(imgray, mask=bg_mask)
        fg_ma_rgb = ma.masked_array(
            im, mask=cv2.cvtColor(bg_mask, cv2.COLOR_GRAY2RGB))
        # compute mean/std background value
        self.bg_lightness = bg_ma.mean()/255.0
        self.bg_variance = bg_ma.std()/255.0
        self.fg_lightness = fg_ma.mean()/255.0
        self.fg_variance = fg_ma.std()/255.0
        # brightness difference
        self.bg_fg_brightness_difference = abs(
            self.bg_lightness - self.fg_lightness)
        self.bg_fg_contrast_difference = abs(
            self.bg_variance - self.fg_variance)
        # colorfulness
        #self.colorfulness = colorfulness (fg_ma_rgb/255.0)
        self.colorfulness = colorfulness(im/255.0)
        # crispyness
        (self.grad, self.crispyness) = crispyness(imgray)
        # ratio of surfaces
        self.occupancy = 1.0 - \
            (1.0 * np.size(np.flatnonzero(bg_mask)) / np.size(bg_mask))
        # aspect ratio
        self.aspect_ratio = (1.0 - 1.0 * w / h) ** 2
        self.params_valid = True

    def compute_score(self):
        self.score_valid = False
        if not self.params_valid:
            return False
        #self.score = self.colorfulness * self.bg_lightness * self.bg_variance * self.bg_fg_bright_difference * self.bg_fg_contrast_difference
        score1 = self.colorfulness
        score2 = self.crispyness
        score3 = 1.0 - self.bg_variance
        score4 = self.bg_fg_contrast_difference
        score5 = self.bg_fg_brightness_difference
        score6 = self.occupancy
        score7 = self.fg_variance
        score8 = self.aspect_ratio
        self.score = [score1, score2, score3,
                      score4, score5, score6, score7, score8]
        for s in self.score:
            if math.isnan(s) or np.isnan(s):
                return
        self.score_valid = True

    def printout(self, scoreid):
        if not self.score_valid:
            return
        # if self.score < 0.01 or self.score > 0.57:
        #im_r_size = 256
        #imr = cv2.resize(self.im, (im_r_size,im_r_size))
        outdir = 'output/%d' % self.partnerid
        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        score_string = ('%.5f' % self.score[scoreid]).zfill(9)
        outname = os.path.join(outdir, '%02d-%s-%s' %
                               (scoreid, score_string, self.img_file))
        cv2.imwrite(outname, self.im)
        if scoreid == 0:
            outname = os.path.join(outdir, '_bg_%02d_%s-%s' %
                                   (scoreid, score_string, self.img_file))
            cv2.imwrite(outname, self.imbg)
        print('writing image to %s...' % outname)
        #print (self.img_file)
        #print (self.score[scoreid])


def unit_test():
    img_dir = '/opt/_img'
    img_name = '995__-4634592461251361049.jpg'
    img_name = '905__-1572187982742140121.jpg'
    img_name = '16398__-5963514622292355091.jpg'

    imq = ImageQuality(img_dir, img_name)
    imq.compute_params()
    imq.compute_score()
    imq.printout(0)


def main():

    # unit_test()
    # return

    output_dir = 'output'
    maximages = 10000
    maxpartners = 100
    maxscores = 1
    shuffle = False

    # clean output
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # list partners
    partners = list_partners(['partners.txt'])[:maxpartners]

    score_fd = open('scores.txt', 'w')

    for partnerid in partners:

        #partnerid = 13045

        print('processing partner %d' % partnerid)

        # list images
        img_dir = '/opt/_img'
        #img_dir = '/opt/lowik/images'
    #    img_list = os.listdir (img_dir)
        img_list = [os.path.basename(x) for x in glob.glob(
            os.path.join(img_dir, '%d__*.jpg' % partnerid))]

        # skip some images
        if maximages > 0:
            if shuffle:
                shuffle(img_list)
            img_list = img_list[:maximages]

        print('\t %d images...' % len(img_list))

        # compute features
        imq_list = []
        n_scores = 0
        progress = 1
        for (img_file, count) in zip(img_list, range(len(img_list))):
            imq = ImageQuality(img_dir, img_file)
            imq.compute_params()
            imq.compute_score()
            if imq.score_valid:
                n_scores = len(imq.score)
                imq_list.append(imq)
                score_fd.write('%.6f\n' % imq.score[0])

            if (100.0*count/len(img_list) > progress):
                print('%d %%' % progress)
                progress += 1

        if maxscores > 0:
            n_scores = min([n_scores, maxscores])

        # sort
        for scoreid in range(n_scores):
            imq_list = sorted(imq_list, key=lambda x: x.score[scoreid])
            for imq in imq_list[:10]:
                imq.printout(scoreid)
            for imq in imq_list[-10:]:
                imq.printout(scoreid)

    score_fd.close()


if __name__ == "__main__":
    main()
