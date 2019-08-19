import os
import shutil
import numpy as np

"""Split image files in a round-robin fashion. """

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def image_filename(partner_id, external_item_id):
    return os.path.join('/opt/_img/', '%d__%d.jpg' % (partner_id, external_item_id))


def reset_dir(dirname):
    if os.path.isdir(dirname):
        shutil.rmtree(dirname)
    os.makedirs(dirname)


def process_file(imglist_filename, output_dir):

    reset_dir(output_dir)
    count = 0
    sale_count = [0]*1000000

    with open(imglist_filename, 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            d = line.strip().split(',')
            partnerid = int(d[0])
            external_item_id = int(d[1])
            nsales = int(d[2])
            img_filename = image_filename(partnerid, external_item_id)
            if os.path.isfile(img_filename):
                #            print ('%s --> %s' % (img_filename,output_dir))
                shutil.copy(img_filename, output_dir)
                sale_count[count] = nsales
                count += 1

    sale_count = np.array(sale_count[:count])
    print('%d files copied to %s, # mean sales: %.2f, max sales: %d, median %.2f' % (
        count, output_dir, np.mean(sale_count), np.amax(sale_count), np.median(sale_count)))


def main():
    img_dir = '/opt/_img'
    sales_file = '/opt/sales.txt'
    nosales_file = '/opt/nosales.txt'
    outputdir_sales = '/opt/tf/criteo/sales'
    outputdir_nosales = '/opt/tf/criteo/nosales'

    process_file(sales_file, outputdir_sales)
    process_file(nosales_file, outputdir_nosales)


if __name__ == "__main__":
    main()
