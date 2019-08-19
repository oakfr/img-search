from datetime import datetime

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


class FileStatus:
    def __init__(self, permissions, number_of_replicas, userid, groupid, filesize, modification_date, modification_time, filename):
        self.permissions = permissions
        self.number_of_replicas = number_of_replicas
        self.userid = userid
        self.groupid = groupid
        self.filesize = int(filesize)
        self.modification_date = datetime.strptime(
            modification_date + " " + modification_time, '%Y-%m-%d %H:%M')
        self.filename = filename

    def __repr__(self):
        return str((self.permissions, self.number_of_replicas, self.userid, self.groupid, self.filesize, self.modification_date, self.filename))

    def __eq__(self, other):
        return (self.permissions == other.permissions and
                self.number_of_replicas == other.number_of_replicas and
                self.userid == other.userid and
                self.groupid == other.groupid and
                self.filesize == other.filesize and
                self.modification_date == other.modification_date and
                self.filename == other.filename)
