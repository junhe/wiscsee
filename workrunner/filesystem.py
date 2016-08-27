import abc
import os

import fshelper
from utilities import utils

class LoopDevice(object):
    def __init__(self, dev_path, tmpfs_mount_point, size_mb, img_file=None):
        self.dev_path = dev_path
        self.tmpfs_mount_point = tmpfs_mount_point
        self.size_mb = size_mb
        self.img_file = img_file

    def create(self):
        fshelper.make_loop_device(self.dev_path, self.tmpfs_mount_point,
            self.size_mb, self.img_file)

    def delete(self):
        fshelper.delLoopDev(self.dev_path)

class FileSystemBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, device, mount_point):
        self.dev = device
        self.mount_point = mount_point

    @abc.abstractmethod
    def make(self):
        "will never be here"
        raise NotImplementedError

    def mount(self, opt_list=None):
        opt_str = mountoption_to_str(opt_list)

        utils.prepare_dir(self.mount_point)
        ret = utils.shcmd('mount {opt} {dev} {mp}'.format(
            opt = opt_str, dev = self.dev, mp = self.mount_point),
            ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to mount dev:{} to dir:{}".format(
                self.dev, self.mount_point))

    def umount(self):
        ret = fshelper.umountFS(self.mount_point)
        if ret != 0:
            raise RuntimeError("Failed to umount {}".format(self.mount_point))

    def sync(self):
        common.shcmd("sync")

def opts_to_str(opt_dic):
    """
    This function translate opt_dic to a string complying command requirement

    opt_dic is in the format of:
    {'-O':['has_journal', '^uninit_bg'], '-X':['xx']}

    We will turn it to string:
    "-O has_journal,^uninit_bg -X xx"
    """
    if opt_dic == None or len(opt_dic) == 0:
        return ''

    opt_list = []
    for opt, values in opt_dic.items():
        values = [str(s) for s in values]
        value_str = ','.join(values)
        tmp = ' '.join((opt, value_str))
        opt_list.append(tmp)

    opt_str = ' '.join(opt_list)

    return opt_str

def mountoption_to_str(options):
    """
    options is a list of dictionaries:
    for example:
        { 'data':     {'opt_name':'data',
           'value': 'data',
           'include_name': True},
          'delalloc': {'opt_name':'dealloc',
           'value': 'dealloc',
           'include_name': False},
         ...
        }

      If you want to override mount options from /etc/fstab  you  have
      to use the -o option:

             mount device|dir -o options

      and  then  the  mount  options  from  the  command  line will be
      appended to the list of  options  from  /etc/fstab.   The  usual
      behavior  is  that the last option wins if there are conflicting
      ones.

    """
    if options == None:
        return ''

    strs = []
    for _, opt in options.items():
        if opt['value'] != None:
            if opt['include_name'] == True:
                itemstr = opt['opt_name'] + '=' + str(opt['value'])
            else:
                itemstr = str(opt['value'])
            strs.append(itemstr)

    if len(options) > 0:
        opt_str = '-o ' + ','.join(strs)
    else:
        opt_str = ''

    return opt_str

class Ext4(FileSystemBase):
    def make(self, opt_dic=None):
        opt_str = opts_to_str(opt_dic)

        ret = utils.shcmd('mkfs.ext4 {opt_str} -E nodiscard {dev}'.format(
            opt_str = opt_str, dev = self.dev), ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to make dev:{}".format(self.dev))

class F2fs(FileSystemBase):
    def make(self, opt_dic=None):
        opt_str = opts_to_str(opt_dic)

        ret = utils.shcmd('mkfs.f2fs -t 0 {opt} {dev}'.format(
            opt=opt_str, dev = self.dev), ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to make dev:{}".format(self.dev))

    def mount(self, opt_list=None):
        """
        Overriding mount() in parent since you need to have '-t f2fs' to
        mount f2fs, somehow.
        """
        opt_str = mountoption_to_str(opt_list)

        utils.prepare_dir(self.mount_point)
        ret = utils.shcmd('mount -t f2fs {opt} {dev} {mp}'.format(
            opt = opt_str, dev = self.dev, mp = self.mount_point), ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to mount dev:{} to dir:{}".format(
                self.dev, self.mount_point))
    def sysfs_setup(self, option, value):
        """
        This function sets up the parameters in sysfs.
        Option is the file name in sysfs.
        """
        devname = os.path.basename(self.dev)
        folder = '/sys/fs/f2fs/{dev}'.format(dev = devname)
        path = os.path.join(folder, option)
        with open(path, 'w') as f:
            f.write(str(value))

class Btrfs(FileSystemBase):
    def make(self, opt_dic=None):
        opt_str = opts_to_str(opt_dic)

        ret = utils.shcmd('mkfs.btrfs -f {opt} --nodiscard {dev}'.format(
            opt=opt_str, dev = self.dev), ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to make dev:{}".format(self.dev))

class Xfs(FileSystemBase):
    def make(self, opt_dic=None):
        if opt_dic == None:
            opt_str = ''
        else:
            items = [ ' '.join([k,v]) for k,v in opt_dic.items() ]
            opt_str = ' '.join(items)

        ret = utils.shcmd("mkfs.xfs {opt} -K -f -s size=4096 -b size=4096 {dev}"\
            .format(opt = opt_str, dev = self.dev), ignore_error = True)

        if ret != 0:
            raise RuntimeError("Failed to make dev:{}".format(self.dev))

# loopdev = LoopDevice(dev_path = '/dev/loop0', tmpfs_mount_point = '/mnt/tmpfs',
        # size_mb = 4096)
# loopdev.create()

# ext4 = Ext4(device='/dev/loop0', mount_point='/mnt/fsonloop')
# ext4.make()
# ext4.mount()

# f2fs = F2fs(device='/dev/loop0', mount_point='/mnt/fsonloop')
# f2fs.make()
# f2fs.mount()


