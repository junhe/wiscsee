import abc

import fshelper
import utils

class MountOption(dict):
    """
    This class abstract the option of file system mount command.
    The initial motivation is to handle the difference, for example, between
    data=ordered and delalloc. Note that one of them has format opt_name=value,
    the other is just value.

    It inherite dict class so json module can serialize it.
    """
    def __init__(self, opt_name, value, include_name):
        """
        opt_name: such as discard
        value: such as discard, nodiscard
        include_name: such as True, False
        """
        self['opt_name'] = opt_name
        self['value'] = value
        self['include_name'] = include_name

    def __str__(self):
        if self['include_name']:
            prefix = self['opt_name'] + '='
        else:
            prefix = ''

        return prefix + self['value']

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
        value_str = ','.join(values)
        tmp = ' '.join((opt, value_str))
        opt_list.append(tmp)

    opt_str = ' '.join(opt_list)

    return opt_str

def mountoption_to_str(options):
    """
    options is a list of MountOption instances
    """
    if options == None:
        return ''

    strs = [str(opt) for _,opt in options.items()]
    opt_str = '-O ' + ','.join(strs)

    return opt_str

class Ext4(FileSystemBase):
    def make(self, opt_dic=None):
        opt_str = opts_to_str(opt_dic)

        ret = utils.shcmd('mkfs.ext4 -b 4096 {opt_str} {dev}'.format(
            opt_str = opt_str, dev = self.dev), ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to make dev:{}".format(self.dev))

class F2fs(FileSystemBase):
    def make(self, opt_dic=None):
        opt_str = opts_to_str(opt_dic)

        ret = utils.shcmd('mkfs.f2fs {opt} {dev}'.format(
            opt=opt_str, dev = self.dev), ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to make dev:{}".format(self.dev))

    def mount(self, opt_list=None):
        """
        Overriding mount() in parent since you need to have '-t f2fs' to
        mount f2fs, somehow.
        """
        opt_str = mountoption_to_str(opt_list)

        ret = utils.shcmd('mount -t f2fs {opt} {dev} {mp}'.format(
            opt = opt_str, dev = self.dev, mp = self.mount_point), ignore_error = True)
        if ret != 0:
            raise RuntimeError("Failed to mount dev:{} to dir:{}".format(
                self.dev, self.mount_point))

class Btrfs(FileSystemBase):
    def make(self, opt_dic=None):
        opt_str = opts_to_str(opt_dic)

        ret = utils.shcmd('mkfs.btrfs {opt} {dev}'.format(
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

        ret = utils.shcmd("mkfs.xfs {opt} -f -s size=4096 -b size=4096 {dev}"\
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


