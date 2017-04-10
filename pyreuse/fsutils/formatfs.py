import os

from pyreuse.helpers import shcmd, prepare_dir

def create_fs(dev, mntpoint, fstype):
    shcmd("sudo chmod 777 -R {}".format(mntpoint))

    if fstype == 'ext4':
        shcmd("sudo mkfs.ext4 {}".format(dev))
    elif fstype == 'ext3':
        shcmd("sudo mkfs.ext3 {}".format(dev))
    else:
        raise NotImplementedError('{} not supported yet'.format(fstype))

    shcmd("sudo mount {dev} {mnt}".format(dev = dev, mnt = mntpoint))

def register_fstab(dev, mntpoint, fstype):
    line = "{dev}         {mntpoint}      {fstype} defaults     0       0"\
        .format(dev = dev, mntpoint = mntpoint, fstype = fstype)

    with open('/etc/fstab', 'a') as f:
        f.write(line)

def format_fs(dev, mntpoint, fstype):
    """
    It will format, mount the file system. Then it will register the file
    system in the /etc/fstab.
    """
    prepare_dir(mntpoint)
    create_fs(dev, mntpoint, fstype)
    register_fstab(dev, mntpoint, fstype)


