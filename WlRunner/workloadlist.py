import os

class SysCall(dict):
    def __init__(self, dic):
        super(SysCall, self).__init__(dic)
        assert not self.has_key("NA")

    def __str__(self):

        if self['name'] in ("write", "read"):
            items = ['pid', 'path', 'name', 'offset', 'count']
        elif self['name'] in ("open", "close", "fsync", "mkdir"):
            items = ['pid', 'path', 'name']
        elif self['name'] in ("sync",):
            items = ['pid', 'NA', 'name']
            self['NA'] = 'NA'
        elif self['name'] in ("sched_setaffinity",):
            items = ['pid', "NA", "name", 'cpuid']
            self['NA'] = 'NA'
        else:
            raise RuntimeError("Syscall {} is not supported."\
                .format(self['name']))

        if set(items) != set(self.keys()):
            raise RuntimeError("{} != {}".format(set(items), set(self.keys())))

        entry = ';'.join([str(self[k]) for k in items])

        return entry


class WorkloadList(object):
    """The class encapsulate operations in a more intuitive way."""
    def __init__(self, mountpoint):
        self.action_list = []
        self.mountpoint = mountpoint

    def get_abs_path(self, path):
        if not os.path.isabs(path):
            path = os.path.join(self.mountpoint, path)
        return path

    def add_call(self, **kwargs):
        # change path if necessary
        if kwargs.has_key('path'):
            kwargs['path'] = self.get_abs_path(kwargs['path'])

        self.action_list.append(SysCall(kwargs))

    def __str__(self):
        return '\n'.join([str(x) for x in self.action_list])

    def save(self, file_path):
        with open(file_path, 'w') as f:
            f.write(str(self))

if __name__ == '__main__':
    wl = WorkloadList('/tmp')
    wl.add_call(name='mkdir', pid=0, path='mydir')
    wl.add_call(name='open', pid=0, path='mypath')
    wl.add_call(name='write', pid=0, path='mypath', offset=0, count=4096)
    wl.add_call(name='read', pid=0, path='mypath', offset=0, count=4096)
    wl.add_call(name='fsync', pid=0, path='mypath')
    wl.add_call(name='close', pid=0, path='mypath')
    wl.add_call(name='sync', pid=0)
    wl.add_call(name='sched_setaffinity', pid=0, cpuid=0)

    print wl
    print str(wl)
    wl.save("mywl.txt")
