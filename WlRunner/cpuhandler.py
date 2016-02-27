import glob
import os

def get_possible_cpus():
    f = open("/sys/devices/system/cpu/possible", 'r')
    line = f.readline()
    f.close()

    # assuming format of 0-2,4,6-63
    items = line.split(',')
    cpus = []
    for item in items:
        if '-' in item:
            a,b = item.split('-')
            a = int(a)
            b = int(b)
            cpus.extend(range(a, b+1))
        else:
            cpus.append(int(item))

    return cpus

def get_available_cpu_dirs():
    "Counting dirs is more accurate than */cpu/possible, at least on emulab"
    cpudirs = [name for name in glob.glob("/sys/devices/system/cpu/cpu[0-9]*") \
                        if os.path.isdir(name)]
    return cpudirs

def get_online_cpuids():
    with open('/sys/devices/system/cpu/online', 'r') as f:
        line = f.readline().strip()

    # assuming format of 0-2,4,6-63
    items = line.split(',')
    cpus = []
    for item in items:
        if '-' in item:
            a,b = item.split('-')
            a = int(a)
            b = int(b)
            cpus.extend(range(a, b+1))
        else:
            cpus.append(int(item))

    return cpus

def switch_cpu(cpuid, mode):
    path = "/sys/devices/system/cpu/cpu{cpuid}/online"
    path = path.format(cpuid=cpuid)

    modedict = {'ON':'1', 'OFF':'0'}

    f = open(path, 'w')
    f.write(modedict[mode])
    f.flush()
    f.close()

    return

def enable_all_cpus():
    possible_cpus = get_possible_cpus()
    enable_n_cpus(len(possible_cpus))

def set_cpus(n):
    if n == 'NOOP' or n == None:
        return

    if n == 'all':
        enable_all_cpus()
        return

    enable_n_cpus(n)

def enable_n_cpus(n):
    """
    Enable n CPUs
    """
    online_cpus = get_online_cpuids()

    n_online = len(online_cpus)
    if n_online == n:
        return
    elif n_online > n:
        # more than wanted is online, disable some
        for cpuid in online_cpus[n:]:
            switch_cpu(cpuid, 'OFF')
    else:
        # we need some more CPU to be online
        need = n - n_online
        possible_cpus = get_possible_cpus()
        for cpuid in possible_cpus:
            if not cpuid in online_cpus:
                switch_cpu(cpuid, 'ON')
                need -= 1

                if need == 0:
                    break
        if need > 0:
            raise RuntimeError("Need {} CPUS, but only got {}".format(
                n, n - need))

    online_cpus = get_online_cpuids()
    assert len(online_cpus) == n


