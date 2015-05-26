#!/usr/bin/env python
import os
import subprocess
import shlex
import sys

def run_and_get_output(cmd):
    output = []
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    p.wait()

    return p.stdout.readlines()

def commit_file(fpath):
    cmd = ['git', 'commit',
           '-m', 'update '+fpath,
           fpath]
    print cmd
    ret = subprocess.call(cmd)
    if ret != 0:
        print '***********WARNING in commit_file()*********'
        print 'command not succeed'

def get_lastest_hash(fpath):
    cmd = ['git', 'log', '--pretty=format:"%H"',
            '-n', '1', fpath]
    print cmd
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE)
    proc.wait()
    hash = proc.communicate()[0]
    print hash
    return hash

def to_clipboard(s):
    cmd = ['pbcopy']
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    proc.communicate(input=s)
    proc.wait()
    print s, 'copied to clipboard'
    return()

def get_root_dir_name():
    path = run_and_get_output("git rev-parse --show-toplevel")
    path = path[0].strip()
    dirname = os.path.basename(path)
    return dirname

def compose_url(hash, fpath):
    # this is not robust, root dir name may not be reposit name
    # but it is OK..
    repname = get_root_dir_name()

    hash = hash.strip('"')
    url_base = "https://github.com/junhe/{rep}/blob/{hash}/{fpath}"
    url = url_base.format(hash=hash, fpath=fpath, rep=repname)
    return url

def git_push():
    cmd = ['git', 'push']
    print cmd
    subprocess.call(cmd)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage:", sys.argv[0], "filepath"
        exit(1)
    fpath = sys.argv[1]

    commit_file(fpath)
    git_push()
    hash = get_lastest_hash(fpath)
    url = compose_url(hash, fpath)
    to_clipboard(url)
