#!/usr/bin/env python
import os
import subprocess
import shlex
import sys

from utilities import utils

def run_and_get_output(cmd):
    output = []
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    p.wait()

    return p.stdout.readlines()

def commit_file(fpath):
    rootdir = get_root_dir_path()
    cmd = ['git', 'commit',
           '-am', 'Update '+fpath+' by get_github_url.py']
           # os.path.join(rootdir, fpath)]
    print cmd
    ret = subprocess.call(cmd)
    if ret != 0:
        print '***********WARNING in commit_file()*********'
        print 'command not succeed'

def get_lastest_hash(fpath):
    rootdir = get_root_dir_path()
    cmd = ['git', 'log', '--pretty=format:"%H"',
            '-n', '1',
            os.path.join(rootdir, fpath)]
    print cmd
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE)
    proc.wait()
    hash = proc.communicate()[0].strip('"')
    print hash
    return hash

def to_clipboard(s):
    cmd = ['pbcopy']
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    proc.communicate(input=s)
    proc.wait()
    print s, 'copied to clipboard'
    return()

def get_root_dir_path():
    path = run_and_get_output("git rev-parse --show-toplevel")
    return path[0].strip()

def get_root_dir_name():
    path = get_root_dir_path()
    dirname = os.path.basename(path)
    return dirname

def compose_url(hash, fpath):
    # this is not robust, root dir name may not be reposit name
    # but it is OK..
    repname = get_root_dir_name()

    hash = hash.strip('"')

    # need_raw = True
    need_raw = False
    if need_raw == True:
        # https://raw.githubusercontent.com/junhe/doraemon/6d06b0dcf8193a26100804d1a6dd06ea22b88ff8/analysis/analyzer.r?token=AAmfJeReIQX2oaVbztHhUhYzD9z82o1Sks5VbnLHwA%3D%3D
        mytoken = 'AAmfJeReIQX2oaVbztHhUhYzD9z82o1Sks5VbnLHwA%3D%3D'
        url_base_raw = "https://raw.githubusercontent.com/junhe/{rep}/{hash}/{fpath}?token={token}"
        url = url_base_raw.format(hash=hash, fpath=fpath, rep=repname, token=mytoken)
    else:
        url_base = "https://github.com/junhe/{rep}/blob/{hash}/{fpath}"
        url = url_base.format(hash=hash, fpath=fpath, rep=repname)

    return url

def compose_r_code_block(commit, fpath):
    code = """``` {{r cache=TRUE, echo=FALSE, warning=FALSE, message=FALSE, results='hide'}}
library(devtools)
source_url("https://gist.github.com/junhe/1f7e41f4c2829486e46f/raw/source_private_github_file.r")
source_private_github_file("{repo_name}", "{file_path}", "{commit}")
```"""\
        .format(repo_name = get_root_dir_name(), file_path = fpath,
        commit = commit)
    return code

def git_push():
    cmd = ['git', 'push']
    print cmd
    subprocess.call(cmd)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage:", sys.argv[0], "filepath"
        print "filepath should be a relative path to repository root dir"
        print "This script can be executed anywhere within the repository"
        exit(1)
    fpath = sys.argv[1]

    utils.shcmd("git pull")

    commit_file(fpath)
    utils.shcmd("git pull")
    git_push()
    commithash = get_lastest_hash(fpath)
    # url = compose_url(hash, fpath)
    code = compose_r_code_block(commithash, fpath)
    to_clipboard(code)
