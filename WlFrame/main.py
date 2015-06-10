# This a framework for create file systems, manipulate blktraces, and parse
# blktraces
import fs

def main():
    fs.prepare_loop()
    fs.ext4_make_simple()
    fs.ext4_mount_simple()

if __name__ == '__main__':
    main()

