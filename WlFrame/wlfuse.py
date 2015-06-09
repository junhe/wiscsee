# This a framework for create file systems, manipulate blktraces, and parse
# blktraces
import fs

def main():
    fs.ext4_create_on_loop()

if __name__ == '__main__':
    main()

