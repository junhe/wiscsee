#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#include <linux/ioctl.h>

#define F2FS_IOCTL_MAGIC                0xf5
#define F2FS_IOC_START_ATOMIC_WRITE     _IO(F2FS_IOCTL_MAGIC, 1)
#define F2FS_IOC_COMMIT_ATOMIC_WRITE    _IO(F2FS_IOCTL_MAGIC, 2)
#define F2FS_IOC_START_VOLATILE_WRITE   _IO(F2FS_IOCTL_MAGIC, 3)
#define F2FS_IOC_RELEASE_VOLATILE_WRITE _IO(F2FS_IOCTL_MAGIC, 4)
#define F2FS_IOC_ABORT_VOLATILE_WRITE   _IO(F2FS_IOCTL_MAGIC, 5)
#define F2FS_IOC_GARBAGE_COLLECT        _IO(F2FS_IOCTL_MAGIC, 6)
#define F2FS_IOC_WRITE_CHECKPOINT       _IO(F2FS_IOCTL_MAGIC, 7)
#define F2FS_IOC_DEFRAGMENT             _IO(F2FS_IOCTL_MAGIC, 8)

int main(int argc, char** argv)
{
    char *devpath;
    int fd;
    int ret;
    int status;
    int arg;

    if (argc != 3) {
        printf("Usage: %s mount-point sync\n", argv[0]);
        printf("sync is the third arg passed to ioctl. "
                "Generally, sync=1 implies forground gc. "
                "sync=0 implies background gc.");
        exit(1);
    }

    devpath = argv[1];
    arg = atoi(argv[2]);
    printf("arg:%d\n", arg);

    fd = open(devpath, 0);
    if (fd == -1) {
        perror("open file error");
        exit(1);
    }

    ret = ioctl(fd, F2FS_IOC_GARBAGE_COLLECT, &arg);

    if (ret == -1) {
        perror("ioctl error");
    }
    printf("ioctl ret: %d.\n", ret);

    close(fd);
    return(0);
}

