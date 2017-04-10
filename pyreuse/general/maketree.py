import os


def mklevel(cur_level, max_level, dir_width, prefix):
    if cur_level == max_level:
        return

    # Create directory
    os.makedirs(prefix)
    # print prefix

    for i in range(dir_width):
        mklevel(cur_level = cur_level + 1,
                max_level = max_level,
                dir_width = dir_width,
                prefix = os.path.join(prefix, str(i)))


def main():
    mklevel(cur_level=0, max_level=3, dir_width=3, prefix='./new4')

if __name__ == '__main__':
    main()


