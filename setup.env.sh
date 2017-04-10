# This file should be executed within the current dir
sudo apt-get update
sudo apt-get install -y btrfs-tools f2fs-tools
sudo apt-get install -y python-bitarray
sudo apt-get install -y blktrace
sudo apt-get install -y xfsprogs
sudo apt-get install -y python-dev
sudo apt-get install -y python-pip

sudo pip install bidict
sudo pip install simpy

make f2fsgc

