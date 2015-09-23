# This file should be executed within the current dir
doradir=`pwd`
sudo apt-get update
sudo apt-get install -y btrfs-tools f2fs-tools

sudo apt-get install -y cmake

sudo apt-get install -y mpich mpich-doc libmpich-dev

cd ../../
git clone https://github.com/junhe/chopper.git
cd chopper
cmake CMakeLists.txt
make
cd $doradir

sudo apt-get install -y python-bitarray

sudo apt-get install -y python-pip
sudo pip install bidict

sudo apt-get install -y blktrace

