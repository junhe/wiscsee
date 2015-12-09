# This file should be executed within the current dir
doradir=`pwd`
sudo apt-get update
sudo apt-get install -y btrfs-tools f2fs-tools

sudo apt-get install -y cmake

sudo apt-get install -y mpich mpich-doc libmpich-dev

cd ../../
git clone https://github.com/junhe/wlgen.git
cd wlgen
make
cd $doradir

cd ../../
wget https://github.com/axboe/fio/archive/fio-2.2.12.tar.gz
cd fio-2.2.12
./configure
make
sudo make install
cd $doradir

sudo apt-get install -y python-bitarray

sudo apt-get install -y python-pip
sudo pip install bidict

sudo apt-get install -y blktrace



