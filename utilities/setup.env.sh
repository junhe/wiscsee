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

# for FIO
sudo apt-get install -y libaio-dev

cd ../../
wget https://github.com/axboe/fio/archive/fio-2.2.12.tar.gz
tar xf fio-2.2.12.tar.gz
cd fio-fio-2.2.12
./configure
make
sudo make install
cd $doradir

cd ../..
git clone git@github.com:junhe/leveldb.git
cd leveldb
make
make db_bench
cd $doradir

cd ../..
python reuse/scripts/install-filebench.py .
cd $doradir



sudo apt-get install -y python-bitarray

sudo apt-get install -y python-pip
sudo pip install bidict

sudo apt-get install -y blktrace

sudo pip install simpy

sudo apt-get install -y xfsprogs

sudo apt-get install -y python-dev

sudo apt-get install -y libffi-dev

sudo pip install cffi

sudo pip install ordereddict

cd ../pyfallocate && python fallocate_build.py 

cd ../ && make f2fsgc


# for RocksDB
sudo apt-get install -y libgflags-dev
sudo apt-get install -y zlib1g-dev
sudo apt-get install -y libbz2-dev
cd $doradir
cd ../..
git clone git@github.com:junhe/rocksdb.git
cd rocksdb
git checkout jun-v4.11.2
make db_bench
cd $doradir





