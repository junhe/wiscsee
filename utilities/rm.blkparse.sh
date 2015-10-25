# Usage ./rm.blkparse.sh dir
echo $1
find $1 -name 'blkparse*' -delete -print
