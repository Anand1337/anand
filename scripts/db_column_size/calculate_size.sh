DATA_DIR=$1
RES_DIR=$2
SCRIPT=$3

DUMP_FILE='dump_result'
COLUMNS_FILE='db.rs'

if [[ ! -f /usr/bin/sst_dump ]]
then
	apt install -y g++
	apt install -y libsnappy-dev
	apt install -y zlib1g-dev
	apt install -y libbz2-dev
	apt install -y liblz4-dev
	apt install -y libzstd-dev
	apt install -y make 
	git clone https://github.com/facebook/rocksdb.git
	cd rocksdb
	DEBUG_LEVEL=0 make sst_dump
	cp sst_dump /usr/bin
	cd ../
	rm -rf rocksdb
fi

mkdir -p $RES_DIR
time sst_dump --show_properties --file=$DATA_DIR --command=none \
	| grep -Eiw '.*(table size:|column family name:).*' \
	>  $RES_DIR/$DUMP_FILE
wget https://raw.githubusercontent.com/near/nearcore/master/core/store/src/db.rs -O $RES_DIR/$COLUMNS_FILE
