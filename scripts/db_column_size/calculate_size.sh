DATA_DIR=$1
RES_DIR=$2
SCRIPT=$3

DUMP_FILE='dump_result'
COLUMNS_FILE='db.rs'

mkdir -p $RES_DIR
time sst_dump --show_properties --file=$DATA_DIR --command=none \
	| grep -Eiw '.*(table size:|column family name:).*' \
	>  $RES_DIR/$DUMP_FILE
wget https://raw.githubusercontent.com/near/nearcore/master/core/store/src/db.rs -O $RES_DIR/$COLUMNS_FILE
