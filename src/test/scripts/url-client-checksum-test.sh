#!/bin/bash

GOOD_CHECKSUM="77dd08be8a0abd55aae7e2456aa4929c"
BAD_CHECKSUM="77dd08be8a0abd55aae7e2456aa492ff"
CONFIG_DIR="/home/yousee/Yousee-config/youseebitrepositoryingester/"
TEST_FILE="testfile.jpg"
FILE_SIZE="1076911"
FILE_URL="/home/yousee/scratch/$TEST_FILE"
YOUSEE_HOME="/home/yousee/yousee-workflow/services/workflow"


do_ingest() {
    echo "Checksum: $1"
    CMD="$JAVA_HOME/bin/java -cp $YOUSEE_HOME/components/yousee-bitrepository-ingester/bin/*:$YOUSEE_HOME/components/yousee-bitrepository-ingester/external-products/* \
    dk.statsbiblioteket.medieplatform.bitrepository.ingester.Ingester \
    $CONFIG_DIR file://$FILE_URL $TEST_FILE $checksum $FILE_SIZE"

    OUTPUT=`$CMD`
    return "$?"
}

cd $(dirname $(readlink -f $0))

cp $TEST_FILE $FILE_URL

echo "Attempting ingest of file with bad checksum"
checksum=$BAD_CHECKSUM
do_ingest $checksum
case "$?" in 
    0) echo "Failure! Ingest succeded when it should not!";;
    *) echo "Success! Ingest failed as expected.";;
esac

echo "Attempting ingest of file with good checksum"
checksum=$GOOD_CHECKSUM
do_ingest $checksum
case "$?" in
    0)  echo "Success! Ingest succeded as expected.";;    
    *)  echo "Failure! Ingest failed when it should not!";;
esac


