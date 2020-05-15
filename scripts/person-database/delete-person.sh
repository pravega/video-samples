#!/bin/bash

ROOT_DIR='/home/vidyat/Desktop/video-samples'

for i in "$@"
do
case $i in
    --personId=*)
    personId="${i#*=}"
    shift # past argument=value
    ;;
esac
done

echo "personId  = ${personId}"
if [[ -n $1 ]]; then
    echo "Last line of file specified as non-opt/last argument:"
    tail -1 "$1"
fi

export PERSON_DATABASE_OPTS="-DpersonId=${personId} -DtransactionType=delete -DPRAVEGA_SCOPE=examples
 -DOUTPUT_STREAM_NAME=person-database-transaction -DPRAVEGA_CONTROLLER_URI=tcp://localhost:9090 -DIS_CREATE_SCOPE=true"

/tmp/person-database-0.1.0/bin/person-database
