#!/bin/bash

ROOT_DIR='/home/vidyat/Desktop/video-samples'
$ROOT_DIR/gradlew classes -b $ROOT_DIR/person-database/build.gradle

for i in "$@"
do
case $i in
    --personId=*)
    personId="${i#*=}"
    shift # past argument=value
    ;;
    --imagePath=*)
    imagePath="${i#*=}"
    shift # past argument=value
    ;;
    --transactionType=*)
    transactionType="${i#*=}"
    shift # past argument=value
    ;;
esac
done

echo "personId  = ${personId}"
echo "imagePath = ${imagePath}"
echo "transactionType = ${transactionType}"
if [[ -n $1 ]]; then
    echo "Last line of file specified as non-opt/last argument:"
    tail -1 "$1"
fi

$ROOT_DIR/gradlew run -b $ROOT_DIR/person-database/build.gradle -PjvmArgs="-DpersonId=${personId},-DimagePath=${imagePath},-DtransactionType=${transactionType}"