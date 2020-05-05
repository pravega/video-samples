#!/bin/bash

ROOT_DIR='/home/vidyat/Desktop/video-samples/camera-recorder/src/main/java/io/pravega/example/camerarecorder'

while [ $# -gt 0 ]; do
  case "$1" in
    --personId=*)
      personId="${1#*=}"
      ;;
    --imagePath=*)
      imagePath="${1#*=}"
      ;;
    *)
      printf "***************************\n"
      printf "* Error: Invalid argument.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
  printf 'personId=%s' "$personId"
  printf 'imagePath=%s' "$imagePath"

  javac $ROOT_DIR/PersonDatabaseConnection.java --personId $personId --imagePath=$imagePath
done





