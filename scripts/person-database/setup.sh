#!/bin/bash

ROOT_DIR='/home/vidyat/Desktop/video-samples'
TMP_DIR='/tmp/person-database-0.1.0'

$ROOT_DIR/gradlew person-database:distTar
#if [ -d "$TMP_DIR" ]; then rm -Rf $TMP_DIR; fi
tar -xvf person-database/build/distributions/person-database-0.1.0.tar -C /tmp
