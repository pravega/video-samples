#!/bin/bash

ROOT_DIR= "$(dirname $0)/../.."
TMP_DIR= '/tmp/person-database-0.1.0'
export CONTROLLER_URL='tcp://10.243.37.168:9090'
export pravega_client_auth_method='Bearer'
export pravega_client_auth_loadDynamic='true'
export KEYCLOAK_SERVICE_ACCOUNT_FILE='/home/vidyat/Desktop/video-samples/keycloak.json'


$ROOT_DIR/gradlew person-database:distTar
#if [ -d "$TMP_DIR" ]; then rm -Rf $TMP_DIR; fi
tar -xvf person-database/build/distributions/person-database-0.1.0.tar -C /tmp
