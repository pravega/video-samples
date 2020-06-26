#!/bin/bash


ROOT_DIR="$(dirname $0)/../.."
IMAGES_DIR="${ROOT_DIR}/images/person-database"

ls ${IMAGES_DIR}
for dir in "${IMAGES_DIR}"/*; do
#    echo $dir
    PERSON_NAME="$(basename "$dir")"
#    echo $PERSON_NAME
    for file in "${dir}"/*; do
#      echo $file
      FILE_NAME="$(basename "$file")"
      if [ "${FILE_NAME}" != "*" ]; then
        echo "${FILE_NAME}"
        "${ROOT_DIR}"/scripts/person-database/add-person.sh --personId="${PERSON_NAME}" --imagePath="${file}"
      fi
    done
done
