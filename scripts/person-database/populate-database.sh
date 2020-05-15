#!/bin/bash


ROOT_DIR='/home/vidyat/Desktop/video-samples'
IMAGES_DIR='/home/vidyat/Desktop/video-samples/images/person-database'

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
