#!/bin/bash


containers=("P6_TEST_AUTOMATIZE")

SOURCE_BASE="/home/aac/NOISEPORT-TENERIFE/CONTENEDORES"
DEST_BASE="/srv/contenedores/CONTENEDORES/CONTENEDORES"



for container in "${containers[@]}"; do
    SRC_DIR="${SOURCE_BASE}/P1_CONTENEDORES/wav_files"
    DEST_DIR="${DEST_BASE}/3-Medidas/${container}/wav_files"


    if [ ! -d "$SRC_DIR" ]; then
        echo "Source directory $SRC_DIR does not exist. Skipping ${container}."
        continue
    fi


    if [ ! -d "$DEST_DIR" ]; then
        echo "Destination directory $DEST_DIR does not exist. Creating it..."
        mkdir -p "$DEST_DIR"
    fi

    echo "Moving contents from $SRC_DIR to $DEST_DIR..."
    rsync -avh --progress "$SRC_DIR"/* "$DEST_DIR"



done

echo "All moves completed."
