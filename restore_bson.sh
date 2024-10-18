#!/bin/bash

# Variabel untuk konfigurasi MongoDB
MONGO_URI="mongodb://root:example_password@localhost:27017"  # Ganti dengan URI MongoDB Anda
DB_NAME="iot-server"  # Ganti dengan nama database Anda
FOLDER_PATH="/home/uwais/mongodb/iot-server/data"  # Ganti dengan path folder yang berisi file BSON

# Fungsi untuk merestore satu file BSON
restore_bson_file() {
    local bson_file="$1"
    local collection_name=$(basename "$bson_file" .bson)  # Nama collection dari nama file BSON

    echo "Restoring collection: $collection_name from file: $bson_file"
    mongorestore --uri="$MONGO_URI/$DB_NAME?authSource=admin" "$bson_file"
    
    if [ $? -eq 0 ]; then
        echo "Successfully restored $collection_name"
    else
        echo "Error restoring $collection_name"
    fi
}

# Loop melalui semua file BSON di dalam folder
for bson_file in "$FOLDER_PATH"/*.bson; do
    if [ -f "$bson_file" ]; then
        restore_bson_file "$bson_file"
    else
        echo "No BSON files found in $FOLDER_PATH"
        exit 1
    fi
done

echo "All BSON files have been processed."
