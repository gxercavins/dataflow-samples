#!/bin/bash
if [ "$#" -ne 1 ]; then
   echo "Please specify the GCS Bucket Name"
   echo "Usage:   ./run_locally.sh bucket-name"
   exit
fi

pip install --upgrade google-cloud-bigquery

export BUCKET=$1

cat <<EOT >> countries1.csv
id,country
1,sweden
2,spain
EOT

cat <<EOT >> countries2.csv
id,country
3,italy
4,france
EOT

gsutil cp countries* gs://$BUCKET

rm countries*

bq mk test.file_mapping FILENAME:STRING,FILE_ID:STRING
bq query --use_legacy_sql=false 'INSERT INTO test.file_mapping (FILENAME, FILE_ID) values ("countries1.csv", "COUNTRIES ONE"), ("countries2.csv", "COUNTRIES TWO")'

python filenames.py
