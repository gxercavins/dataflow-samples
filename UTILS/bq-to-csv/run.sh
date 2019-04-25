#!/bin/bash
if [ "$#" -ne 2 ]; then
   echo "Please specify the Project ID and GCS Bucket Name"
   echo "Usage:   ./run.sh project-id bucket-name"
   exit
fi

PROJECT=$1
BUCKET=$2

echo "Project: $PROJECT, Bucket: $BUCKET"

python bq-to-csv.py --bucket $BUCKET --project $PROJECT

echo -e "\nResults:\n======="
gsutil cat gs://$BUCKET/results/output-00000-of-00001.csv
