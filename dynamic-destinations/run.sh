#!/bin/bash
if [ "$#" -ne 3 ]; then
   echo "Please specify the Project ID, GCS Bucket Name and BigQuery Table prefix"
   echo "Usage:   ./run.sh project-id bucket-name project:dataset.table"
   exit
fi

PROJECT=$1
BUCKET=$2
TABLE=$3

echo "Project: $PROJECT, Bucket: $BUCKET, Table: $TABLE"

# sudo pip install lorem
# python generate.py
# gsutil cp input.csv gs://$BUCKET

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.WriteToDifferentTables \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=gs://$BUCKET/input.csv \
      --output=$TABLE \
      --runner=DataflowRunner"
