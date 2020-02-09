#!/bin/bash
if [ "$#" -ne 3 ]; then
   echo "Please specify the Project ID, GCS Bucket Name and BigQuery Table"
   echo "Usage:   ./run.sh project-id bucket-name project:dataset.table"
   exit
fi

PROJECT=$1
BUCKET=$2
TABLE=$3

echo "Project: $PROJECT, Bucket: $BUCKET, Table: $TABLE"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=org.apache.beam.examples.BigQueryUpsert \
      -Dexec.args="--project=$PROJECT \
      --output=$TABLE \
      --tempLocation=gs://$BUCKET/temp/"


