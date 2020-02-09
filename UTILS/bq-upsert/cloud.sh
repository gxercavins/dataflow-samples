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
mvn -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=org.apache.beam.examples.BigQueryUpsert \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/temp/ \
      --output=$TABLE \
      --runner=DataflowRunner"
