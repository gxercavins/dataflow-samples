#!/bin/bash
if [ "$#" -ne 2 ]; then
   echo "Please specify the Project ID and GCS Bucket Name"
   echo "Usage:   ./run.sh project-id bucket-name"
   exit
fi

PROJECT=$1
BUCKET=$2

echo "Project: $PROJECT, Bucket: $BUCKET, looking for files matching gs://$BUCKET/sales/* and gs://$BUCKET/events/*"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.RegexFileIO \
      -Dexec.args="--project=$PROJECT \
      --bucket=$BUCKET \
      --runner=DirectRunner"

