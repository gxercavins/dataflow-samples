#!/bin/bash
if [ "$#" -ne 2 ]; then
   echo "Please specify the Project ID and GCS Bucket Name"
   echo "Usage:   ./cloud.sh project-id bucket-name"
   exit
fi

PROJECT=$1
BUCKET=$2

echo "Project: $PROJECT, Bucket: $BUCKET"

mvn -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.ChronologicalOrder \
      -Dexec.args="--project=$PROJECT \
      --path=gs://$BUCKET/data/** \
      --stagingLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner"

