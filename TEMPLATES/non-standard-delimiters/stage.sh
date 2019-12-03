#!/bin/bash
if [ "$#" -ne 2 ]; then
   echo "Please specify the Project ID and GCS Bucket Name"
   echo "Usage:   ./stage.sh project-id bucket-name"
fi

PROJECT=$1
BUCKET=$2

echo "Project: $PROJECT, Bucket: $BUCKET"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH

mvn -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.NonStandardDelimiters \
      -Dexec.args="--project=$PROJECT \
      --runner=DataflowRunner \
      --tempLocation=gs://$BUCKET/temp/ \
      --stagingLocation=gs://$BUCKET/staging/ \
      --templateLocation=gs://$BUCKET/templates/non-standard-delimiters"

