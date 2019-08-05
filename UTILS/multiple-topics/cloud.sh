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
 -Dexec.mainClass=com.dataflow.samples.MultipleTopics \
      -Dexec.args="--project=$PROJECT \
      --topicList=projects/$PROJECT/topics/topic1,projects/$PROJECT/topics/topic2,projects/$PROJECT/topics/topic3 \
      --stagingLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner"

