#!/bin/bash
if [ "$#" -ne 3 ]; then
   echo "Please specify the Project ID, GCS Bucket Name and Pub/Sub Topic"
   echo "Usage:   ./run.sh project-id bucket-name topic-name"
   exit
fi

PROJECT=$1
BUCKET=$2
TOPIC=$3

echo "Project: $PROJECT, Bucket: $BUCKET, Topic: $TOPIC"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.EmptyWindows \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/topics/$TOPIC \
      --runner=DataflowRunner"
