#!/bin/bash
if [ "$#" -ne 4 ]; then
   echo "Please specify the Project ID, GCS Bucket Name, Pub/Sub Topic and BigQuery Table"
   echo "Usage:   ./run.sh project-id bucket-name topic-name project:dataset.table"
   exit
fi

PROJECT=$1
BUCKET=$2
TOPIC=$3
TABLE=$4

echo "Project: $PROJECT, Bucket: $BUCKET, Topic: $TOPIC, Table: $TABLE"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.LateData \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/topics/$TOPIC \
      --output=$TABLE \
      --runner=DataflowRunner"

echo "Sleeping for 120s while job starts before publishing messages to topic"
sleep 120

# publish 4 Pub/Sub messages, one of them starting with the word 'late'
gcloud pubsub topics publish $TOPIC --message="this is a test"
gcloud pubsub topics publish $TOPIC --message="to see what happens with"
gcloud pubsub topics publish $TOPIC --message="late data"
gcloud pubsub topics publish $TOPIC --message="and timestamps"