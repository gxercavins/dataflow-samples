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
 -Dexec.mainClass=com.dataflow.samples.AdaptiveTriggers \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --input=projects/$PROJECT/topics/$TOPIC \
      --output=$TABLE \
      --runner=DataflowRunner"

sudo pip install google-cloud-pubsub
echo "Sleeping for 90s while job starts before publishing messages to topic"
sleep 90
python publish.py $PROJECT publish $TOPIC
