#!/bin/bash
if [ "$#" -ne 1 ]; then
   echo "Please specify the Project ID"
   echo "Usage:   ./local.sh project-id"
   exit
fi

PROJECT=$1

echo "Project: $PROJECT"

mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.MultipleTopics \
      -Dexec.args="--project=$PROJECT \
      --topicList=projects/$PROJECT/topics/topic1,projects/$PROJECT/topics/topic2,projects/$PROJECT/topics/topic3 \
      --runner=DirectRunner"
