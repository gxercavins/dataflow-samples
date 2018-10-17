#!/bin/bash

PROJECT=PROJECT_ID
BUCKET=BUCKET_NAME

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.OptionsInParDo \
      -Dexec.args="--project=$PROJECT \
      --tempLocation=gs://$BUCKET/temp/ \
      --gcpTempLocation=gs://$BUCKET/temp/ \
      --templateLocation=gs://$BUCKET/templates/java_options \
      --runner=DataflowRunner"
