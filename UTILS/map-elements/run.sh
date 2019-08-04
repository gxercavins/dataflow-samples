#!/bin/bash
if [ "$#" -ne 1 ]; then
   echo "Please specify the Project ID"
   echo "Usage:   ./run.sh project-id"
   exit
fi

PROJECT=$1

mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.SampleTextIO \
      -Dexec.args="--project=$PROJECT \
      --runner=DirectRunner"

