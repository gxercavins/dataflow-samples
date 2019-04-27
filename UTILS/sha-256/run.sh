#!/bin/bash
if [ "$#" -ne 2 ]; then
   echo "Please specify the GCS Input path and GCS Output path"
   echo "Usage:   ./run.sh input-path output-path"
   #else
fi

INPUT=$1
OUTPUT=$2

echo "Input: $INPUT, Output: $OUTPUT"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.DataflowSHA256 \
           -Dexec.args="--input=$INPUT "
#                        --output=$OUTPUT"
