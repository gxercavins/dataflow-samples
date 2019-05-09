#!/bin/bash

PROJECT=$1

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=org.apache.beam.examples.BeamSQL \
      -Dexec.args="--project=$PROJECT \
      --runner=DirectRunner"
