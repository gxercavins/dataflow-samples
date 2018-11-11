#!/bin/bash
PROJECT=$1

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.DynamicQueries \
      -Dexec.args="--project=$PROJECT"
