#!/bin/bash

mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.BigQueryStorageAPI \
      -Dexec.args="--output=hacker-stories/ \
      --runner=DirectRunner"

