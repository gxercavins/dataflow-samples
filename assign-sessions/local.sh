#!/bin/bash

mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.AssignSessions \
      -Dexec.args="--runner=DirectRunner"

