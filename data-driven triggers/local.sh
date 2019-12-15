#!/bin/bash

mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.SODemoQuestion \
      -Dexec.args="--runner=DirectRunner"

