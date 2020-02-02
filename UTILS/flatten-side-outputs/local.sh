#!/bin/bash
export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=org.apache.beam.examples.FlattenSideOutputs
