#!/bin/bash
PROJECT=$1

# create table with data
bq mk test
bq mk test.students name:STRING,grade:STRING
bq query --use_legacy_sql=false 'INSERT INTO test.students (name, grade) VALUES ("Yoda", "A+"), ("Leia", "B+"), ("Luke", "C-"), ("Chewbacca", "F")'

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.DynamicQueries \
      -Dexec.args="--project=$PROJECT"
