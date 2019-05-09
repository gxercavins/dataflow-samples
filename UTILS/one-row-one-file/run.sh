#!/bin/bash
#!/bin/bash
if [ "$#" -ne 1 ]; then
   echo "Please specify the Project ID"
   echo "Usage:   ./run.sh project-id"
   exit
fi

PROJECT=$1

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.OneRowOneFile \
      -Dexec.args="--project=$PROJECT \
      --output="output/" \
      --runner=DirectRunner"

