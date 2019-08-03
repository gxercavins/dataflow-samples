#!/bin/bash

if [ "$#" -ne 2 ]; then
   echo "Please specify the Project ID and GCS Bucket Name"
   echo "Usage:   ./run.sh project-id bucket-name"
   exit
fi

PROJECT=$1
BUCKET=$2

echo "Which example do you want to run?"
echo "[1]: Initial example (OOM)"
echo "[2]: Combine tags into a single key"
echo "[3]: Composite hierarchical keys"
echo "[4]: Group questions into daily windows"
echo "[5]: Just counting questions"
echo "[6]: Ingest only the fields needed"
echo "[7]: Define our own combiner"
echo "[8]: Use a side input to control hot-key fanout"
echo "Please specify number (1-8): "

while [ -z "$MAIN_CLASS" ];
do
	read EXAMPLE

	case $EXAMPLE in
	1)  MAIN_CLASS=InitialExample
	    FOLDER=initial
	    ;;
	2)  MAIN_CLASS=CombineTags
	    FOLDER=combined
	    ;;
	3)  MAIN_CLASS=HierarchicalKeys
	    FOLDER=hierarchical
	    ;;
	4)  MAIN_CLASS=AddWindowing
	    FOLDER=with-windows
	    ;;
	5)  MAIN_CLASS=CountQuestions
	    FOLDER=count
	    ;;
	6)  MAIN_CLASS=ChangeIngestion
	    FOLDER=from-query
	    ;;
	7)  MAIN_CLASS=SimpleHotKeyFanout
	    FOLDER=accum
	    ;;
	8)  MAIN_CLASS=DynamicHotKeyFanout
	    FOLDER=dynamic-fanout
	    ;;
	*)  echo "$EXAMPLE is not a valid number (1-8). Try again: "
	    ;;
	esac
done

echo "Example: $MAIN_CLASS, Output: gs://$BUCKET/hot-keys/$FOLDER/"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn  -Pdataflow-runner compile -e exec:java \
 -Dexec.mainClass=org.apache.beam.examples.$MAIN_CLASS \
      -Dexec.args="--project=$PROJECT \
      --tempLocation=gs://$BUCKET/temp/ \
      --stagingLocation=gs://$PROJECT/staging/ \
      --output=gs://$BUCKET/hot-keys/$FOLDER/ \
      --runner=DataflowRunner"
