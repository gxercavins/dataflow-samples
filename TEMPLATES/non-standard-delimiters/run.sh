#!/bin/bash
if (( $# < 3 )); then
   echo "Please specify the GCS bucket, input file, BigQuery Table and (optionally) the custom delimiter"
   echo "Usage:   ./run.sh bucket_name gs://bucket/path/to/file project:dataset.table '\x01\n'"
   exit 1
fi

BUCKET=$1
INPUT=$2
TABLE=$3

if [ -z "$4" ]
  then
    echo "Bucket: $BUCKET, File: $INPUT, Table: $TABLE"
    gcloud dataflow jobs run non-standard-delimiters \
        --gcs-location gs://$BUCKET/templates/non-standard-delimiters \
        --parameters input=$INPUT,output=$TABLE
  else
    DELIMITER=$4
    echo "Bucket: $BUCKET, File: $INPUT, Table: $TABLE, Delimiter: $DELIMITER"
    gcloud dataflow jobs run non-standard-delimiters \
        --gcs-location gs://$BUCKET/templates/non-standard-delimiters \
        --parameters input=$INPUT,output=$TABLE,delimiter=$DELIMITER
fi

