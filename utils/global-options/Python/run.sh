#!/bin/bash
if [ "$#" -ne 2 ]; then
   echo "Please specify the Project ID and GCS Bucket Name"
   echo "Usage:   ./run.sh project-id bucket-name"
   exit
fi

PROJECT=$1
BUCKET=$2

echo "Project: $PROJECT, Bucket: $BUCKET"

# stage the template
python global.py \
    --runner DataflowRunner \
    --project $PROJECT \
    --staging_location gs://$BUCKET/staging \
    --temp_location gs://$BUCKET/temp \
    --template_location gs://$BUCKET/templates/global_options

sleep 2

# execute the template
gcloud dataflow jobs run global_options \
   --gcs-location gs://$BUCKET/templates/global_options \
   --parameters path=test_path,table_name=test_table