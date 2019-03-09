#!/bin/bash

JOB_NAME=global-options
BUCKET=gxt-proj1
TEMPLATE_NAME=java_options

gcloud dataflow jobs run $JOB_NAME \
    --gcs-location gs://$BUCKET/templates/$TEMPLATE_NAME\
    --parameters orgId=jomama47
