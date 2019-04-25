#!/bin/bash

gcloud pubsub topics publish topic-one --message "message 1"

for i in `seq 1 5`;
do
  gcloud pubsub topics publish topic-five --message "message $i"
done

for i in `seq 1 10`;
do
  gcloud pubsub topics publish topic-ten --message "message $i"
done

