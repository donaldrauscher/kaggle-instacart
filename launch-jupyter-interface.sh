#!/bin/bash

DATAPROC_CLUSTER_NAME=instacart-dataproc
REGION=us-central1
ZONE=us-central1-f

JUPYTER_PORT=$(gcloud dataproc clusters describe ${DATAPROC_CLUSTER_NAME} --region=${REGION} | python -c "import sys,yaml; cluster = yaml.load(sys.stdin); print(cluster['config']['gceClusterConfig']['metadata']['JUPYTER_PORT'])")

ZONE_FLAG=""
[[ -v ZONE ]] && ZONE_FLAG="--zone=${ZONE}"
gcloud compute ssh "${ZONE_FLAG}" --ssh-flag='-D 10000' --ssh-flag='-N' --ssh-flag='-n' "${DATAPROC_CLUSTER_NAME}-m" &
sleep 5 # Wait for tunnel to be ready before opening browser...

/usr/bin/chromium-browser \
  "http://${DATAPROC_CLUSTER_NAME}-m:${JUPYTER_PORT}" \
  --proxy-server='socks5://localhost:10000' \
  --host-resolver-rules='MAP * 0.0.0.0 , EXCLUDE localhost' \
  --user-data-dir='/tmp/'
