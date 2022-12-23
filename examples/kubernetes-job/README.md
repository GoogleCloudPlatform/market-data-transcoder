# `Deploy a simple example as a Kubernetes Job`

## Overview
This example downloads data from cmegroup and outputs the transcoded data to STDOUT.

## Requirements

### CLI Tools:
* KPT - [Installation Docs](https://kpt.dev/installation/kpt-cli)
* Skaffold - [Installation Docs](https://skaffold.dev/docs/install/)

### Requirements
A kubernetes cluster, in this example we will use GKE. 

#### Create a GKE Cluster

```
export PROJECT_ID=my-project-id
```

```
gcloud container clusters create-auto mdt-cluster \
    --region us-central1 \
    --project=${PROJECT_ID} 
```
#### Create your Artifact Registry repo to store your container images
```
gcloud artifacts repositories create docker --repository-format=docker 
--location=us --project=${PROJECT_ID}
```
##### Auth gcloud for Artifact Registry

```
gcloud auth configure-docker us-docker.pkg.dev
```

### Deployment
* [kpt](https://github.com/GoogleCloudPlatform/market-data-transcoder/blob/main/examples/kubernetes-job/kubernetes-manifests/Kptfile)
* [Skaffold](https://github.com/GoogleCloudPlatform/market-data-transcoder/blob/main/examples/kubernetes-job/skaffold.yaml)

### Configure Skaffold File
skaffold.yaml
* `us-docker.pkg.dev/my-project-id/repo-name/` to the registry you wish to use. 

### Update kpt setters
kubernetes-manifests/setters.yaml
```
image-repo: us-docker.pkg.dev/my-project-id/repo-name/
```
### Deploy workload
```
skaffold run
```

To view the output view the logs of the transcoder
```
kubectl get pods
```
Sample output
```
NAME                              READY   STATUS      RESTARTS   AGE
transcoder-job-simple-job-5b9bc   0/1     Completed   0          91s
```
Now get the logs
```
kubectl get logs $POD_NAME -c market-data-transcoder
```
You will see the transcoder output in the logs.

### Alternate Approach
#### Create a GCS Storage Bucket and Pre Load Data

```
wget -q -O - ftp://ftp.cmegroup.com/SBEFix/Production/secdef.dat.gz|gunzip - | head -10 > secdef.dat
wget -q -O - ftp://ftp.cmegroup.com/SBEFix/Production/TradingSessionList.dat| head -10 >> secdef.dat
wget -q 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/dict/FIX50SP2.CME.xml'
```

```
gcloud storage buckets create gs://${BUCKET_NAME}
gcloud storage cp secdef.dat gs://${BUCKET_NAME}/secdef.dat
gcloud storage cp FIX50SP2.CME.xml gs://${BUCKET_NAME}/secdef.dat
```
 #### Create Service Account
 ```
export PROJECT_ID=my-project-id

gcloud iam service-accounts create mdt-storage-reader \
    --project=${PROJECT_ID}

gcloud projects add-iam-policy-binding {$PROJECT_ID} \
    --member "serviceAccount:mdt-storage-reader@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role "roles/storage.objectViewer"

gcloud iam service-accounts add-iam-policy-binding mdt-storage-reader@${PROJECT_ID}.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:${PROJECT_ID}.svc.id.goog[market-data-transcoder/mdt-storage-reader-simple-job]"
```

#### Modify Job template to use gcloud init-container

Swap out the init containers by un commenting:

```
          image: google/cloud-sdk
          securityContext:
            allowPrivilegeEscalation: false
          command:
            - sh
            - -c
            - gcloud storage cp gs://bucket-name/secdef/* /data/
```
This will download the files from gcs and place the in the shared volume mount. 
