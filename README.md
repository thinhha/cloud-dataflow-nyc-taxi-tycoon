# NYC Taxi Tycoon - Dataflow Codelab

This is a modified example from the [NYC Taxi Tycoon Dataflow Codelab](https://gcplab.me/codelabs/cloud-dataflow-nyc-taxi-tycoon) 

## TL;DR
In this codelab you learn how to process streaming data with Dataflow. The public emulated data stream is based on 
the [NYC Taxi & Limousine Commission’s open dataset](https://data.cityofnewyork.us/) expanded with additional routing 
information using the [Google Maps Direction API](https://developers.google.com/maps/documentation/directions/) and 
interpolated timestamps to simulate a real time scenario.

## Public Pubsub Data Stream
The public [Google Cloud Pubsub](https://cloud.google.com/pubsub/) topic used in the codelab is available at: 
`projects/pubsub-public-data/topics/taxirides-realtime`.

We've copied this PubSub Topic into this project for you to access at:
`projects/skydataflowworkshop/topics/taxirides-realtime`

For this codelab, you should create your own data stream to pipe outputs to. You can do this using the [gcloud cli](https://cloud.google.com/sdk/gcloud/) from Cloud Shell. 
Make sure you are on the `skydataflowworkshop` project.

```gcloud pubsub topics create <your_username>-taxirides-outputs```

Then create a subscription for your topic using the following command (otherwise the messages won't be persisted)
```gcloud pubsub subscriptions create taxi-test-sub --topic projects/skydataflowworkshop/topics/<your_username>-taxirides-outputs```

To pull messages from your subscription:

```gcloud pubsub subscriptions pull projects/skydataflowworkshopsubscriptions/<your_username>-taxirides-outputs```

## Contents of this repository

This is a mini-version of the original Codelab.

To get an overview of the scenario, see https://codelabs.developers.google.com/codelabs/cloud-dataflow-nyc-taxi-tycoon/#0

In `dataflow/src/main/java/com/google/codelabs/dataflow/DollarRides.java`, you will find the answer to https://codelabs.developers.google.com/codelabs/cloud-dataflow-nyc-taxi-tycoon/#7
Use `dataflow/run.sh` to try executing this script, e.g.:

```
./run.sh -m com.google.codelabs.dataflow.DollarRides -p thinhha-visualizer -r DataflowRunner
```

Your task in this session is to complete https://codelabs.developers.google.com/codelabs/cloud-dataflow-nyc-taxi-tycoon/#9
We've already provided a backbone template for you to start in `dataflow/src/main/java/com/google/codelabs/dataflow/ExactDollarRides.java`

*Use: The NYC Taxi & Limousine Commission’s dataset is publicly available for anyone to use under the following terms 
provided by the Dataset Source —https://data.cityofnewyork.us/— and is provided "AS IS" without any warranty, express 
or implied, from Google. 
Google disclaims all liability for any damages, direct or indirect, resulting from the use of the dataset.*

*This is not an official Google product*
