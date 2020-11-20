### Example pangeo-forge pipeline usage with test Fargate cluster.

The .env file contains cluster information for an existing test stack.  Or you can deploy a new test stack using [pangeo-forge-cluster](https://github.com/developmentseed/pangeo-forge-cluster).

Create and publish an image which contains the necessary dependecies for your the flow. At a minimum this image should be based on `prefecthq/prefect:latest-python3.7` and requires `boto3` in order to retrieve the flow from S3 Storage.  See the example Dockerfile which creates an image with dependecies for `/recipe/pipeline.py`.

Update the .env file with the published image tag 

Install dependecies (preferably in a virtualenv) with 
```bash
pip install -r requirements.txt
```

Configure your prefect cli with
```bash
$ prefect auth login -t <COPIED_TOKEN>
```
```bash
$ prefect auth create-token -n my-runner-token -s RUNNER
```

Create and register the test flow 
```bash
python recipe/pipeline.py
```

To start the local agent which will poll prefect for this flow and then run it use
```bash
prefect agent local start --token <Your copied runner token>
```

Intitiate Flow run by submitting a flow run to the prefect api with
```bash
prefect run flow --name 'Test flow' --project 'edstars'
```
