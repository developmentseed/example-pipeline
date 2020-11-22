### Example pangeo-forge pipeline usage with test Fargate cluster.

The .env file contains cluster information for an existing test stack.  Or you can deploy a new test stack using [pangeo-forge-cluster](https://github.com/developmentseed/pangeo-forge-cluster).

Create and publish an image which contains the necessary dependecies for your the flow. At a minimum this image should be based on `prefecthq/prefect:latest-python3.7` and requires `boto3` in order to retrieve the flow from S3 Storage.  See the example Dockerfile which creates an image with dependecies for `/recipe/pipeline.py`.

Alternatively, you can use the `Dockerfile` in this repository:

```bash
export DOCKER_TAG=pangeo-forge-edstars
export AWS_ACCOUNT_ID=XXX

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
docker build -t $DOCKER_TAG .
docker tag $DOCKER_TAG:latest $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/$DOCKER_TAG:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/$DOCKER_TAG:latest
```

Update the `.env` file with the published image tag.

### Running a local agent

Install dependecies (preferably in a virtualenv) with 
```bash
pip install -r requirements.txt
# or
python -m pip install -r requirements.txt
```

Configure your prefect cli with
```bash
$ prefect auth login -t <COPIED_TOKEN>
```

```bash
$ export RUNNER_TOKEN=$(prefect auth create-token -n my-runner-token -s RUNNER)
```

Create and register the test flow 
```bash
python recipe/pipeline.py
```

To start the local agent which will poll prefect for this flow and then run it use
```bash
export AWS_PROFILE=devseed
prefect agent local start --token $RUNNER_TOKEN
```

Intitiate Flow run by submitting a flow run to the prefect api with
```bash
prefect run flow --name $FLOW_NAME --project 'edstars'
```
