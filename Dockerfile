FROM prefecthq/prefect:latest-python3.7
RUN pip install boto3
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git
RUN pip install -r requirements.txt
