FROM prefecthq/prefect:latest-python3.7
RUN pip install boto3
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git
RUN pip install git+https://github.com/developmentseed/pangeo-forge
RUN pip install s3fs
RUN pip install h5py
RUN pip install h5netcdf
