# There are many comments in this example. Remove them when you've
# finalized your pipeline.
from bs4 import BeautifulSoup
import os
import pandas as pd
import pangeo_forge
import pangeo_forge.utils
from pangeo_forge.tasks.http import download
from pangeo_forge.tasks.xarray import combine_and_write
from pangeo_forge.tasks.zarr import consolidate_metadata
from prefect import Flow, Parameter, task, unmapped, flatten
from prefect.environments import LocalEnvironment
from prefect.environments.storage import S3
from prefect.engine.executors import DaskExecutor
from dotenv import load_dotenv
import os
import requests
load_dotenv()

# We use Prefect to manage pipelines. In this pipeline we'll see
# * Tasks: https://docs.prefect.io/core/concepts/tasks.html
# * Flows: https://docs.prefect.io/core/concepts/flows.html
# * Parameters: https://docs.prefect.io/core/concepts/parameters.html

# A Task is one step in your pipeline. The `source_url` takes a day
# like '2020-01-01' and returns the URL of the raw data.

def noaa_sst_avhrr_url_pattern(datetime_str: str) -> str:
    return (
        "https://www.ncei.noaa.gov/data/"
        "sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/"
        "{datetime_str:%Y%m}/oisst-avhrr-v02r01.{datetime_str:%Y%m%d}.nc"
    )

def list_files(url: str, ext='') -> list:
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    url = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return url

gpm_host = "https://gpm1.gesdisc.eosdis.nasa.gov"
gpm_l3_imerg_path = "data/GPM_L3"
gpm_l3_imerg_daily = "GPM_3IMERGDF.06/{pd_datetime:%Y}/{pd_datetime:%m}/"
gpm_l3_imerg_half_hourly = "GPM_3IMERGHH.06/{pd_datetime:%Y}/{pd_datetime:%j}/"
def gesdisc_gpm_imerg_dir_pattern(pd_datetime: str, product: str) -> str:
    product_path = gpm_l3_imerg_daily if product == 'daily' else gpm_l3_imerg_half_hourly
    return (f"{gpm_host}/{gpm_l3_imerg_path}/{product_path}")

@task
def source_url(datetime_str: str) -> str:
    """
    Format the URL for a specific day.
    """
    pd_datetime = pd.Timestamp(datetime_str)
    source_url_pattern = gesdisc_gpm_imerg_dir_pattern(pd_datetime, 'half-hourly')
    url = source_url_pattern.format(pd_datetime=pd_datetime)
    if os.path.isfile(url):
        return url
    else:
        urls = list_files(url, ext='HDF5')
        return urls

# All pipelines in pangeo-forge must inherit from pangeo_forge.AbstractPipeline


class Pipeline(pangeo_forge.AbstractPipeline):
    # You must define a few pieces of metadata in your pipeline.
    # name is the pipeline name, typically the name of the dataset.
    name = os.getenv("FLOW_NAME")
    # repo is the URL of the GitHub repository this will be stored at.
    repo = "notused"
    image = os.getenv("IMAGE")
    vpc = os.getenv("VPC")
    security_group = os.getenv("SECURITY_GROUP")
    cluster_arn = os.getenv("CLUSTER_ARN")
    task_role_arn = os.getenv("TASK_ROLE_ARN")
    execution_role_arn = os.getenv("EXECUTION_ROLE_ARN")
    executor = DaskExecutor(
        cluster_class="dask_cloudprovider.aws.FargateCluster",
        cluster_kwargs={
            "image": image,
            "vpc": vpc,
            "cluster_arn": cluster_arn,
            "task_role_arn": task_role_arn,
            "execution_role_arn": execution_role_arn,
            "security_groups": [
                security_group
            ],
            "n_workers": 1,
            "scheduler_cpu": 256,
            "scheduler_mem": 512,
            "worker_cpu": 1024,
            "worker_mem": 2048,
            "scheduler_timeout": "15 minutes",
        },
    )
    environment = LocalEnvironment(
        #executor=executor,
    )
    storage = S3(bucket=os.getenv("STORAGE_BUCKET"))
    # Some pipelines take parameters. These are things like subsets of the
    # data to select or where to write the data.
    # See https://docs.prefect.io/core/concepts/parameters.htm for more
    days = Parameter(
        # All parameters have a "name" and should have a default value.
        "days",
        default=pd.date_range("2000-06-01", "2000-06-01", freq="D").strftime("%Y-%m-%d").tolist(),
    )
    cache_location = Parameter(
        "cache_location", default=f"s3://{os.getenv('SCRATCH_BUCKET')}/cache/{name}.zarr"
    )
    target_location = Parameter("target_location", default=f"s3://{os.getenv('STORAGE_BUCKET')}/{name}.zarr")

    auth = Parameter(
        "auth", default=('usename', 'password')
    )

    @property
    def sources(self):
        # This can be ignored for now.
        pass

    @property
    def targets(self):
        # This can be ignored for now.
        pass

    def get_test_parameters(self, defaults: dict):
        parameters = dict(defaults)  # copy the defaults
        parameters["days"] = defaults["days"][:5]
        parameters["cache_location"] = "cache/"
        parameters["target_location"] = "target.zarr"
        parameters["auth"] = (os.getenv('EARTHDATA_USERNAME'), os.getenv('EARTHDATA_PASSWORD'))
        return parameters

    # The `Flow` definition is where you assemble your pipeline. We recommend using
    # Prefects Functional API: https://docs.prefect.io/core/concepts/flows.html#functional-api
    # Everything should happen in a `with Flow(...) as flow` block, and a `flow` should be returned.
    @property
    def flow(self):
        with Flow(
            self.name,
            environment=self.environment,
            storage=self.storage,
        ) as flow:
            # Use map the `source_url` task to each day. This returns a mapped output,
            # a list of string URLS. See
            # https://docs.prefect.io/core/concepts/mapping.html#prefect-approach
            # for more. We'll have one output URL per day.
            sources = source_url.map(self.days)

            # Map the `download` task (provided by prefect) to download the raw data
            # into a cache.
            # Mapped outputs (sources) can be fed straight into another Task.map call.
            # If an input is just a regular argument that's not a mapping, it must
            # be wrapped in `prefect.unmapped`.
            # https://docs.prefect.io/core/concepts/mapping.html#unmapped-inputs
            # nc_sources will be a list of cached URLs, one per input day.
            nc_sources = download.map(
                flatten(sources),
                cache_location=unmapped(self.cache_location),
                auth=unmapped(self.auth),
                use_source_filename=unmapped(True)
            )

            # The individual files would be a bit too small for analysis. We'll use
            # pangeo_forge.utils.chunk to batch them up. We can pass mapped outputs
            # like nc_sources directly to `chunk`.
            chunked = pangeo_forge.utils.chunk(nc_sources, size=5)

            # Combine all the chunked inputs and write them to their final destination.
            writes = combine_and_write.map(
                chunked,
                unmapped(self.target_location),
                append_dim=unmapped("time"),
                concat_dim=unmapped("time"),
                group=unmapped('Grid')
            )

            # Consolidate the metadata for the final dataset.
            consolidate_metadata(self.target_location, writes=writes)

        return flow


# pangeo-forge and Prefect require that a `flow` be present at the top-level
# of this module.
flow = Pipeline().flow
flow.register(project_name="edstars")
