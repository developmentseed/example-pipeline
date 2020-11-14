# There are many comments in this example. Remove them when you've
# finalized your pipeline.
import pandas as pd
import pangeo_forge
import pangeo_forge.utils
from pangeo_forge.tasks.http import download
from pangeo_forge.tasks.xarray import combine_and_write
from pangeo_forge.tasks.zarr import consolidate_metadata
from prefect import Flow, Parameter, task, unmapped
from prefect.environments import LocalEnvironment
from prefect.environments.storage import S3
from prefect.engine.executors import DaskExecutor

# We use Prefect to manage pipelines. In this pipeline we'll see
# * Tasks: https://docs.prefect.io/core/concepts/tasks.html
# * Flows: https://docs.prefect.io/core/concepts/flows.html
# * Parameters: https://docs.prefect.io/core/concepts/parameters.html

# A Task is one step in your pipeline. The `source_url` takes a day
# like '2020-01-01' and returns the URL of the raw data.

@task
def source_url(day: str) -> str:
    """
    Format the URL for a specific day.
    """
    day = pd.Timestamp(day)
    source_url_pattern = (
        "https://www.ncei.noaa.gov/data/"
        "sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/"
        "{day:%Y%m}/oisst-avhrr-v02r01.{day:%Y%m%d}.nc"
    )
    return source_url_pattern.format(day=day)


# All pipelines in pangeo-forge must inherit from pangeo_forge.AbstractPipeline


class Pipeline(pangeo_forge.AbstractPipeline):
    # You must define a few pieces of metadata in your pipeline.
    # name is the pipeline name, typically the name of the dataset.
    name = "example"
    # repo is the URL of the GitHub repository this will be stored at.
    repo = "pangeo-forge/example-pipeline"

    executor = DaskExecutor(
        cluster_class="dask_cloudprovider.FargateCluster",
        cluster_kwargs={
            "image": "552819999234.dkr.ecr.us-west-2.amazonaws.com/pangeoforgeedstarsbdd50ad8-xutm9k0eec0y",
            "cluster_arn": "arn:aws:ecs:us-west-2:552819999234:cluster/pangeo-forge-cluster-cluster611F8AFF-FXH7MKgq80pR",
            "task_role_arn": "arn:aws:iam::552819999234:role/pangeo-forge-cluster-taskRole4695B131-1A9FZ6LJAMGGR",
            "execution_role_arn": "arn:aws:iam::552819999234:role/pangeo-forge-cluster-taskExecutionRole505FC329-W32D8BO43LJV",
            "security_groups": [
                "sg-09a5ee69d3671fa12"
            ],
            "n_workers": 1,
            "scheduler_cpu": 256,
            "scheduler_mem": 512,
            "worker_cpu": 256,
            "worker_mem": 512,
            "scheduler_timeout": "15 minutes",
        },
    )
    environment = LocalEnvironment(
        executor=executor,
    )
    storage = S3(bucket="pangeo-forge-cluster-pangeoforgeedstarsstoragece5-18wtdurqlxlsy")
    # Some pipelines take parameters. These are things like subsets of the
    # data to select or where to write the data.
    # See https://docs.prefect.io/core/concepts/parameters.htm for more
    days = Parameter(
        # All parameters have a "name" and should have a default value.
        "days",
        default=pd.date_range("1981-09-01", "1981-09-10", freq="D").strftime("%Y-%m-%d").tolist(),
    )
    cache_location = Parameter(
        "cache_location", default=f"s3://pangeo-forge-cluster-pangeoforgeedstarsscratch82c-1njs2mewbiv9j/cache/{name}.zarr"
    )
    target_location = Parameter("target_location", default=f"s3://pangeo-forge-cluster-pangeoforgeedstarsscratch82c-1njs2mewbiv9j/{name}.zarr")

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
        parameters["cache_location"] = "memory://cache/"
        parameters["target_location"] = "memory://target.zarr"
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
            # be wrapepd in `prefect.unmapped`.
            # https://docs.prefect.io/core/concepts/mapping.html#unmapped-inputs
            # nc_sources will be a list of cached URLs, one per input day.
            nc_sources = download.map(sources, cache_location=unmapped(self.cache_location))

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
            )

            # Consolidate the metadata for the final dataset.
            consolidate_metadata(self.target_location, writes=writes)

        return flow


# pangeo-forge and Prefect require that a `flow` be present at the top-level
# of this module.
flow = Pipeline().flow
flow.register(project_name="edstars")
