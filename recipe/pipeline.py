# There are many comments in this example. Remove them when you've
# finalized your pipeline.
import pandas as pd
import pangeo_forge
import pangeo_forge.utils
from pangeo_forge.tasks.http import download
from pangeo_forge.tasks.xarray import combine_and_write
from pangeo_forge.tasks.zarr import consolidate_metadata
from prefect import Flow, Parameter, task, unmapped

# We use Prefect to manage pipelines. In this pipeline we'll see
# * Tasks: https://docs.prefect.io/core/concepts/tasks.html
# * Flows: https://docs.prefect.io/core/concepts/flows.html
# * Parameters: https://docs.prefect.io/core/concepts/parameters.html

# A Task is one step in your pipeline. The `source_url` takes a day
# like '2020-01-01' and returns the URL of the raw data.

def noaa_sst_avhrr_url_pattern(timespan: str) -> str:
    return (
        "https://www.ncei.noaa.gov/data/"
        "sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/"
        "{timespan:%Y%m}/oisst-avhrr-v02r01.{timespan:%Y%m%d}.nc"
    )

def gesdisc_gpm_imerg_url_pattern(timespan: str) -> str:
    # https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.06/2000/153/3B-HHR.MS.MRG.3IMERG.20000601-S000000-E002959.0000.V06B.HDF5
    hours_pattern = "{timespan:%H}"
    hours = hours_pattern.format(timespan=timespan)
    seconds = int(hours) * 60
    return (
        "https://gpm1.gesdisc.eosdis.nasa.gov/data/"
        "/GPM_L3/GPM_3IMERGHH.06/"
        "{timespan:%Y}/{timespan:%j}/3B-HHR.MS.MRG.3IMERG.{timespan:%Y}{timespan:%m}{timespan:%d}-S000000-"
        # this won't work (seconds)
        "E{timespan:%h}{timespan:%m}{timespan:%s}.{seconds}.V06B.HDF5"        
    )

@task
def source_url(timespan: str) -> str:
    """
    Format the URL for a specific day.
    """
    timespan = pd.Timestamp(timespan)
    source_url_pattern = gesdisc_gpm_imerg_url_pattern(timespan)
    return source_url_pattern.format(timespan=timespan)


# All pipelines in pangeo-forge must inherit from pangeo_forge.AbstractPipeline


class Pipeline(pangeo_forge.AbstractPipeline):
    # You must define a few pieces of metadata in your pipeline.
    # name is the pipeline name, typically the name of the dataset.
    name = "example"
    # repo is the URL of the GitHub repository this will be stored at.
    repo = "pangeo-forge/example-pipeline"

    # Some pipelines take parameters. These are things like subsets of the
    # data to select or where to write the data.
    # See https://docs.prefect.io/core/concepts/parameters.htm for more
    days = Parameter(
        # All parameters have a "name" and should have a default value.
        "days",
        default=pd.date_range("1981-09-01", "1981-09-10", freq="D").strftime("%Y-%m-%d").tolist(),
    )
    half_hours = Paramter(
        "half_hours"
        default=pd.date_range("2000-06-01 00:29:59", "2000-07-01 00:29:59", freq="0.5H").strftime("%Y-%m-%d %H:%M:%S").tolist()
    )
    cache_location = Parameter(
        "cache_location", default=f"pangeo-forge-scratch/cache/{name}.zarr"
    )
    target_location = Parameter("target_location", default=f"pangeo-forge-scratch/{name}.zarr")

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
        return parameters

    # The `Flow` definition is where you assemble your pipeline. We recommend using
    # Prefects Functional API: https://docs.prefect.io/core/concepts/flows.html#functional-api
    # Everything should happen in a `with Flow(...) as flow` block, and a `flow` should be returned.
    @property
    def flow(self):
        with Flow(self.name) as flow:
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
