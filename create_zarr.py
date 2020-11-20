import os
import xarray as xr

source_dir = 'cache'
target_zarr = 'target.zarr'
files = [os.path.join(source_dir, f) for f in os.listdir(source_dir)]
ds = xr.open_mfdataset(files, group='Grid', combine="by_coords", concat_dim='time')
ds.to_zarr(target_zarr)

dsz = xr.open_zarr(target_zarr)
print(dsz['time'])