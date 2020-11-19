from datetime import datetime
import h5py
import numpy as np
import os
import xarray as xr

source_dir = 'cache'
target_zarr = 'target.zarr'
files = [os.path.join(source_dir, f) for f in os.listdir(source_dir)]
ds = xr.open_mfdataset(files, group='Grid')
ds.to_zarr(target_zarr)

