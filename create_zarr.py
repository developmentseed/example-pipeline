from datetime import datetime
import h5py
import numpy as np
import os
import xarray as xr

source_dir = 'cache'
target_zarr = 'target.zarr'
files = os.listdir(source_dir)
fields = ['precipitationCal', 'precipitationUncal', 'randomError', 'HQprecipitation', 'HQprecipSource', 'HQobservationTime', 'IRprecipitation', 'IRkalmanFilterWeight', 'probabilityLiquidPrecipitation', 'precipitationQualityIndex']

for idx, file in enumerate(files[3:6]):
    f = h5py.File(f"{source_dir}/{file}", 'r+')
    timestamp = f['Grid']['time'][0]
    ds = xr.Dataset(
        {
            field: (['lon', 'lat'], f[f'Grid/{field}'][0]) for field in fields
        },
        coords={
            'lon': f['Grid/lon'],
            'lat': f['Grid/lat'],
            'time': [datetime.utcfromtimestamp(timestamp)]
        }
        ).transpose()
    if idx == 0:
        ds.to_zarr(target_zarr)
    else:
        # THIS ISN'T GOING TO WORK BECAUSE FILES MAY COME OUT OF ORDER
        ds.to_zarr(target_zarr, append_dim='time')

