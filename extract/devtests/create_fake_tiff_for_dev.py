# -*- coding: utf-8 -*-
"""
Created on Tue May 16 09:36:41 2023

@author: ccrevier
"""

import numpy as np
from rasterio.transform import Affine
import rasterio

def make_fake_data(nb_x:int, nb_y:int):
    """
    Creating fake array with a transform
    Parameters
    ----------
    nb_x : int
        Number of pixel in x
    nb_y : int
        Number of pixel in y

    Returns
    -------
    values : array
        array data
    transform : TYPE
        transform

    """
    x = np.linspace(-4.0, 4.0, nb_x)
    y = np.linspace(-3.0, 3.0, nb_y)
    X, Y = np.meshgrid(x, y)
    Z1 = np.exp(-2 * np.log(2) * ((X - 0.5) ** 2 + (Y - 0.5) ** 2) / 1 ** 2)
    Z2 = np.exp(-3 * np.log(2) * ((X + 0.5) ** 2 + (Y + 0.5) ** 2) / 2.5 ** 2)
    values = 10.0 * (Z2 - Z1)
    res = 1
    transform = Affine.translation(x[0] - res / 2, y[0] - res / 2) * Affine.scale(res, res)
    return values, transform

def create_temp_tif(temp_file_name:str, x:int, y:int):
    """
    Creating a fake tif and retuning the kwargs

    Parameters
    ----------
    temp_file_name : str
        Path and name of destination file.

    Returns
    -------
    kwargs : TYPE
        kwargs.

    """

    values, transform = make_fake_data(x,y)
    print(values.shape[0])
    new_dataset = rasterio.open(
        temp_file_name,
        'w',
        driver='GTiff',
        height=values.shape[0],
        width=values.shape[1],
        count=1,
        dtype=values.dtype,
        crs='EPSG:3979',
        transform=transform,
    )
    new_dataset.write(values, 1)
    kwargs = new_dataset.meta.copy()
    new_dataset.close()
    return kwargs

def handle_it():
    file_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\CHN-issue\MemoryError\input_test_tif\input_15km.tif'
    create_temp_tif(file_path, 15000, 15000)
    
if __name__ == "__main__":
    handle_it()