# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 15:55:53 2022

@author: mdaviaul
"""

import time
from pathlib import Path
import math
import ccmeo_datacube.extract as dce
import matplotlib.pyplot as plt



def calculate_limit_wcs_coverage_extract(path_file:str, poly_dic:dict):
    """
    Parameters
    ----------
    path_file : str
        Path of the working directory
    poly_dic : dict
        The extract bounding box as geojson style dictionary.

    Returns
    -------
    tests_results : dict
        Dictionary for time and number of pixel.
    sqrt_pixel_fail : int
        Number of pixel where it has failed.

    """
    tests_results = {}
    sqrt_pixel = None
    sqrt_pixel_fail = None
    path_file = Path(path_file)
    path_file_full = path_file / 'test_sample-wcs-dtm.tif'
    dex = dce.DatacubeExtract()
    for cellsize in range (1000,10,-25):
        time_1 = time.time()
        print('Extraction with cellsize =', cellsize)
        try:
            dex.wcs_coverage_extract(poly_dic,'EPSG:3979',cellsize,'HTTPS','dsm'
                                                ,'stage','elevation', path_file, method='nearest')
            time_2 = time.time()
            delta_t = time_2 - time_1
            print(delta_t)
            if path_file_full.is_file():
                minx,miny,maxx,maxy=poly.bounds
                nb_pixel = dex.calculate_size(maxx-minx,maxy-miny,cellsize)
                sqrt_pixel = math.sqrt(nb_pixel/1000000)
                tests_results[sqrt_pixel] = delta_t
                #tests_results[cellsize] = delta_t
            else:
                print('Extraction failed')
                sqrt_pixel_fail = sqrt_pixel**2
                break
        except:
            print('Extraction failed')
            sqrt_pixel_fail = sqrt_pixel**2
            break
    return tests_results, sqrt_pixel_fail


dex = dce.DatacubeExtract() # creer un object de la classe DatacubeExtract
poly = dex.tbox_to_poly(tbox='-1285959,2601049,371107,1299069')
poly_dic = dex.poly_to_dict(poly)
result, sqrt_pixel_fail = calculate_limit_wcs_coverage_extract(
    'D:/PROJET_DADA_CUBE_2020/spyder/extract/result_test/',poly_dic)


if sqrt_pixel_fail:
    print(f'Maximum number of pixel in million : {sqrt_pixel_fail}')
else:
    print('No error')

keysList = list(result.keys())
values = result.values()
plt.plot(keysList, values)
plt.title('Wcs request')
plt.xlabel('Square root of the Number of pixel (Million)')
#plt.xlabel('Pixel size (m)')
plt.ylabel('Time (sec.)')
plt.grid(True)
plt.show()
