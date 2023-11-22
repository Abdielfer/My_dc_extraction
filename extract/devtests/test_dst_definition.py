import math
import pathlib
import sys
from typing import Tuple

root = pathlib.Path(__file__).parents[2]
if str(root) not in sys.path:
    sys.path.insert(0,str(root))

import ccmeo_datacube.extract as dce

@dce.win_ssl_patch
def test_dst_definition():
    """Testing how to resolve problem

    Resources
    ---------
    # Merge does not use 1/2 pixel resolution in transform calc, quickstart does ... hmm...
    https://github.com/rasterio/rasterio/blob/main/rasterio/merge.py
    https://rasterio.readthedocs.io/en/stable/quickstart.html#dataset-georeferencing
    """
    dst_res = 4
    clip_bbox = '1708821, -107045, 1713821, -102045'
    clip_bbox_crs = 'EPSG:3979'
    files = ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
        'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']
    xs = []
    ys = []
    # Calculate clip_box transform
    clip_w,clip_s,clip_e,clip_n = tuple([float(x) for x in clip_bbox.split(',')])
    print("clip box bounds in crs")
    print(clip_w,clip_s,clip_e,clip_n)
    print("out image transform")
    clip_transform = dce.rasterio.Affine.translation(clip_w, clip_n) * dce.rasterio.Affine.scale(dst_res, -dst_res)
    print(clip_transform)
    print('Clip tap window')
    print(dce.tap_window(clip_transform,clip_bbox,clip_bbox_crs,'EPSG:3979'))
    print('***')

    for file in files:
        with dce.rasterio.Env():
            with dce.rasterio.open(file) as c:
                print(c.name)
                print(c.transform)
                print(dce.tap_window(c.transform,clip_bbox,clip_bbox_crs,'EPSG:3979'))
                print('***')
                # Getting bounds of output image
                left, bottom, right, top = c.bounds
            xs.extend([left, right])
            ys.extend([bottom, top])
    dst_w, dst_s, dst_e, dst_n = min(xs), min(ys), max(xs), max(ys)
    print("out image bounds in crs")
    print(dst_w,dst_s,dst_e,dst_n)
    print("out image transform")
    dst_transform = (dce.rasterio.Affine.translation(dst_w, dst_n) 
                     * dce.rasterio.Affine.scale(dst_res, -dst_res))
    print(dst_transform)
    print('Out image tap window')
    print(dce.tap_window(dst_transform,clip_bbox,clip_bbox_crs,'EPSG:3979'))


    # Out_bbox doit ....
    """
    tap_window()
    output_profile for output cog from extraction (result from get_extract_params())

    J'essaie de trouver une manière de définir 
    le output height/width/transform qui n'est pas dépendant de 
    l'image en input (input res or crs), parce que pour le moment, 
    le output profile created is dependent and that creates difference 
    between output extent when doing normal extraction and 
    errors when doing mosaic extraction with 2m resolution on top. 
    J'aimerais que ça soit juste dépendant des out_res et crs define by user.
    
    """
    # dce.tap_window(img_transform,bbox,bbox_crs,img_crs,add)
    # dce.get_extract_params(in_path,bbox,bbox_crs,out_crs,out_res,resampling)

def example_code():
    """Example de Charlotte"""
    bbox = '1708821, -107045, 1713821, -102045'
    bbox_crs = 'EPSG:3979'

    files = ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
        'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']
	
    out_crs='EPSG:3979'
    resolution=4
    method=None

    list_params=[]
    list_profile=[]
    for file in files:
        profile, params = dce.get_extract_params(in_path=file,
                                                out_crs=out_crs,
                                                out_res=resolution,
                                                bbox=bbox,
                                                bbox_crs=bbox_crs,
                                                resampling_method=method)
        list_params.append(params)
        list_profile.append(profile)

    print(list_profile)
    #list_profile[0] != list_profile[1] for width and height, but the goal is that they would be
    return


def calc_res_origin(orig_w:float,orig_n:float,ress:list)->Tuple[float,float]:
    """
    Calculates the resolutions aligned North West corner.
    
    Parameters
    ----------
    orig_w: float
        The bbox / clip box north west corner, western value.
    orig_n: float
        The bbox / clip box north west corner, northern value
    ress: list
        The list of all input and the output resolution
    
    Returns
    -------
    west: float
        The resolution aligned western value for the north west corner.
    north: float
        The resolution aligned northern value for the north west corner.
    """
    lcm = math.lcm(*tuple(ress))
    west = ((math.floor(orig_w / lcm)) * lcm) + lcm
    north = ((math.floor(orig_w / lcm)) * lcm) + lcm
    return west,north

if __name__ == '__main__':
    test_dst_definition()