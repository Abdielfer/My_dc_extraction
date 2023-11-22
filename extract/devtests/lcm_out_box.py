"""
# Inputs
bbox = '1708821, -107045, 1713821, -102045'
bbox_crs = 'EPSG:3979'
out_res = 10
resolutions = (1, 2)

# Out coords that are correct
#out_crs = EPSG:3979
#landcover 
1708800.0000000000000000,-107070.0000000000000000 : 1713810.0000000000000000,-102030.0000000000000000
h, w = 504, 501
#dsm
1708820.0000000000000000,-107060.0000000000000000 : 1713830.0000000000000000,-102040.0000000000000000
h, w = 502, 502

#out_crs = EPSG:2979
#landcover
180440.0000000000000000,5032430.0000000000000000 : 186890.0000000000000000,5038890.0000000000000000
h, w, = 646, 645
#dsm
180440.0000000000000000,5032430.0000000000000000 : 186890.0000000000000000,5038890.0000000000000000
h, w, = 646, 645
"""
import pathlib
import sys

from rasterio import warp
from shapely.geometry import box, mapping, shape

root = pathlib.Path(__file__).parents[2]
if str(root) not in sys.path:
    sys.path.insert(0,str(root))


import ccmeo_datacube.extract as dce

def _handle_it():
    # test_calc_res_origin_valid_inputs()
    test_output_dimension()

def test_params():
    out_res = 10
    bbox = '1708821, -107045, 1713821, -102045'
    bbox_crs = 'EPSG:3979'
    hrdem_ress = [1,2]
    landcover_ress = [30]
    return out_res,bbox,bbox_crs,hrdem_ress,landcover_ress

def reproj_params():
    """
    bd2979
    {'type': 'Polygon', 'coordinates': [[(-2455120.4461391144, 24220071.923787966), (-2454239.5488839312, 24214684.595121652), (-2448852.7914221575, 24215566.634216838), (-2449734.151320432, 24220952.257616095), (-2455120.4461391144, 24220071.923787966)]]}
    
    shape(b2979).bounds
    (-2455120.4461391144, 24214684.595121652, -2448852.7914221575, 24220952.257616095)
    """
    (out_res,
     bbox,
     bbox_crs,
     hrdem_ress,
     landcover_ress) = test_params()
    
    # Shapely polygon 
    bp = box(*tuple([float(b) for b in bbox.split(',')]))

    # WKT style geojson like dict 
    bd = mapping(bp)
    bd2979 = warp.transform_geom(bbox_crs,'EPSG:2979',bd)

    # Convert back to shapely.polygon for bounds
    w,s,e,n = shape(bd2979).bounds
    return w,s,e,n

def test_calc_res_origin_valid_inputs():
    """
    Previous results from code
    ----------------
    EPSG: 3979
    ----------
    res 1,2
    -------
    w : 1708820 # correct it is 1 m 'wester' than original
    n : -102040 # correct it is 5 m 'norther' than original

    res: 30
    -------
    w : 1708800 # corret it is 21 m 'wester' than original
    n : -102030 # correct it is 15 m 'norther' than original

    Print out from test
    -------------------
    orig_w = 1708821.0
    orig_n = -102045.0
    ***
    West for [1, 2, 10] : 1708820.0
    North for [1, 2, 10]: -102040.0
    ***
    West for [30, 10] : 1708800.0
    North for [30, 10]: -102030.0
    ***
    orig_w = -12345
    orig_n = -45677
    ***
    West for [1, 2, 10] : -12350.0
    North for [1, 2, 10]: -45670.0
    ***
    West for [30, 10] : -12360.0
    North for [30, 10]: -45660.0
    ***
    orig_w = -12345
    orig_n = 45677
    ***
    West for [1, 2, 10] : -12350.0
    North for [1, 2, 10]: 45680.0
    ***
    West for [30, 10] : -12360.0
    North for [30, 10]: 45690.0
    ***
    orig_w = 12345
    orig_n = 45677
    ***
    West for [1, 2, 10] : 12340.0
    North for [1, 2, 10]: 45680.0
    ***
    West for [30, 10] : 12330.0
    North for [30, 10]: 45690.0
***
    """

    (out_res,
     bbox,
     bbox_crs,
     hrdem_ress,
     landcover_ress) = test_params()
    
    # Get bbox corners
    orig_w,orig_s,orig_e,orig_n = tuple([float(b) for b in bbox.split(',')])

    # Add output res to input ress
    ress_1_2 = hrdem_ress + [out_res] # hrdem-lidar input
    ress_30 = landcover_ress + [out_res] # landcover input
    # Make a test list of resolutions lists
    ress_tests = [ress_1_2,ress_30]

    for west,north in [(orig_w,orig_n),(-12345,-45677),(-12345,45677),(12345,45677)]:
        print(f'orig_w = {west}')
        print(f'orig_n = {north}')
        for ress_test in ress_tests:
            w,n = dce.calc_res_origin(west,north,ress_test)
            print('***')
            print(f'West for {ress_test} : {w}')
            print(f'North for {ress_test}: {n}')
        print('***')
    print('***')

    return

def test_output_dimension():
    """
    Original input
    --------------
    orig_w = 1708821.0
    orig_n = -102045.0

    Previous results from code
    ----------------
    EPSG:3979
    ---------
    res 1,2
    -------
    w : 1708820 # correct it is 1 m 'wester' than original
    n : -102040 # correct it is 5 m 'norther' than original

    res: 30
    -------
    w : 1708800 # corret it is 21 m 'wester' than original
    n : -102030 # correct it is 15 m 'norther' than original

    EPSG: 2979
    ----------
    res 1,2
    -------
    w : 180440
    n : 5038890

    res: 30
    -------
    w : 180440
    n : 5038890
    
    orig_w = 1708821.0
    orig_n = -102045.0

    Print out from test
    3979 bbox: 1708821, -107045, 1713821, -102045
    2979 bounds: (-2455120.4461391144, 24214684.595121652, -2448852.7914221575, 24220952.257616095)
    Out crs EPSG:3979
    -----------------
    ress after get_output_dimensions: [1, 2]
    w : 1708820.0
    n : -102040.0
    x_distance: 5010.0
    y_distance: 5010.0
    e: 1713830.0
    s: -107050.0
    height: 501
    width: 501
    -------
    ress after get_output_dimensions: [30]
    w : 1708800.0
    n : -102030.0
    x_distance: 5040.0
    y_distance: 5040.0
    e: 1713840.0
    s: -107070.0
    height: 504
    width: 504
    -------
    Out crs EPSG:2979
    -----------------
    ress after get_output_dimensions: [1, 2]
    w : -2455130.0
    n : 24220960.0
    x_distance: 6280.0
    y_distance: 6280.0
    e: -2448850.0
    s: 24214680.0
    height: 628
    width: 628
    -------
    ress after get_output_dimensions: [30]
    w : -2455140.0
    n : 24220980.0
    x_distance: 6300.0
    y_distance: 6300.0
    e: -2448840.0
    s: 24214680.0
    height: 630
    width: 630
    -------
    """
    (out_res,
     bbox,
     bbox_crs,
     hrdem_ress,
     landcover_ress) = test_params()
    
    print(f'3979 bbox: {bbox}')
    print(f'2979 bounds: {reproj_params()}')
    for out_crs in ['EPSG:3979','EPSG:2979']:
        print(f'Out crs {out_crs}')
        print('-----------------')
        for ress in [hrdem_ress,landcover_ress]:
            # print(f'ress before get_output_dimensions: {ress}')
            trans,height,width = dce.get_output_dimension(ress,bbox,bbox_crs,out_crs,out_res)
            print(f'ress after get_output_dimensions: {ress}')
            w = trans.c
            n = trans.f
            r = trans.a
            x_distance = width * r
            y_distance = height * r
            print(f'w : {w}')
            print(f'n : {n}')
            print(f'x_distance: {x_distance}')
            print(f'y_distance: {x_distance}')
            print(f'e: {w + y_distance}')
            print(f's: {n - x_distance}')
            # print(f'transform: {trans}')
            print(f'height: {height}')
            print(f'width: {width}')
            print('-------')

if __name__ == '__main__':
    _handle_it()