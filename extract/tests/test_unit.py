# -*- coding: utf-8 -*-
"""
Unit Testing module for extract.py

Best pratices :
    utiliser TemporaryDirectory
    regroupper les parametres dans un dictionnaire **params
    Contantes = MAJ
    variable = minuscule


"""
# Python standard library
import json
import os
# import re
import sys
from tempfile import TemporaryDirectory, TemporaryFile, NamedTemporaryFile
import unittest
from unittest.mock import MagicMock, Mock, patch
import pathlib
# from pathlib import Path

# Custom packages
import geopandas as gpd
import numpy as np
import pandas
from pandas.testing import assert_frame_equal
import pytest
import rasterio
from rasterio.io import DatasetReader
from rasterio.transform import Affine
import shapely
from shapely.geometry import Polygon

# Datacube custom packages
_CHILD_LEVEL = 2
DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
if DIR_NEEDED not in sys.path:
    sys.path.append(DIR_NEEDED)
import ccmeo_datacube.extract as dce


# setup
DEX = dce.DatacubeExtract()
BBOX ='-1862977,312787,-1200000,542787' #EPSG:3979

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
    res = (x[-1] - x[0]) / 240.0
    transform = Affine.translation(x[0] - res / 2, y[0] - res / 2) * Affine.scale(res, res)
    return values, transform

def create_temp_tif(temp_file_name:str):
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

    values, transform = make_fake_data(240,180)
    print(values.shape[0])
    new_dataset = rasterio.open(
        temp_file_name,
        'w',
        driver='GTiff',
        height=values.shape[0],
        width=values.shape[1],
        count=1,
        dtype=values.dtype,
        crs='+proj=latlong',
        transform=transform,
    )
    new_dataset.write(values, 1)
    kwargs = new_dataset.meta.copy()
    new_dataset.close()
    return kwargs


@pytest.fixture
def value_list():
    res = [('img_2020_2m','2020:04:29 12:00:00', 2.0,'c1a1','c1'),
           ('img_2022_2m','2022:04:29 12:00:00', 2.0,'c1a2','c1'),
           ('img_2008_5m','2008:12:04 12:00:00', 5.0,'c1a3','c1'),
           ('img_2022_1m', '2022:01:29 12:00:00', 1.0,'c2a1','c2')]
    yield res

@pytest.fixture
def df_res(value_list):
    # Resolution DataFrame
    yield pandas.DataFrame(data=value_list,columns=['url','item_datetime','item_resolution','asset_key','collection_id'])

@pytest.fixture
def df_invalid_fields(value_list):
    # Invalid field name DataFrame
    yield pandas.DataFrame(data=value_list,columns=['toto1','toto2','toto3','toto4','toto5'])

@pytest.fixture
def df_invalid_datatype(value_list):
    # Invalid datatype DataFrame
    yield pandas.DataFrame(data=value_list,columns=['item_datetime','item_resolution','asset_key','collection_id','url'])

@pytest.fixture
def bbox():
    yield '-75,45,-74,46'
@pytest.fixture
def bbox_crs():
    yield 'EPSG:4326'

@pytest.fixture
def patch_asset_url_singlepage():
    """
    Single page return from STAC API search

    collection_id: flood-susceptibility
    assets: class, index
    No links
    1 items, 2 assets returned
    """
    result = MagicMock()
    rf = pathlib.Path(__file__).parent/'data/stac_api_search_page1of1.json'
    with rf.open() as rp:
        result.json.return_value=json.load(rp)
    rp.close()
    result.status_code = 200
    with patch('ccmeo_datacube.extract._search_pages') as pages_patch:
        pages_patch.return_value = ['page1url']
        with patch('ccmeo_datacube.extract.requests') as requests_patch:
            requests_patch.get.return_value = result
            requests_patch.post.return_value = result
            yield pages_patch,requests_patch

@pytest.fixture
def patch_asset_url_multipage():
    """
    Two pages of returns.
     First one is through post and includes a next link
     Second one is through get and has no links

    collection_id: hrdem-lidar
    Seven assets from 3 different roles
     data: dtm, dsm
     metadata: dtm-vrt, dsm-vrt, coverage, extent
     thumbnail: thumbnail

     33 items returned 
     66 data assets returned
    
    """
    result1 = MagicMock()
    result2 = MagicMock()
    rf1 = pathlib.Path(__file__).parent/'data/stac_api_search_page1of2.json'
    with rf1.open() as rp1:
        result1.json.return_value = json.load(rp1)
    result1.status_code = 200
    rf2 = pathlib.Path(__file__).parent/'data/stac_api_search_page2of2.json'
    with rf2.open() as rp2:
        result2.json.return_value = json.load(rp2)
    result2.status_code = 200

    with patch('ccmeo_datacube.extract._search_pages') as pages_patch:
        pages_patch.return_value = ['page1url','page2url']
        with patch('ccmeo_datacube.extract.requests') as requests_patch:
            
            # All asset_url calls are posts first page has next link
            requests_patch.post.side_effect = [result1,result2]

            yield pages_patch,requests_patch

def test_append_to_file(tmp_path):
    """
    Test function append_to_file
    """
    print('Preparation')
    with TemporaryDirectory() as temp_dir:
        content = ['test 1-2, 10/4', 'this is a test for line 2']
        temp_file_name  = os.path.join(temp_dir, 'toto.txt')
        print('Execution')
        result = DEX.append_to_file(content,temp_file_name)
        print('Validation')
        result = [line.rstrip() for line in open(temp_file_name)]
        assert result == content, f"Message It's ({result}) should be­ ({content}) "


def test_bbox_to_poly():
    """
    Test function bbox_to_poly.

    """
    poly = DEX.bbox_to_poly(BBOX)
    result = 'POLYGON ((-1200000 312787, -1200000 542787, -1862977 542787,'\
            ' -1862977 312787, -1200000 312787))'
    assert str(poly) == result, f"The result polygon is not what was expected."\
            " It's ({result}) should be­ ({poly})"
    assert type(poly) == shapely.geometry.polygon.Polygon, "Result geometry is not a polygon"

def test_bbox_to_tuple():
    """
    test for function bbox_to_tuple()
    """
    print('Validation')
    result = DEX.bbox_to_tuple(BBOX)
    print('Execution')
    prediction = (-1862977.0, 312787.0, -1200000.0, 542787.0)
    assert result == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "

def test_calculate_file_size():
    """
    test function calculate_file_size
    """
    print('Preparation')
    params = {'dtype' :'float32',
              'width':1048576,
              'height':1024}
    print('Execution')
    result = DEX.calculate_file_size(**params)
    print('Validation')
    prediction = 4.0
    assert result == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


def test_calculate_size():
    """
    Test function calculate_size
    """
    print('Preparation')
    deltax = 8000
    deltay = 90000
    cellsize = 20
    print('Execution')
    result = DEX.calculate_size(deltax, deltay, cellsize)
    print('Validation')
    prediction = 1800000
    assert result == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


def test_clip_window():
    """
    Test function clip_window
    """
    win = rasterio.windows.Window(col_off=240640, row_off=291328, width=512, height=512)
    clip = rasterio.windows.Window(col_off=240243, row_off=291098, width=2001, height=2001)
    print('Preparation')
    params={'win':win,
            'clip_to':clip}
    print('Execution')
    result = dce.clip_window(**params)
    print('Validation')
    prediction = rasterio.windows.Window(col_off=240640, row_off=291328, width=512, height=512)
    assert result == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


class TestCheckDateTime():
    """
    class to test function check_date_time
    """


    def test_check_date_time_format(self):
        """
        Test function check_date_time is in the right format
        """
        print('Preparation')
        date_time = '2015:07:01 12:00:00'
        params = {'tifftag_datetime':date_time}
        print('Execution')
        try:
            DEX.check_date_time(**params)
        except:
            raise Exception('check date time format function failed')
        print('Validation')


    def test_check_date_time_old(self):
        """
        Test function check_date_time raise error if it is older than 1900
        """
        print('Preparation')
        params = {'tifftag_datetime':'1899:07:01 12:00:00'}
        print('Execution')
        with pytest.raises(ValueError, match="Date should be after 1900"):
            DEX.check_date_time(**params)
        print('Validation')


    def test_check_date_time_futur(self):
        """
        Test function check_date_time raise error if date time is in the futur
        """
        print('Preparation')
        params = {'tifftag_datetime':'2300:07:01 12:00:00'}
        #Not datetime.now + timedelta(days=1) because check_date_time needs string input of format '%Y:%m:%d %H:%M:%S'
        print('Execution')
        with pytest.raises(ValueError, match="Date should be before tomorrow"):
            DEX.check_date_time(**params)
        print('Validation')
    # def test_check_date_time_none(self):
    #     """
    #     Test function check_date_time if no TIFFTAG_DATETIME is passed
    #     """
    #     print('Preparation')
    #     params={'tifftag_datetime':None}
    #     print('Execution')
    #     with pytest.raises(ValueError, match= "Datetime is None"):
    #         DEX.check_date_time(**params)
    #     print('Validation')


def test_check_outfile(tmpdir):
    """
    Test function check_outfile
    """
    print('Preparation')
    filename = tmpdir.join('toto.tif')
    try:
        DEX.check_outpath(filename)
    except:
        raise Exception('check_outfile function failed')


def test_check_outpath():
    """
    Test function check_outpath
    """
    try:
        with TemporaryDirectory() as d:
            temp_file_name = os.path.join(d, 'test/toto')
            DEX.check_outpath(temp_file_name)
    except:
        raise Exception('check_outpath function failed')
        
        
def test_collection_str_to_df():
    """
    Test function create_dictonnary
    """
    print('Preparation')
    collection = 'toto:tata,cdem,cdem:dtm,    hrdem:dsm'
    print('Execution')
    result = DEX.collection_str_to_df(collection)

    print('Validation')
    prediction = pandas.DataFrame(
        {'collection': ['toto', 'cdem', 'cdem', 'hrdem'],
          'asset': ['tata', None, 'dtm', 'dsm']
          })
    assert_frame_equal(prediction, result), f"Message It's ({result}) should be­ ({prediction}) "
        
        
def test_collection_str_to_dict():
    """
    Test function create_dictonnary
    """
    print('Preparation')
    collection = 'toto:tata,cdem,cdem:dtm,    hrdem:dsm'
    print('Execution')
    result = DEX.collection_str_to_dict(collection)

    print('Validation')
    prediction = {0: {'collection': 'toto', 'asset': 'tata'},\
                  1: {'collection': 'cdem', 'asset': None},\
                  2: {'collection': 'cdem', 'asset': 'dtm'},\
                  3: {'collection': 'hrdem', 'asset': 'dsm'}}
    assert result == prediction, f"Message It's ({result}) should be­ ({prediction}) "


def test_copy_cog():
    """
    Test function copy_cog
    """
    try:
        print('Preparation')
        with TemporaryDirectory() as d:
            temp_file_name = os.path.join(d, 'toto.tif')
            temp_file_copy = os.path.join(d, 'tata.tif')
            kwargs = create_temp_tif(temp_file_name)

            print('Execution')
            DEX.copy_cog(temp_file_name, temp_file_copy, **kwargs)
    except:
        raise Exception('copy cog function failed')


def test_dict_to_poly():
    """
    Test function dict_to_poly
    """
    print('Preparation')
    dicti = {'type': 'Polygon', 'coordinates': (((371107.0, 2601049.0),\
         (371107.0, 1299069.0), (-1285959.0, 1299069.0),\
             (-1285959.0, 2601049.0), (371107.0, 2601049.0)),)}
    print('Execution')
    result = DEX.dict_to_poly(dicti)

    print('Validation')
    poly = ([371107, 2601049], [371107, 1299069], [-1285959, 1299069],
            [-1285959, 2601049], [371107, 2601049])
    prediction = str(Polygon(poly))
    assert str(result) == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


def test_dict_to_vector_file(tmp_path):
    """
    test function dict_to_vector_file()
    """
    print('Preparation')
    file_name = 'toto.geojson'
    params = {'dict_poly':{'type': 'Polygon',
                            'coordinates': (((-265875.0, 90015.0),
                              (-265875.0, 95895.0),
                              (-269985.0, 95895.0),
                              (-269985.0, 90015.0),
                              (-265875.0, 90015.0)),)},
              'crs':'EPSG:3979',
              'outpath':tmp_path,
              'filename':file_name,
              'driver':'GeoJSON'}
    print('Execution')
    result = DEX.dict_to_vector_file(**params)
    print('Validation')
    gdf = gpd.read_file(os.path.join(tmp_path, file_name))
    assert gdf.is_valid[0], "Message : Error in function dict_to_vector_file"


def test_get_cells():
    """
    Test function get_cell.

    """
    print('Preparation')
    delta = 20
    cellsize = 9
    print('Execution')
    result = DEX.get_cells(delta, cellsize)
    print('Validation')
    prediction = 11
    assert result == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


class TestGetRootDomain():
    """
    class to test function get_root_domain
    """


    def test_get_root_domain_beta(self):
        """
        Test function get_root_domain if level is beta
        """
        print('Preparation')
        params = {'level':'beta'}
        print('Execution')
        result = DEX.get_root_domain(**params)
        print('Validation')
        prediction = 'beta.datacube-stage.services.geo.ca/ows/'
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "


    def test_get_root_domain_prod(self):
        """
        Test function get_root_domain if level is prod
        """
        print('Preparation')
        params={'level':'prod'}
        print('Execution')
        result = DEX.get_root_domain(**params)
        print('Validation')
        prediction = 'datacube.services.geo.ca/ows'
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "


    def test_get_root_domain_default(self):
        """
        Test function get_root_domain if level is default (stage)
        """
        print('Preparation')
        params={'level':'stage'}
        print('Execution')
        result = DEX.get_root_domain(**params)
        print('Validation')
        prediction = 'datacube-stage.services.geo.ca/ows'
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "


    def test_get_root_domain_other(self):
        """
        Test function get_root_domain if level is other than valid value (beta, dev, prod, stage)
        """
        print('Preparation')
        params={'level':'toto'}
        print('Execution')
        result = DEX.get_root_domain(**params)
        print('Validation')
        prediction = 'datacube-stage.services.geo.ca/ows'
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "


class TestOverviewLevel():
    """
    class to test function overview_level
    """
    def test_overview_level_with_overview(self):
        """
        Test function overview_level if overviews are needed
        """
        print('Preparation')
        params={'rows':1000000,
                'columns':1000000,
                'blocksize':512}
        print('Execution')
        result = DEX.overview_level(**params)
        print('Validation')
        prediction = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]
        assert isinstance(result, list),\
            f"Message It's ({type(result)}) should be­ ({type(prediction)}) "
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "
    def test_overview_level_no_overview(self):
        """
        Test function overview_level if overviews are not needed
        """
        print('Preparation')
        params={'rows':10,
                'columns':10,
                'blocksize':512}
        print('Execution')
        result = DEX.overview_level(**params)
        print('Validation')
        prediction = None
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "

def test_poly_to_dict():
    """
    Test function poly_to_dict
    """
    print('Preparation')
    poly = ([371107, 2601049], [371107, 1299069], [-1285959, 1299069],
            [-1285959, 2601049], [371107, 2601049])
    shaply_poly = Polygon(poly)

    print('Execution')
    result = DEX.poly_to_dict(shaply_poly)

    print('Validation')
    prediction = "{'type': 'Polygon', 'coordinates': (((371107.0, 2601049.0),"\
        " (371107.0, 1299069.0), (-1285959.0, 1299069.0),"\
            " (-1285959.0, 2601049.0), (371107.0, 2601049.0)),)}"
    assert str(result) == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


class TestResampleValue():
    """
    class to test function resample_value
    """
    def test_resample_value_bad(self):
        """
        test function resample value if the resample name is not standard return bilinear value of 1
        """
        print('Preparation')
        params = {'resample':'bad_method'}
        print('Execution')
        result = dce.resample_value(**params)
        print('Validation')
        prediction = 1
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "


    def test_resample_value_good(self):
        """
        Test function resample_value is the resample name is standard return value
        """
        for r in rasterio.enums.Resampling:
            print('Preparation')
            params={'resample':r.name}
            print('Execution')
            result = dce.resample_value(**params)
            print('Validation')
            prediction = r.value
            assert result == prediction, \
                f"Message It's ({result}) should be­ ({prediction}) "


def test_tap_params():
    """
    test function tap_params()
    """
    print('Preparation')
    params = {'in_crs':'EPSG:3979',
              'out_crs':'EPSG:2960',
              'in_left':2150469.0,
              'in_bottom':144975.0,
              'in_right':2200470.0,
              'in_top':194976.0,
              'in_width':50001,
              'in_height':50001,
              'out_res':20}
    print('Execution')
    (result_transform,
      result_width,
      result_height) = dce.tap_params(**params)
    print('Validation')
    prediction_transform = Affine(20.0, 0.0, 678640.0,
                                  0.0, -20.0, 5135940.0)
    prediction_height = 3260
    prediction_width = 3260

    assert result_transform == prediction_transform,\
        f"Message It's ({result_transform}) should be­ ({prediction_transform}) "
    assert result_height == prediction_height,\
        f"Message It's ({result_height}) should be­ ({prediction_height}) "
    assert result_width == prediction_width,\
        f"Message It's ({result_width}) should be­ ({prediction_width}) "

class TestTapWindow():

    def test_tap_window_same_crs(self):
        '''
        test function tap_window if bbox crs is the same as img_crs
        '''
        print('Preparation')
        bbox_not_tap = '2150469.4724999964, 144975.05299999937,'\
            ' 2152469.4724999964, 146975.05299999937' #2x2 km
        bbox_crs = 'EPSG:3979'
        transform = rasterio.Affine(1.0, 0.0, 1910226.0,
               0.0, -1.0, 438074.0)
        params = {'img_transform': transform,
                  'bbox':bbox_not_tap,
                  'bbox_crs':bbox_crs,
                  'img_crs': 'EPSG:3979',
                  'add':1}
        print('Execution')
        result = dce.tap_window(**params)
        print('Validation')
        prediction = rasterio.windows.Window(col_off=240243, row_off=291098, width=2001, height=2001)
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "

    def test_tap_window_diff_crs(self):
        """
        test function tap_window if bbox crs is different from img_crs
        """
        print('Preparation')
        bbox_not_tap = '-66.69506460983133, 45.93939632920388,'\
            ' -66.66111267835966, 45.96307900445787' #2x2 km
        bbox_crs = 'EPSG:4326'
        transform = rasterio.Affine(1.0, 0.0, 1910226.0,
               0.0, -1.0, 438074.0)
        params = {'img_transform': transform,
                  'bbox':bbox_not_tap,
                  'bbox_crs':bbox_crs,
                  'img_crs': 'EPSG:3979',
                  'add':1}
        print('Execution')
        result = dce.tap_window(**params)
        print('Validation')
        prediction = rasterio.windows.Window(col_off=239466, row_off=290321, width=3556, height=3556)
        assert result == prediction,\
            f"Message It's ({result}) should be­ ({prediction}) "


def test_transform_dict_to_dict():
    """
    test functon transform_dict_to_dict
    """
    print('Preparation')
    params={'bbox_dict': {'type': 'Polygon', 'coordinates': (((371107.0, 2601049.0),\
                            (371107.0, 1299069.0), (-1285959.0, 1299069.0),\
                            (-1285959.0, 2601049.0), (371107.0, 2601049.0)),)},
            'in_crs':'EPSG:2960',
            'out_crs':'EPSG:3979'}
    print('Execution')
    result = DEX.transform_dict_to_dict(**params)
    print('Validation')
    prediction_type = 'Polygon'
    prediction_coord = "[(2919944, -2471297), "\
                         "(3553521, -3986562), "\
                         "(1449716, -4644079), "\
                         "(1080599, -3095796), "\
                         "(2919944, -2471297)]"
    # prediction = "{'type': 'Polygon', 'coordinates':"\
    #     " [[(2919944.497790946, -2471297.312822262), (3553521.9645528803,"\
    #     " -3986562.439553468), (1449716.2224180573, -4644079.371128343),"\
    #     " (1080599.6568144409, -3095796.7038175096), (2919944.497790946, -2471297.312822262)]]}"
    formatted_result = [(int(x), int(y)) for x, y in result['coordinates'][0]]
    assert str(formatted_result) == prediction_coord,\
        f"Message It's ({formatted_result}) should be­ ({prediction_coord}) "
    assert str(result['type']) == prediction_type,\
        f"Message It's ({result['type']}) should be­ ({prediction_type}) "


def test_transform_dict_to_poly():
    """
    test functon transform_dict_to_poly
    """
    print('Preparation')
    params={'bbox_dict': {'type': 'Polygon', 'coordinates': (((371107.0, 2601049.0),\
                            (371107.0, 1299069.0), (-1285959.0, 1299069.0),\
                            (-1285959.0, 2601049.0), (371107.0, 2601049.0)),)},
            'in_crs':'EPSG:2960',
            'out_crs':'EPSG:3979'}
    print('Execution')
    result = DEX.transform_dict_to_poly(**params)
    print('Validation')
    # prediction = "POLYGON ((2919944.497790946 -2471297.312822262,"\
    #     " 3553521.96455288 -3986562.439553468, 1449716.222418057 -4644079.371128343,"\
    #     " 1080599.656814441 -3095796.70381751, 2919944.497790946 -2471297.312822262))"
    prediction = "POLYGON ((2919944 -2471297, "\
            "3553522 -3986562, 1449716 -4644079, "\
            "1080600 -3095797, 2919944 -2471297))"
    assert str(shapely.wkt.loads(shapely.wkt.dumps(result, rounding_precision=0))) == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


def test_tuple_to_bbox():
    """
    test function tuple_to_bbox()
    """
    print('Preparation')
    # params={'bounds_tuple':(-1862977.0, 312787.0, -1200000.0, 542787.0)}
    print('Execution')
    result = DEX.tuple_to_bbox((-1862977.0, 312787.0, -1200000.0, 542787.0))
    print('Validation')
    prediction = '-1862977.0,312787.0,-1200000.0,542787.0'
    assert result == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "


def test_update_profile():
    """
    test function update_profile()
    """
    print('Preparation')
    params = {'in_profile':{'driver': 'GTiff', 'dtype': 'float32',
                            'nodata': -32767.0, 'width': 452261, 'height': 463663,
                            'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979),
                            'transform': Affine(1.0, 0.0, 1910226.0, 0.0, -1.0, 438074.0),
                            'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                            'compress': 'lzw', 'interleave': 'band'},
              'new_crs':rasterio.crs.CRS.from_epsg(2960),
              'new_height':1000,
              'new_width':1000,
              'new_transform':Affine(40.0, 0.0, 1000.0, 0.0, -40.0, 1000.0),
              'new_blocksize':256}
    print('Execution')
    result = dce.update_profile(**params)
    print('Validation')
    prediction = {'driver': 'GTiff', 'dtype': 'float32',
                  'nodata': -32767.0, 'width': 1000, 'height': 1000,
                  'count': 1, 'crs': rasterio.crs.CRS.from_epsg(2960),
                  'transform': Affine(40.0, 0.0, 1000.0, 0.0, -40.0, 1000.0),
                  'blockxsize': 256, 'blockysize': 256, 'tiled': True,
                  'compress': 'lzw', 'interleave': 'band'}
    assert result == prediction,\
        f"Message It's ({result}) should be­ ({prediction}) "

class TestValidRFC3339():
    """All the tests for valid_rfc3339"""
    def test_valid_rfc3339_invalid_time_a(self):
        invalid_date1 = '2021-12-02  00:00:00'
        assert not dce.valid_rfc3339(invalid_date1)

    def test_valid_rfc3339_invalid_time_b(self):
        invalid_date2 = '2021-12-02T00:00:00 Z'
        assert not dce.valid_rfc3339(invalid_date2)

    def test_valid_rfc3339_valid_time_zone_behind(self):
        valid_date1a = '2021-12-02T23:00:00-05:00' # add 5hrs to get UTC
        assert dce.valid_rfc3339(valid_date1a) == '2021-12-03T04:00:00Z'
        
    def test_valid_rfc3339_valid_time_zone_ahead(self):
        valid_date1b = '2021-12-02T23:00:00+05:00' # subtract 5 hrs to get UTC
        assert dce.valid_rfc3339(valid_date1b) == '2021-12-02T18:00:00Z'

    def test_valid_rfc3339_valid_time_hours_included(self):
        valid_date2a = '2021-12-02T05:00:00Z'
        assert dce.valid_rfc3339(valid_date2a) == '2021-12-02T05:00:00Z'

    def test_valid_rfc3339_valid_time_zero_hours(self):
        valid_date2b = '2016-12-02T00:00:00Z'
        assert dce.valid_rfc3339(valid_date2b) == '2016-12-02T00:00:00Z'

    def test_valid_rfc3339_valid_date_only(self):
        valid_date3 = '2021-12-02'
        assert dce.valid_rfc3339(valid_date3) == valid_date3 + 'T00:00:00Z'

    def test_valid_rfc3339_open_range_time_zone_to(self):
        valid_date1b = '2021-12-02T23:00:00+05:00' # subtract 5 hrs to get UTC
        valid_range1 = f'../{valid_date1b}'
        assert dce.valid_rfc3339(valid_range1) == '../2021-12-02T18:00:00Z'

    def test_valid_rfc3339_open_range_time_zone_from(self):
        valid_date1b = '2021-12-02T23:00:00+05:00' # subtract 5 hrs to get UTC
        valid_range2 = f'{valid_date1b}/..'
        assert dce.valid_rfc3339(valid_range2) == '2021-12-02T18:00:00Z/..'

    def test_valid_rfc3339_open_range_date_only_from(self):
        valid_range3 = '2016-12-02/..'
        assert dce.valid_rfc3339(valid_range3) == '2016-12-02T00:00:00Z/..'

class TestOrderBy():
    """Testcases for extract.order_by

    Expectations writtent into code and passed to DataFrame.sort:
    Collection_id is always ASC
    Date or resolution is orderd by desc parameter value
    When resolution, date is always DESC
    """

    def test_invalid_dataframe_fields(self,df_invalid_fields):
        """Should return None"""
        df,urls = dce.order_by(df_invalid_fields)
        assert not urls

    def test_invalid_dataframe_datatypes(self,df_invalid_fields):
        """Should return None"""
        df,urls = dce.order_by(df_invalid_fields)
        assert not urls

    def test_invalid_method_field(self,df_res):
        """Test order by field that is invalid"""
        df,urls = dce.order_by(df_res,'toto')
        assert not urls

    def test_invalid_desc_field(self,df_res):
        """Test order by field that is invalid"""
        df,urls = dce.order_by(df_res,'datetime','Yes')
        assert not urls
    
    def test_resolution_already_exists(self,df_res):
        df,urls = dce.order_by(df_res,'resolution',False)
        print(f'Checking dataframe has values in resolution column {df[["collection_id","item_resolution"]]}')
        assert urls


class TestAssetURLs():
    """
    Using patched requests package _search pages function 
    to validate parsing by _scrape_results and params and logic of asset_url

    """

    def test_single_page(self,patch_asset_url_singlepage,bbox,bbox_crs):
        """Test return of both data asset types"""
        df_collections = pandas.DataFrame({'collection': ['flood-susceptibility'], 'asset': [None]})
        df = dce.asset_urls(df_collections,bbox_crs,bbox)
        df['file'] = df['url'].str[-45:]
        print(df.file)
        print(df.asset_key)
        assert len(df) == 2

    def test_single_page_key(self,patch_asset_url_singlepage,bbox,bbox_crs):
        """Test one of two data asset types returned"""
        df_collections = pandas.DataFrame({'collection': ['flood-susceptibility'], 'asset': ['class']})
        df = dce.asset_urls(df_collections,bbox_crs,bbox)
        df['file'] = df['url'].str[-45:]
        print(df.file)
        print(df.asset_key)
        assert len(df) == 1
        assert len(df.asset_key.unique()) == 1
        assert df.asset_key.unique()[0] == 'class'

    def test_multipage(self,patch_asset_url_multipage,bbox,bbox_crs):
        """Test return of both data asset types multi-page"""
        df_collections = pandas.DataFrame({'collection': ['hrdem-lidar'], 'asset': [None]})
        df = dce.asset_urls(df_collections,bbox_crs,bbox)
        df['file'] = df['url'].str[-45:]
        print(df.file)
        # print(df.asset_key)
        assert len(df) == 66

    def test_multipage_key(self,patch_asset_url_multipage,bbox,bbox_crs):
        """Test one of two data asset types returned multi-page"""
        df_collections = pandas.DataFrame({'collection': ['hrdem-lidar'], 'asset': ['dtm']})
        df = dce.asset_urls(df_collections,bbox_crs,bbox)
        df['file'] = df['url'].str[-45:]
        print(df.file)
        # print(df.asset_key)
        assert len(df) == 33
        assert len(df.asset_key.unique()) == 1
        assert df.asset_key.unique()[0] == 'dtm'
        