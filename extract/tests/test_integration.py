# -*- coding: utf-8 -*-
"""
Integration Testing module for extract.py
"""
# Python standard library
from datetime import datetime
import json
import math
import os
import pathlib
import re
import sys
from tempfile import TemporaryDirectory, NamedTemporaryFile
import unittest
from unittest.mock import MagicMock, patch

# Python custom packages
import numpy as np
import pandas
import pytest
import rasterio
from rasterio.transform import Affine
from shapely.geometry import Polygon

# local packages
_CHILD_LEVEL = 2
DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
sys.path.append(DIR_NEEDED)
import ccmeo_datacube.extract as dce
import extract.tests.test_unit as test_unit

#setup
DEX = dce.DatacubeExtract()
BBOX ='-1862977,312787,-1200000,542787'


def raster_temp_def():
    """
    Definition of a fake raster used as input for tests
    """
    values = test_unit.make_fake_data(2500, 2500)[0]
    res = 1
    ori_x = 2150469
    ori_y = 144975
    transform = rasterio.transform.from_origin(ori_x, ori_y, res, res)
    kwargs = {'driver':'GTiff',
              'height':values.shape[0],
              'width':values.shape[1],
              'count':1,
              'dtype':'float64',
              'crs':rasterio.crs.CRS().from_epsg(3979),
              'transform':transform,
              'tiled':True,
              'blockxsize':512,
              'blockysize':512,
              'compress':'lzw',
              'nodata': -32767.0,
              'interleave': 'band'}
    # new_dataset = rasterio.open(temp_file_name, 'w', **kwargs)

    return kwargs, values

@pytest.fixture
def bbox():
    yield '-75,45,-74,46'

@pytest.fixture
def bbox_crs():
    yield 'EPSG:4326'

##Test
def test_add_overviews():
    """
    Test function add_overviews
    """
    print('Preparation')
    with TemporaryDirectory() as temp_dir:
        temp_file_name  = os.path.join(temp_dir, 'toto.txt')
        test_unit.create_temp_tif(temp_file_name)
        dst=rasterio.open(temp_file_name,'r+')

        print('Execution')
        DEX.add_overviews(dst, blocksize=32)

        print('Validation')
        result_overviews = [dst.overviews(i) for i in dst.indexes]
        dst.close()
        prediction_overviews = [[2, 4, 8]]
        assert result_overviews == prediction_overviews,\
            f"Overviews are ({result_overviews}) should be­ ({prediction_overviews}) "


class TestBboxWindows():
    """
    class to test function bbox_windows
    """

    bbox = '2150520.4724999964, 142500.05299999937,'\
         ' 2151530.4724999964, 143885.05299999937'
    bbox_crs = 'EPSG:3979'

    def test_bbox_windows_clipped(self):
        """
        Test for function bbox_window if clip=True
        """
        print('Preparation')
        with NamedTemporaryFile(suffix='.tif', delete=False) as fp :
            kwargs, values = raster_temp_def()
            new_dataset = rasterio.open(fp.name, 'w', **kwargs)
            new_dataset.write(values, 1)
            new_dataset.close()
            params = {'img_path':fp.name,
                      'bbox':self.bbox,
                      'bbox_crs':self.bbox_crs,
                      'band':1,
                      'clip':True}
            print('Execution')
            result = dce.bbox_windows(**params)

        os.remove(fp.name)
        print('Validation')
        prediction_lenght = 9
        # prediction = [rasterio.windows.Window(col_off=51, row_off=1089, width=461, height=447),
        #  rasterio.windows.Window(col_off=512, row_off=1089, width=512, height=447),
        #  rasterio.windows.Window(col_off=1024, row_off=1089, width=37, height=447),
        #  rasterio.windows.Window(col_off=51, row_off=1536, width=461, height=512),
        #  rasterio.windows.Window(col_off=512, row_off=1536, width=512, height=512),
        #  rasterio.windows.Window(col_off=1024, row_off=1536, width=37, height=512),
        #  rasterio.windows.Window(col_off=51, row_off=2048, width=461, height=427),
        #  rasterio.windows.Window(col_off=512, row_off=2048, width=512, height=427),
        #  rasterio.windows.Window(col_off=1024, row_off=2048, width=37, height=427)]
        assert type(result) == list,\
            f"Message It's ({type(result)}) should be­ ({list}) "
        assert len(result) == prediction_lenght,\
            f"Message It's ({len(result)}) should be­ ({prediction_lenght}) "
        # assert result == prediction,\
        #     f"Message It's ({result}) should be­ ({prediction}) "


    def test_bbox_windows_not_clipped(self):
        """
        Test for function bbox_windows if clip=False
        """
        print('Preparation')
        #Create a temp rasterfile
        with NamedTemporaryFile(suffix='.tif', delete=False) as fp :
            kwargs, values = raster_temp_def()
            new_dataset = rasterio.open(fp.name, 'w', **kwargs)
            new_dataset.write(values, 1)
            new_dataset.close()

            params = {'img_path':fp.name,
                      'bbox':self.bbox,
                      'bbox_crs':self.bbox_crs,
                      'band':1,
                      'clip':False}
            print('Execution')
            result = dce.bbox_windows(**params)

        os.remove(fp.name)
        print('Validation')
        prediction_lenght = 9
        # prediction = [rasterio.windows.Window(col_off=0, row_off=1024, width=512, height=512),
        #  rasterio.windows.Window(col_off=512, row_off=1024, width=512, height=512),
        #  rasterio.windows.Window(col_off=1024, row_off=1024, width=512, height=512),
        #  rasterio.windows.Window(col_off=0, row_off=1536, width=512, height=512),
        #  rasterio.windows.Window(col_off=512, row_off=1536, width=512, height=512),
        #  rasterio.windows.Window(col_off=1024, row_off=1536, width=512, height=512),
        #  rasterio.windows.Window(col_off=0, row_off=2048, width=512, height=452),
        #  rasterio.windows.Window(col_off=512, row_off=2048, width=512, height=452),
        #  rasterio.windows.Window(col_off=1024, row_off=2048, width=512, height=452)]
        assert type(result) == list,\
            f"Message It's ({type(result)}) should be­ ({list}) "
        assert len(result) == prediction_lenght,\
            f"Message It's ({len(result)}) should be­ ({prediction_lenght}) "
        # assert result == prediction,\
        #     f"Message It's ({result}) should be­ ({prediction}) "


class TestExtractCogchip():
    """
    Test class for function extract.extract_cogchip()
    """
    def test_extract_cogchip_natif(self, tmp_path):
        """
        Test for function extract_cogchip() with native projection and resolution
        """
        print('Preparation')
        in_file = tmp_path / "toto.tif"
        # in_file = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_integration\get_extract_params\toto.tif'
        kwargs, values = raster_temp_def()
        new_dataset = rasterio.open(in_file, 'w', **kwargs)
        new_dataset.write(values, 1)
        new_dataset.close()

        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))

        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                                'width': 1011, 'height': 1386, 'count': 1,
                                'crs': rasterio.crs.CRS.from_epsg(3979),
                                'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                                    'width': 1011, 'height': 1386, 'count': 1,
                                    'crs': rasterio.crs.CRS.from_epsg(3979),
                                    'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                    'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                    'compress': 'lzw', 'interleave': 'band',
                                    'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}

        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        result_raster = rasterio.open(out_file)
        result_profile = result_raster.profile
        #The plan is to read the outfile and extract some key parameters
        prediction_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 1011, 'height': 1386, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(3979),
                              'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'}
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "

        result_array = result_raster.read()
        prediction_min = 0
        prediction_max = 9
        assert math.floor(result_array.min()) == prediction_min,\
            f"Message It's ({math.floor(result_array.min())}) should be­ ({prediction_min}) "
        assert math.floor(result_array.max()) == prediction_max,\
            f"Message It's ({math.floor(result_array.max())}) should be­ ({prediction_max}) "

        result_raster.close()


    def test_extract_cogchip_reproject(self, tmp_path):
        """
        Test for function extract_minicube() reprojected to EPSG:2960 and native resolution
        """
        print('Preparation')
        in_file = tmp_path / "toto.tif"
        # in_file = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_integration\get_extract_params\toto.tif'
        kwargs, values = raster_temp_def()
        new_dataset = rasterio.open(in_file, 'w', **kwargs)
        new_dataset.write(values, 1)
        new_dataset.close()

        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))


        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 1467, 'height': 1656, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(2960),
                              'transform': Affine(1.0, 0.0, 677703.0, 0.0, -1.0, 5089743.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                             'width': 1467, 'height': 1656, 'count': 1,
                             'crs': rasterio.crs.CRS.from_epsg(2960),
                             'transform': Affine(1.0, 0.0, 677703.0, 0.0, -1.0, 5089743.0),
                             'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                             'compress': 'lzw', 'interleave': 'band',
                             'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}

        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        result_raster = rasterio.open(out_file)
        result_profile = result_raster.profile
        #The plan is to read the outfile and extract some key parameters
        prediction_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 1467, 'height': 1656, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(2960),
                              'transform': Affine(1.0, 0.0, 677703.0, 0.0, -1.0, 5089743.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'}
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "

        result_array = result_raster.read()
        prediction_min = kwargs['nodata']
        prediction_max = 9
        assert math.floor(result_array.min()) == prediction_min,\
            f"Message It's ({math.floor(result_array.min())}) should be­ ({prediction_min}) "
        assert math.floor(result_array.max()) == prediction_max,\
            f"Message It's ({math.floor(result_array.max())}) should be­ ({prediction_max}) "

        result_raster.close()


    def test_extract_cogchip_resample(self, tmp_path):
        """
        Test function extract_minicube() in native projection but resampled to 20m
        """
        print('Preparation')
        in_file = tmp_path / "toto.tif"
        # in_file = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_integration\get_extract_params\toto6.tif'
        kwargs, values = raster_temp_def()
        new_dataset = rasterio.open(in_file, 'w', **kwargs)
        new_dataset.write(values, 1)
        new_dataset.close()

        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))

        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 26, 'height': 36, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(3979),
                              'transform': Affine(40.0, 0.0, 2150520.0, 0.0, -40.0, 143920.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                             'width': 1040.0, 'height': 1440.0, 'count': 1,
                             'crs': rasterio.crs.CRS.from_epsg(3979),
                             'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143920.0),
                             'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                             'compress': 'lzw', 'interleave': 'band',
                             'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}

        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        result_raster = rasterio.open(out_file)
        result_profile = result_raster.profile
        #The plan is to read the outfile and extract some key parameters
        prediction_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 26, 'height': 36, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(3979),
                              'transform': Affine(40.0, 0.0, 2150520.0, 0.0, -40.0, 143920.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'}
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "

        result_array = result_raster.read()
        prediction_min = 0
        prediction_max = 9
        assert math.floor(result_array.min()) == prediction_min,\
            f"Message It's ({math.floor(result_array.min())}) should be­ ({prediction_min}) "
        assert math.floor(result_array.max()) == prediction_max,\
            f"Message It's ({math.floor(result_array.max())}) should be­ ({prediction_max}) "

        result_raster.close()


    def test_extract_minicube_reproj_resamp(self, tmp_path):
        """
        Test function extract_minicube() with reprojection to EPSG:2960 and resampling to 30m
        """
        print('Preparation')
        in_file = tmp_path / "toto.tif"
        # in_file = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_integration\get_extract_params\toto3.tif'
        kwargs, values = raster_temp_def()
        new_dataset = rasterio.open(in_file, 'w', **kwargs)
        new_dataset.write(values, 1)
        new_dataset.close()

        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))

        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 38, 'height': 43, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(2960),
                              'transform': Affine(40.0, 0.0, 677680.0, 0.0, -40.0, 5089760.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'},
                  'extract_params':{'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                             'width': 1520.0, 'height': 1720.0, 'count': 1,
                             'crs': rasterio.crs.CRS.from_epsg(2960),
                             'transform': Affine(1.0, 0.0, 677680.0, 0.0, -1.0, 5089760.0),
                             'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                             'compress': 'lzw', 'interleave': 'band',
                             'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}

        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        result_raster = rasterio.open(out_file)
        result_profile = result_raster.profile
        #The plan is to read the outfile and extract some key parameters
        prediction_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 38, 'height': 43, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(2960),
                              'transform': Affine(40.0, 0.0, 677680.0, 0.0, -40.0, 5089760.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'}
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "

        result_array = result_raster.read()
        prediction_min = kwargs['nodata']
        prediction_max = 9
        assert math.floor(result_array.min()) == prediction_min,\
            f"Message It's ({math.floor(result_array.min())}) should be­ ({prediction_min}) "
        assert math.floor(result_array.max()) == prediction_max,\
            f"Message It's ({math.floor(result_array.max())}) should be­ ({prediction_max}) "

        result_raster.close()


    def test_extract_cogchip_natif_overviews(self, tmp_path):
        """
        Test for function extract_minicube() with native projection and resolution with overviews
        """

        print('Preparation')
        in_file = tmp_path / "toto.tif"
        # in_file = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_integration\get_extract_params\toto3.tif'
        kwargs, values = raster_temp_def()
        new_dataset = rasterio.open(in_file, 'w', **kwargs)
        new_dataset.write(values, 1)
        new_dataset.close()

        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))

        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                                'width': 1011, 'height': 1386, 'count': 1,
                                'crs': rasterio.crs.CRS.from_epsg(3979),
                                'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                                    'width': 1011, 'height': 1386, 'count': 1,
                                    'crs': rasterio.crs.CRS.from_epsg(3979),
                                    'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                    'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                    'compress': 'lzw', 'interleave': 'band',
                                    'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':True}

        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        result_raster = rasterio.open(out_file)
        result_profile = result_raster.profile
        #The plan is to read the outfile and extract some key parameters
        prediction_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0,
                              'width': 1011, 'height': 1386, 'count': 1,
                              'crs': rasterio.crs.CRS.from_epsg(3979),
                              'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                              'compress': 'lzw', 'interleave': 'band'}
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "

        prediction_overviews = [2, 4]
        assert result_raster.overviews(1) == prediction_overviews,\
            f"Message It's ({result_raster.overviews(1)}) should be­ ({prediction_overviews}) "
        result_raster.close()
    
    def test_extract_cogchip_color(self, tmp_path):
        """
        Test for function extract_minicube() with color dictionary
        """

        print('Preparation')
        in_file = tmp_path / "toto.tif"
        kwargs, values = raster_temp_def()
        kwargs['dtype'] = 'uint8'
        kwargs['nodata'] = 255
        with rasterio.open(in_file, 'w', **kwargs) as new_dataset:
            color_map = {0: (255, 0, 0, 255), 254: (0, 0, 255, 255) }
            new_dataset.write_colormap(1, color_map)

        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))
        
        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                'width': 1011, 'height': 1386, 'count': 1,
                                'crs': rasterio.crs.CRS.from_epsg(3979),
                                'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                    'width': 1011, 'height': 1386, 'count': 1,
                                    'crs': rasterio.crs.CRS.from_epsg(3979),
                                    'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                    'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                    'compress': 'lzw', 'interleave': 'band',
                                    'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}
        
        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        with rasterio.open(out_file) as result_raster:
            color_dict_result = result_raster.colormap(1)
            colormap_result = {0: color_dict_result[0], 254: color_dict_result[254] }
        assert colormap_result == color_map,\
            f"Message It's ({colormap_result}) should be­ ({color_map}) "
            
    
    def test_extract_cogchip_no_color(self, tmp_path):
        """
        Test for function extract_minicube() with no color dictionary
        """

        print('Preparation')
        in_file = tmp_path / "toto.tif"
        kwargs, values = raster_temp_def()
        kwargs['dtype'] = 'uint8'
        kwargs['nodata'] = 255
        with rasterio.open(in_file, 'w', **kwargs) as new_dataset:
          new_dataset.write(values, 1)
        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))
        
        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                'width': 1011, 'height': 1386, 'count': 1,
                                'crs': rasterio.crs.CRS.from_epsg(3979),
                                'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                    'width': 1011, 'height': 1386, 'count': 1,
                                    'crs': rasterio.crs.CRS.from_epsg(3979),
                                    'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                    'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                    'compress': 'lzw', 'interleave': 'band',
                                    'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}
        
        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        color_prediction = None
        with rasterio.open(out_file) as result_raster:
                try:
                    color_dict = result_raster.colormap(1)
                except:
                    color_dict = None
        assert color_dict == color_prediction, \
            f"Message It's ({color_dict}) should be­ ({color_prediction}) "
    
    def test_extract_cogchip_datetime(self, tmp_path):
        """
        Test for function extract_minicube() with datetime
        """

        print('Preparation')
        in_file = tmp_path / "toto.tif"
        kwargs, values = raster_temp_def()
        with rasterio.open(in_file, 'w', **kwargs) as new_dataset:
            new_dataset.update_tags(TIFFTAG_DATETIME='2010:07:01 12:00:00')
            
        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))
        
        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                'width': 1011, 'height': 1386, 'count': 1,
                                'crs': rasterio.crs.CRS.from_epsg(3979),
                                'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                    'width': 1011, 'height': 1386, 'count': 1,
                                    'crs': rasterio.crs.CRS.from_epsg(3979),
                                    'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                    'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                    'compress': 'lzw', 'interleave': 'band',
                                    'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}
        
        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        datetime_prediction = '2010:07:01 12:00:00'
        with rasterio.open(out_file) as result_raster:
            result_database = result_raster.tags()
            result_datetime = result_database["TIFFTAG_DATETIME"]
        assert datetime_prediction ==  result_datetime, \
            f"Message It's ({result_datetime}) should be­ ({datetime_prediction}) "
            
    def test_extract_cogchip_non_datetime(self, tmp_path):
        """
        Test for function extract_minicube() with no datetime
        """

        print('Preparation')
        in_file = tmp_path / "toto.tif"
        kwargs, values = raster_temp_def()
        with rasterio.open(in_file, 'w', **kwargs) as new_dataset:
            new_dataset.write(values, 1)
            
        out_file = pathlib.Path(os.path.join(pathlib.Path(in_file).parent,
                                pathlib.Path(in_file).stem+ '_result'+
                                pathlib.Path(in_file).suffix))
        
        params = {'in_path':in_file,
                  'out_path':out_file ,
                  'out_profile':{'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                'width': 1011, 'height': 1386, 'count': 1,
                                'crs': rasterio.crs.CRS.from_epsg(3979),
                                'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                'compress': 'lzw', 'interleave': 'band'},
                  'extract_params': {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255,
                                    'width': 1011, 'height': 1386, 'count': 1,
                                    'crs': rasterio.crs.CRS.from_epsg(3979),
                                    'transform': Affine(1.0, 0.0, 2150520.0, 0.0, -1.0, 143886.0),
                                    'blockxsize': 512, 'blockysize': 512, 'tiled': True,
                                    'compress': 'lzw', 'interleave': 'band',
                                    'resampling': rasterio.enums.Resampling.bilinear},
                  'overviews':False}
        
        print('Execution')
        result = dce.extract_cogchip(**params)
        print('Validation')
        datetime_prediction = None
        with rasterio.open(out_file) as result_raster:
            result_database = result_raster.tags()
            try:
                result_datetime = result_database["TIFFTAG_DATETIME"]
            except KeyError:
                result_datetime = None
        assert datetime_prediction ==  result_datetime, \
            f"Message It's ({result_datetime}) should be­ ({datetime_prediction}) "
            

class TestSaveCogFromFile():
    """
    class test for function save_cog_from_file()
    """


    def test_save_cog_from_file(self):
        """
        Test function test_save_cog_from_file() if creation of overviews
        """
        print('Preparation')
        tifftag_datetime = '1944:06:06 07:35:00'
        try:
            with TemporaryDirectory() as d:
                temp_file_name = pathlib.Path(os.path.join(d, 'toto.tif'))
                test_unit.create_temp_tif(temp_file_name)

                print('Execution')
                DEX.save_cog_from_file(
                    temp_file_name,
                    tifftag_datetime,
                    resampling_method='average',
                    blocksize = 32,
                    overviews=False)

                print('Validation')
                with rasterio.open(temp_file_name) as dataset:
                    result_profile = re.sub('\\s+', ' ', str(dataset.profile))
                    prediction_profile = "{'driver': 'GTiff', 'dtype': 'float64',"\
                        " 'nodata': None, 'width': 240, 'height': 180, 'count': 1,"\
                        " 'crs': CRS.from_epsg(4326), 'transform': "\
                        "Affine(0.03333333333333333, 0.0, -4.016666666666667,"\
                        " 0.0, 0.03333333333333333, -3.0166666666666666),"\
                        " 'blockxsize': 32, 'blockysize': 32, 'tiled': True,"\
                        " 'compress': 'lzw', 'interleave': 'band'}"
                    result_overviews = [dataset.overviews(i) for i in dataset.indexes]
                    prediction_overviews = [[]]
                    result_datetime = dataset.get_tag_item("TIFFTAG_DATETIME")
                    dataset.close()

            assert result_profile == prediction_profile,\
                f"Profile is ({result_profile}) should be­ ({prediction_profile}) "
            assert result_overviews == prediction_overviews,\
                f"Overviews are ({result_overviews}) should be­ ({prediction_overviews}) "
            assert result_datetime == tifftag_datetime,\
                f"Datetime is ({result_datetime}) should be­ ({tifftag_datetime}) "
        except:
            raise Exception('test_save_cog_from_file function failed')


    def test_save_cog_from_file_ovw(self):
        """
        Test function test_save_cog_from_file() if creation of overviews
        """
        print('Preparation')
        tifftag_datetime = '1944:06:06 07:35:00'
        try:
            with TemporaryDirectory() as d:
                temp_file_name = pathlib.Path(os.path.join(d, 'toto.tif'))
                test_unit.create_temp_tif(temp_file_name)

                print('Execution')
                DEX.save_cog_from_file(
                    temp_file_name,
                    tifftag_datetime,
                    resampling_method='average',
                    blocksize = 32,
                    overviews=True)

                print('Validation')
                with rasterio.open(temp_file_name) as dataset:
                    result_profile = re.sub('\\s+', ' ', str(dataset.profile))
                    prediction_profile = "{'driver': 'GTiff', 'dtype': 'float64',"\
                        " 'nodata': None, 'width': 240, 'height': 180, 'count': 1,"\
                        " 'crs': CRS.from_epsg(4326), 'transform': "\
                        "Affine(0.03333333333333333, 0.0, -4.016666666666667,"\
                        " 0.0, 0.03333333333333333, -3.0166666666666666),"\
                        " 'blockxsize': 32, 'blockysize': 32, 'tiled': True,"\
                        " 'compress': 'lzw', 'interleave': 'band'}"
                    result_overviews = [dataset.overviews(i) for i in dataset.indexes]
                    prediction_overviews = [[2, 4, 8]]
                    result_datetime = dataset.get_tag_item("TIFFTAG_DATETIME")
                    dataset.close()

            assert result_profile == prediction_profile,\
                f"Profile is ({result_profile}) should be­ ({prediction_profile}) "
            assert result_overviews == prediction_overviews,\
                f"Overviews are ({result_overviews}) should be­ ({prediction_overviews}) "
            assert result_datetime == tifftag_datetime,\
                f"Datetime is ({result_datetime}) should be­ ({tifftag_datetime}) "
        except:
            raise Exception('test_save_cog_from_file function failed')


def test_wcs_request():
    """
    Test function wcs_request
    """
    print('Preparation')
    poly = DEX.bbox_to_poly(bbox=BBOX)
    print('Execution')
    request, bbox = DEX.wcs_request(poly,cellsize=300,level='stage')
    print('Validation')
    prediction_request = 'https://datacube-stage.services.geo.ca/ows/elevation?'\
        'service=WCS&version=1.1.1&request=GetCoverage&format=image/geotiff&'\
        'identifier=dtm&BoundingBox=-1862977.0,312787.0,-1110277.0,632587.0,'\
        'urn:ogc:def:crs:EPSG::3979&GridBaseCRS=urn:ogc:def:crs:EPSG::3979&'\
        'GridOffsets=300.0,-300.0'
    prediction_bbox = (-1862977.0, 312787.0, -1110277.0, 632587.0)
    assert request == prediction_request,\
        f"Message It's ({request}) should be­ ({prediction_request}) "
    assert bbox == prediction_bbox,\
        f"Message It's ({bbox}) should be­ ({prediction_bbox}) "


class TestWarpedMosaic():
    """
    class to test function warped_mosaic()
    """
    # kwargs_1 = self._create_fake_file(500, 1)
    # kwargs_2 = self._create_fake_file(1000, 2) 
    # kwargs_3 = self._create_fake_file(3000, 1)
    # kwargs_4 = self._create_fake_file(1500, 1)
    def _create_fake_file(self, size:int, res:int, epsg:int=3979):
        ori_x = 2150469
        ori_y = 144975
        transform = rasterio.transform.from_origin(ori_x, ori_y, res, res)
        kwargs = {'driver':'GTiff',
              'height':size,
              'width':size,
              'count':1,
              'dtype':'float64',
              'crs':rasterio.crs.CRS().from_epsg(epsg),
              'transform':transform,
              'tiled':True,
              'blockxsize':512,
              'blockysize':512,
              'compress':'lzw',
              'nodata': -32767.0,
              'interleave': 'band'}
        
        return kwargs
    
    
    def test_warped_mosaic(self, tmp_path):
        """
        Test function warped_mosaic() for:
            - Input list of file with all the same res
            - Input crs = output_crs
        """
        print('Preparation')
        file_1 = os.path.join(tmp_path, 'raster_1.tif')
        kwargs_1 = self._create_fake_file(500, 1)
        # file_1 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_1.tif'
        with rasterio.open(file_1, 'w', **kwargs_1) as raster_1:
            values = np.full((raster_1.shape[0], raster_1.shape[1]),1)
            raster_1.write(values, 1)
            
        file_3 = os.path.join(tmp_path, 'raster_3.tif')
        # file_3 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_3.tif'
        kwargs_3 = self._create_fake_file(3000, 1)
        with rasterio.open(file_3, 'w', **kwargs_3) as raster_3:
            values = np.full((raster_3.shape[0], raster_3.shape[1]),3)
            raster_3.write(values,1)  
        
        extract_params = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                          'width': 2000, 'height': 1001, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                          'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                          'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                          'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear} #Its the same extract params for both files since they have the same crs and res
        
        out_file = pathlib.Path(os.path.join(pathlib.Path(file_1).parent,
                                'test_warped_mosaic.tif'))
        
        
        list_of_params = [{'file': file_1,'params': extract_params}, 
                          {'file' : file_3, 'params': extract_params}]
        
        out_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                       'width': 2000, 'height': 1001, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                       'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                       'interleave': 'band'}
        
        params = {'list_of_params':list_of_params,
                  'out_path':out_file,
                  'out_profile':out_profile}
        
        print('Execution')
        result = dce.warped_mosaic(**params)
        print('Validation')
        #read the profile from the mosaic output and validate 
        result_mosaic = rasterio.open(out_file)
        #read the profile
        result_profile = result_mosaic.profile
        prediction_profile = out_profile 
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
            
        #Validate that the first file in the list is the one on the top (reverse painters)
        result_array = result_mosaic.read(1)
        prediction_first_raster_value = 1.0
        result_first_raster_value = result_array[200, 200]
        
        prediction_second_raster_value = 3.0
        result_second_raster_value = result_array[501, 600]
        
        assert result_first_raster_value == prediction_first_raster_value,\
            f"Message It's ({result_first_raster_value}) should be­ ({prediction_first_raster_value})"
            
        assert result_second_raster_value == prediction_second_raster_value,\
            f"Message It's ({result_second_raster_value}) should be­ ({prediction_second_raster_value})"
        
        result_mosaic.close()
        
    
    def test_warped_mosaic_multiple_res(self, tmp_path):
        """
        test function warped_mosaic() for:
            - Input list of file with not all the same res (so need a reprojection) chosen res = 4
            - Input_crs = output_crs
        """
        print('Preparation')
        file_1 = os.path.join(tmp_path, 'raster_1.tif')
        kwargs_1 = self._create_fake_file(500, 1)
        # file_1 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_1.tif'
        with rasterio.open(file_1, 'w', **kwargs_1) as raster_1:
            values = np.full((raster_1.shape[0], raster_1.shape[1]),1)
            raster_1.write(values, 1)
        
        extract_params_1 = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                            'width': 2004.0, 'height': 1008.0, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                            'transform': Affine(1.0, 0.0, 2150468.0, 0.0, -1.0, 144976.0), 
                            'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                            'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear}

        
        file_2 = os.path.join(tmp_path, 'raster_3.tif')
        # file_2 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_2.tif'
        kwargs_2 = self._create_fake_file(1000, 2) 
        with rasterio.open(file_2, 'w', **kwargs_2) as raster_2:
            values = np.full((raster_2.shape[0], raster_2.shape[1]),2)
            raster_2.write(values,1)
            
        extract_params_2 = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                            'width': 1002.0, 'height': 504.0, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                            'transform': Affine(2.0, 0.0, 2150468.0, 0.0, -2.0, 144976.0), 
                            'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                            'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear}
        
        out_file = pathlib.Path(os.path.join(pathlib.Path(file_1).parent,
                                'test_warped_mosaic_2.tif'))
        
        list_of_params = [{'file': file_1,'params': extract_params_1}, 
                          {'file' : file_2, 'params': extract_params_2}]
        
        out_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                       'width': 501, 'height': 252, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                       'transform': Affine(4.0, 0.0, 2150468.0, 0.0, -4.0, 144976.0), 
                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                       'interleave': 'band'}

        params = {'list_of_params':list_of_params,
                  'out_path':out_file,
                  'out_profile':out_profile}
        
        print('Execution')
        result = dce.warped_mosaic(**params)
        print('Validation')
        #read the profile from the mosaic output and validate 
        result_mosaic = rasterio.open(out_file)
        #read the profile
        result_profile = result_mosaic.profile
        prediction_profile = out_profile 
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
            
        #Validate that the first file in the list is the one on the top (reverse painters)
        result_array = result_mosaic.read(1)
        prediction_first_raster_value = 1.0
        result_first_raster_value = result_array[1, 1]
        
        prediction_second_raster_value = 2.0
        result_second_raster_value = result_array[200, 200]
        
        assert result_first_raster_value == prediction_first_raster_value,\
            f"Message It's ({result_first_raster_value}) should be­ ({prediction_first_raster_value})"
            
        assert result_second_raster_value == prediction_second_raster_value,\
            f"Message It's ({result_second_raster_value}) should be­ ({prediction_second_raster_value})"
        
        result_mosaic.close()
        
        
    def test_warped_mosaic_reproject(self, tmp_path):
        """
        test function waperd_mosaic() for:
            - Input list of file with all the same res
            - Input crs != output_crs
        """
        print('Preparation')
        file_1 = os.path.join(tmp_path, 'raster_1.tif')
        kwargs_1 = self._create_fake_file(500, 1)
        # file_1 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_1.tif'
        with rasterio.open(file_1, 'w', **kwargs_1) as raster_1:
            values = np.full((raster_1.shape[0], raster_1.shape[1]),1)
            raster_1.write(values, 1)
            
        file_3 = os.path.join(tmp_path, 'raster_3.tif')
        kwargs_3 = self._create_fake_file(3000, 1)
        # file_3 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_3.tif'
        with rasterio.open(file_3, 'w', **kwargs_3) as raster_3:
            values = np.full((raster_3.shape[0], raster_3.shape[1]),3)
            raster_3.write(values,1)
        
        extract_params = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                          'width': 2205, 'height': 1702, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(2960), 
                          'transform': Affine(1.0, 0.0, 678245.0, 0.0, -1.0, 5090747.0), 
                          'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                          'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear} #the same for both since same res and crs
        
        out_file = pathlib.Path(os.path.join(pathlib.Path(file_1).parent,
                                'test_warped_mosaic.tif'))
        
        
        list_of_params = [{'file': file_1,'params': extract_params}, 
                          {'file' : file_3, 'params': extract_params}]
        
        out_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                       'width': 2205, 'height': 1702, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(2960), 
                       'transform': Affine(1.0, 0.0, 678245.0, 0.0, -1.0, 5090747.0), 
                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                       'interleave': 'band'}

        
        params = {'list_of_params':list_of_params,
                  'out_path':out_file,
                  'out_profile':out_profile}
        
        print('Execution')
        result = dce.warped_mosaic(**params)
        print('Validation')
        #read the profile from the mosaic output and validate 
        result_mosaic = rasterio.open(out_file)
        #read the profile
        result_profile = result_mosaic.profile
        prediction_profile = out_profile 
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
            
        #Validate that the first file in the list is the one on the top (reverse painters)
        result_array = result_mosaic.read(1)
        prediction_first_raster_value = 1.0
        result_first_raster_value = result_array[600, 600]
        
        prediction_second_raster_value = 3.0
        result_second_raster_value = result_array[1000, 1000]   
        
        assert result_first_raster_value == prediction_first_raster_value,\
            f"Message It's ({result_first_raster_value}) should be­ ({prediction_first_raster_value})"
            
        assert result_second_raster_value == prediction_second_raster_value,\
            f"Message It's ({result_second_raster_value}) should be­ ({prediction_second_raster_value})"
        
        result_mosaic.close()
        
        
    def test_warped_mosaic_multiple_res_reproject(self, tmp_path):
        """
        test function waperd_mosaic() for:
            - Input list of file with not all the same res (so need a reprojection) chosen res = 4
            - Input crs != output_crs
        """
        print('Preparation')
        file_1 = os.path.join(tmp_path, 'raster_1.tif')
        kwargs_1 = self._create_fake_file(500, 1)
        # file_1 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_1.tif'
        with rasterio.open(file_1, 'w', **kwargs_1) as raster_1:
            values = np.full((raster_1.shape[0], raster_1.shape[1]),1)
            raster_1.write(values, 1)
        
        extract_params_1 = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                            'width': 2208.0, 'height': 1708.0, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(2960), 
                            'transform': Affine(1.0, 0.0, 678244.0, 0.0, -1.0, 5090748.0), 
                            'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                            'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear}

        file_2 = os.path.join(tmp_path, 'raster_3.tif')
        kwargs_2 = self._create_fake_file(1000, 2) 
        # file_2 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_2.tif'
        with rasterio.open(file_2, 'w', **kwargs_2) as raster_2:
            values = np.full((raster_2.shape[0], raster_2.shape[1]),2)
            raster_2.write(values,1)
            
        extract_params_2 = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                            'width': 1104.0, 'height': 854.0, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(2960), 
                            'transform': Affine(2.0, 0.0, 678244.0, 0.0, -2.0, 5090748.0), 
                            'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                            'interleave': 'band', 'resampling':rasterio.enums.Resampling.bilinear}

        
        out_file = pathlib.Path(os.path.join(pathlib.Path(file_1).parent,
                                'test_warped_mosaic.tif'))
        
        list_of_params = [{'file': file_1,'params': extract_params_1}, 
                          {'file' : file_2, 'params': extract_params_2}]
        
        out_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                       'width': 552, 'height': 427, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(2960), 
                       'transform': Affine(4.0, 0.0, 678244.0, 0.0, -4.0, 5090748.0), 
                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                       'interleave': 'band'}


        params = {'list_of_params':list_of_params,
                  'out_path':out_file,
                  'out_profile':out_profile}
        
        print('Execution')
        result = dce.warped_mosaic(**params)
        print('Validation')
        #read the profile from the mosaic output and validate 
        result_mosaic = rasterio.open(out_file)
        #read the profile
        result_profile = result_mosaic.profile
        prediction_profile = out_profile 
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
            
        #Validate that the first file in the list is the one on the top (reverse painters)
        result_array = result_mosaic.read(1)
        prediction_first_raster_value = 1.0
        result_first_raster_value = result_array[100, 100]
        
        prediction_second_raster_value = 2.0
        result_second_raster_value = result_array[300, 500]
        
        assert result_first_raster_value == prediction_first_raster_value,\
            f"Message It's ({result_first_raster_value}) should be­ ({prediction_first_raster_value})"
            
        assert result_second_raster_value == prediction_second_raster_value,\
            f"Message It's ({result_second_raster_value}) should be­ ({prediction_second_raster_value})"
        
        result_mosaic.close()
           
        
    def test_warped_mosaic_reverse_painters(self, tmp_path):
        """
        test function warped_mosaic() to validate that the data are merged using the reverse 
        painters logic for merging the data
        Reverse painters: The first image is the boss, and no data values are filled by the other
        files following the list order.
        The test was made to combine 3 raster, but value from raster_4 should not be added to the output
        mosaic since all the data is suposed to be filled with the 2 other file before. """
        
        print('Preparation')
        file_1 = os.path.join(tmp_path, 'raster_1.tif')
        kwargs_1 = self._create_fake_file(500, 1)
        # file_1 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_1.tif'
        with rasterio.open(file_1, 'w', **kwargs_1) as raster_1:
            values = np.full((raster_1.shape[0], raster_1.shape[1]),1)
            raster_1.write(values, 1)
            
        file_3 = os.path.join(tmp_path, 'raster_3.tif')
        # file_3 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_3.tif'
        kwargs_3 = self._create_fake_file(3000, 1)
        with rasterio.open(file_3, 'w', **kwargs_3) as raster_3:
            values = np.full((raster_3.shape[0], raster_3.shape[1]),3)
            raster_3.write(values,1)  
        
        file_4 = os.path.join(tmp_path, 'raster_4.tif')
        # file_4 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_4.tif'
        kwargs_4 = self._create_fake_file(1500, 1)
        with rasterio.open(file_4, 'w', **kwargs_4) as raster_4:
            values = np.full((raster_4.shape[0], raster_4.shape[1]),4)
            raster_4.write(values,1)  
        
        extract_params = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                          'width': 2000, 'height': 1001, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                          'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                          'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                          'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear} #Its the same extract params for both files since they have the same crs and res
        
        out_file = pathlib.Path(os.path.join(pathlib.Path(file_1).parent,
                                'test_warped_mosaic.tif'))
        
        #make the list in order of priority for data filling of the mosaic.
        list_of_params = [{'file': file_1,'params': extract_params}, 
                          {'file' : file_3, 'params': extract_params},
                          {'file' : file_4, 'params': extract_params}] 
        
        out_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                       'width': 2000, 'height': 1001, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                       'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                       'interleave': 'band'}
        
        params = {'list_of_params':list_of_params,
                  'out_path':out_file,
                  'out_profile':out_profile}
        
        print('Execution')
        result = dce.warped_mosaic(**params)
        print('Validation')
        #read the profile from the mosaic output and validate 
        result_mosaic = rasterio.open(out_file)
        #Validate that value 4 is not in the output mosaic
        #Validate that the first file in the list is the one on the top (reverse painters)
        result_array = result_mosaic.read(1)
        prediction_first_raster_value = 1.0
        result_first_raster_value = result_array[200, 200]
        
        prediction_second_raster_value = 3.0
        result_second_raster_value = result_array[501, 600]
        
        assert result_first_raster_value == prediction_first_raster_value,\
            f"Message It's ({result_first_raster_value}) should be­ ({prediction_first_raster_value})"
            
        assert result_second_raster_value == prediction_second_raster_value,\
            f"Message It's ({result_second_raster_value}) should be­ ({prediction_second_raster_value})"
            
        assert 4 not in result_array, \
            "Message Value 4 is present in result array, reverse painter error, 4 should not be in result array"
    
        result_mosaic.close()
    
    
    def test_warped_mosaic_overviews(self, tmp_path):
        """
        Test function warped_mosaic() for:
            - Input list of file with all the same res
            - Input crs = output_crs
            - overviews = True
        """
        print('Preparation')
        file_1 = os.path.join(tmp_path, 'raster_1.tif')
        kwargs_1 = self._create_fake_file(5000, 1)
        # file_1 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_1.tif'
        with rasterio.open(file_1, 'w', **kwargs_1) as raster_1:
            values = np.full((raster_1.shape[0], raster_1.shape[1]),1)
            raster_1.write(values, 1)
            
        file_3 = os.path.join(tmp_path, 'raster_3.tif')
        # file_3 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_3.tif'
        kwargs_3 = self._create_fake_file(7000, 1)
        with rasterio.open(file_3, 'w', **kwargs_3) as raster_3:
            values = np.full((raster_3.shape[0], raster_3.shape[1]),3)
            raster_3.write(values,1)  
        
        extract_params = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                          'width': 7000, 'height': 8000, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                          'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                          'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                          'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear} #Its the same extract params for both files since they have the same crs and res
        
        out_file = pathlib.Path(os.path.join(pathlib.Path(file_1).parent,
                                'test_warped_mosaic.tif'))
        
        
        list_of_params = [{'file': file_1,'params': extract_params}, 
                          {'file' : file_3, 'params': extract_params}]
        
        out_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                       'width': 7000, 'height': 8000, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                       'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                       'interleave': 'band'}
        
        params = {'list_of_params':list_of_params,
                  'out_path':out_file,
                  'out_profile':out_profile,
                  'overviews':True}
        
        print('Execution')
        result = dce.warped_mosaic(**params)
        print('Validation')
        #read the profile from the mosaic output and validate 
        result_mosaic = rasterio.open(out_file)
        #read the profile
        prediction_overviews = [2, 4, 8, 16]
        assert result_mosaic.overviews(1) == prediction_overviews,\
            f"Message It's ({result_mosaic.overviews(1)}) should be­ ({prediction_overviews}) "
        
        result_mosaic.close()
        
    def test_warped_mosaic_stop_when_fill(self, tmp_path):
        """
        test function warped_mosaic() to validate that the system stop when the mosaic is complete
        before all the files are used.
        The test was made to combine 3 raster, but value from raster_4 should not be added 
        to the output and the program shouldstop before reading raster_4. 
        List of return filled for mosaic should then contain only thefirst 2 raster (raster_1, raster_3)
        """
        
        print('Preparation')
        file_1 = os.path.join(tmp_path, 'raster_1.tif')
        kwargs_1 = self._create_fake_file(500, 1)
        # file_1 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_1.tif'
        with rasterio.open(file_1, 'w', **kwargs_1) as raster_1:
            values = np.full((raster_1.shape[0], raster_1.shape[1]),1)
            raster_1.write(values, 1)
            
        file_3 = os.path.join(tmp_path, 'raster_3.tif')
        # file_3 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_3.tif'
        kwargs_3 = self._create_fake_file(3000, 1)
        with rasterio.open(file_3, 'w', **kwargs_3) as raster_3:
            values = np.full((raster_3.shape[0], raster_3.shape[1]),3)
            raster_3.write(values,1)  
        
        file_4 = os.path.join(tmp_path, 'raster_4.tif')
        # file_4 = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_intergration_extract/raster_4.tif'
        kwargs_4 = self._create_fake_file(1500, 1)
        with rasterio.open(file_4, 'w', **kwargs_4) as raster_4:
            values = np.full((raster_4.shape[0], raster_4.shape[1]),4)
            raster_4.write(values,1)  
        
        extract_params = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                          'width': 2000, 'height': 1001, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                          'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                          'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                          'interleave': 'band', 'resampling': rasterio.enums.Resampling.bilinear} #Its the same extract params for both files since they have the same crs and res
        
        out_file = pathlib.Path(os.path.join(pathlib.Path(file_1).parent,
                                'test_warped_mosaic.tif'))
        
        #make the list in order of priority for data filling of the mosaic.
        list_of_params = [{'file': file_1,'params': extract_params}, 
                          {'file' : file_3, 'params': extract_params},
                          {'file' : file_4, 'params': extract_params}] 
        
        out_profile = {'driver': 'GTiff', 'dtype': 'float64', 'nodata': -32767.0, 
                       'width': 2000, 'height': 1001, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
                       'transform': Affine(1.0, 0.0, 2150469.0, 0.0, -1.0, 144975.0), 
                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 
                       'interleave': 'band'}
        
        params = {'list_of_params':list_of_params,
                  'out_path':out_file,
                  'out_profile':out_profile}
        
        print('Execution')
        result = dce.warped_mosaic(**params)
        print('Validation')
        #Validate the list of file returned
        assert len(result) == 2,\
            f"Message It's ({len(result)}) should be­ 2 "


class TestOrderBy():
    """Testcases for extract.order_by

    Expectations written into code and passed to DataFrame.sort:
    Collection_id is always ASC
    Date or resolution is orderd by desc parameter value
    When resolution, date is always DESC
    """

    def test_resolution(self,bbox,bbox_crs):
        """Validate the resolution lookup works"""
        df_collections = pandas.DataFrame({'collection': ['landcover'], 'asset': [None]})
        df =  dce.asset_urls(df_collections,bbox_crs,bbox)
        df,urls = dce.order_by(df,'resolution',True)
        print(df[['collection_id','item_resolution']])
        assert len(df) == 3
        assert len(urls) == 3
        assert int(df.item_resolution.values[0]) == 30


class TestAssetURLs():
    """Testcases for extract.asset_urls
    """
    def test_bad_collection(self,bbox,bbox_crs):
        df_collections = pandas.DataFrame({'collection': ['not_a_real_collection'], 'asset': [None]})
        df = dce.asset_urls(df_collections,bbox_crs,bbox)
        assert len(df) == 0
    
    def test_bad_bbox(self,bbox_crs):
       """
       TODO STAC API still returns value, not an extract issue but... 
       TODO do we want to validate order of bbox so we are not returned whole country
       TODO coord order is a big deal, not sure want to address it or if it is better handeled by shapely, proj etc
       """
       df_collections = pandas.DataFrame({'collection': ['landcover'], 'asset': [None]})
       df =  dce.asset_urls(df_collections,bbox_crs,'-74,90,-75,45')
       assert len(df) == 3

    def test_key_partial_area_for_some_items(self,bbox,bbox_crs):
        df_collections = pandas.DataFrame({'collection': ['hrdem-lidar'], 'asset': ['dtm']})
        df = dce.asset_urls(df_collections,bbox_crs,bbox)

        # assert len(df) == 19
        assert len(df[df.asset_key != 'dtm']) == 0
        assert len(df.asset_key.unique()) == 1
        assert df.asset_key.unique()[0] == 'dtm'
    
    def test_key_full_area_for_all_items(self,bbox_crs):
        df_collections = pandas.DataFrame({'collection': ['hrdem-lidar'], 'asset': ['dtm']})
        df = dce.asset_urls(df_collections,bbox_crs,'-165,40,-34,90')
        # assert len(df) == 393
        assert len(df[df.asset_key != 'dtm']) == 0
        assert len(df.asset_key.unique()) == 1
        assert df.asset_key.unique()[0] == 'dtm'