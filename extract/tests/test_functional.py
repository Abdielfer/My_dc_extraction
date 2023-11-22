"""
Functional Testing module for extract.py
"""
import pathlib
from pathlib import Path
import os
import sys
import re
import rasterio
from rasterio.transform import Affine
import numpy as np
import pytest
from tempfile import TemporaryDirectory, NamedTemporaryFile
from shapely.geometry import Polygon
import rasterio
import math
import pandas

_CHILD_LEVEL = 2
DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
sys.path.append(DIR_NEEDED)
import ccmeo_datacube.extract as dce

#setup
DEX = dce.DatacubeExtract()
BBOX ='-1862977,312787,-1200000,542787'
BBOX_SMALL ='-1862977,312787,-1700000,442787'

class TestAssetURLs:
    """
    Test function test_asset_urls

    TODO Should this be in integration testing with connection to STAC API
    """
    def test_asset_urls_landcover(self):
        """
        Test function test_asset_urls
        """
        print('Preparation')
        collections = ['landcover']
        asset = [None]
        df_collections = pandas.DataFrame({'collection': collections, 'asset': asset})
        tbox = '-120,49,-118,50'
        tbox_crs = 'EPSG:4326'
        print('Execution')
        df = dce.asset_urls(df_collections,tbox_crs,tbox)
        result_urls = [u for u in df.url]
        print('Validation')
        prediction = ['https://datacube-prod-data-public.s3.amazonaws.com/store/land/landcover/landcover-2010-classification.tif',
                      'https://datacube-prod-data-public.s3.amazonaws.com/store/land/landcover/landcover-2015-classification.tif',
                      'https://datacube-prod-data-public.s3.amazonaws.com/store/land/landcover/landcover-2020-classification.tif']
        assert result_urls == prediction,\
            f"Message It's ({result_urls}) should be­ ({prediction}) "

    def test_asset_urls_asset(self):
        """
        Test function test_asset_urls
        """
        print('Preparation')
        collections = ['flood-susceptibility']
        asset = ['class']
        tbox = '-120,49,-118,50'
        tbox_crs = 'EPSG:4326'
        df_collections = pandas.DataFrame({'collection': collections, 'asset': asset})
        print('Execution')
        df = dce.asset_urls(df_collections,tbox_crs,tbox)
        result_urls = [u for u in df.url]
        print('Validation')
        prediction = ['https://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/water/flood-susceptibility/FS-national-2015-class.tif']
        assert len(result_urls) == len(prediction),\
            f"Message It's ({result_urls}) should be­ ({prediction}) "

    def test_asset_urls_date_filter(self):
        """Tests that the datetime filter returns proper asset list"""
        bbox = '-75,45,-74,46'
        bbox_crs = 'EPSG:4326'

        # Assumption landcover collection contains 2010, 2015, 2020
        # Should return no urls, no landcover item has this datetime
        nourl_date = '2016-12-02T00:00:00Z'

        # Should return landcover 2010
        specific_date = '2010-07-01T12:00:00Z'

        # Should return landcover 2010,2015
        valid_range1 = f'../{nourl_date}'
        
        # Should return landcover 2020
        valid_range2 = f'{nourl_date}/..'

        # Should return landcover 2020
        valid_range3 = '2016-12-02/..'
        
        df_collections = pandas.DataFrame({'collection': ['landcover'], 'asset': [None]})
        params = {'collection_asset':df_collections,'bbox':bbox,'extent_crs':bbox_crs}

        df =  dce.asset_urls(datetime_filter=nourl_date,**params)
        urls = [u for u in df.url]
        print(urls)
        assert len(urls) == 0
        assert len(dce.asset_urls(datetime_filter=specific_date,**params)) == 1
        assert len(dce.asset_urls(datetime_filter=valid_range1,**params)) == 2
        assert len(dce.asset_urls(datetime_filter=valid_range2,**params)) == 1
        assert len(dce.asset_urls(datetime_filter=valid_range3,**params)) == 1
        
    def test_asset_urls_resolution_filter(self):
        """Tests that the resolution filter returns proper asset list"""
        bbox = '-75,45,-74,46'
        bbox_crs = 'EPSG:4326'

        # Assumption hrdem-lidar is resolution 1 or 2m
        # Should return no urls, no hrdem-lidar as 4m native resolution
        nourl_resolution = '4'

        # Should return asset with 2m in title
        specific_resolution = '2'

        # Should return asset with res of 1 and 2
        valid_range1 = f':{specific_resolution}'
        
        # Should return asset with res of 1 and 2
        valid_range2 = f':{nourl_resolution}'

        # Should return asset with res of 1 and 2
        valid_range3 = '1:'
        
        # Should return asset with res of  2
        valid_range4 = '2:'
        
        df_collections = pandas.DataFrame({'collection': ['hrdem-lidar'], 'asset': ['dsm']})
        params = {'collection_asset':df_collections,'bbox':bbox,'extent_crs':bbox_crs}

        df =  dce.asset_urls(resolution_filter=nourl_resolution,**params)
        urls = [u for u in df.url]
        print(urls)
        #todo: test que la resolution du fichier est celle demandé dans le filtre
        assert len(urls) == 0
        assert len(dce.asset_urls(resolution_filter=specific_resolution,**params).url) >= 4
        assert len(dce.asset_urls(resolution_filter=valid_range1,**params).url) >= 22
        assert len(dce.asset_urls(resolution_filter=valid_range2,**params).url) >= 22
        assert len(dce.asset_urls(resolution_filter=valid_range3,**params).url) >= 22
        assert len(dce.asset_urls(resolution_filter=valid_range4,**params).url) >= 4
        
    def test_asset_urls_stage(self):
        """Test that the code can access the data on stage"""
        bbox = '-75,45,-74,46'
        bbox_crs = 'EPSG:4326'
        #Tests avec la collection de GEOAI seulement sur stage : quickbird-2-ortho-pansharp
        df_collections = pandas.DataFrame({'collection': ['quickbird-2-ortho-pansharp'], 'asset': ['N']})
        params = {'collection_asset':df_collections,'bbox':bbox,'extent_crs':bbox_crs}
        
        df =  dce.asset_urls(**params)
        urls = [u for u in df.url]
        #Seulement un asset devrait être retourné
        # 'http://datacube-stage-data-internal.s3.amazonaws.com/store/imagery/optical/quickbird-2-ortho-pansharp/QC15-052686187030_01_P001-QB02-N.tif'
        assert len(urls) == 1


class TestWcsCoverageExtract:
    """All standard test for every function"""
    poly = DEX.bbox_to_poly(bbox=BBOX_SMALL)
    bbox_as_dict = DEX.poly_to_dict(poly)
    crs = '3979'
    cellsize = 300
    protocol = 'HTTPS'
    lid = 'dtm'
    level = 'stage'
    srv_id = 'elevation'
    study_area = 'wcs_extract'
    suffix = lid
    profile_prediction = "{'driver': 'GTiff', 'dtype': 'float32',"\
        " 'nodata': -32767.0, 'width': 844, 'height': 734, 'count': 1,"\
        " 'crs': CRS.from_epsg(3979), 'transform': Affine(300.0, 0.0,"\
        " -1863127.0, 0.0, -300.0, 532837.0), 'blockxsize': 512,"\
        " 'blockysize': 512, 'tiled': True, 'compress': 'lzw',"\
        " 'interleave': 'band'}"
    pixel_prediction = 1542.896728515625
    def test_wcs_coverage_extract(self):
        """Test and example wcs_coverage_extract"""

        with TemporaryDirectory() as temp_dir:
            print('Preparation')
            img_name=pathlib.Path(os.path.join(temp_dir,"{}_sample-{}.tif".format(
                self.study_area,self.suffix)))
            print('Execution')
            DEX.wcs_coverage_extract(bbox_as_dict=self.bbox_as_dict,
                                     crs=self.crs,
                                     cellsize=self.cellsize,
                                     protocol=self.protocol,
                                     lid=self.lid,
                                     level=self.level,
                                     srv_id=self.srv_id,
                                     cwd=temp_dir,
                                     study_area=self.study_area,
                                     suffix=self.suffix)

            print('Validation')
            img = rasterio.open(img_name)
            profile_result = re.sub('\\s+', ' ', str(img.profile))
            row, col = img.index(-1719911,412986)
            pixel_result = float(img.read(1)[row, col])
            img.close()
        assert profile_result == self.profile_prediction,\
            f"Profiles values are different, it's ({profile_result})"\
                " should be­ ({self.profile_prediction})"
        assert pixel_result == self.pixel_prediction,\
            f"Pixel values are different, it's ({pixel_result})"\
                " should be­ ({self.pixel_prediction})"

    def test_wcs_coverage_extract_overviews(self):
        """Test and example wcs_coverage_extract"""

        with TemporaryDirectory() as temp_dir:
            print('Preparation')
            img_name=pathlib.Path(os.path.join(temp_dir,"{}_sample-{}.tif".format(
                self.study_area,self.suffix)))
            print('Execution')
            DEX.wcs_coverage_extract(bbox_as_dict=self.bbox_as_dict,
                                     crs=self.crs,
                                     cellsize=self.cellsize,
                                     protocol=self.protocol,
                                     lid=self.lid,
                                     level=self.level,
                                     srv_id=self.srv_id,
                                     cwd=temp_dir,
                                     study_area=self.study_area,
                                     method='nearest',
                                     suffix=self.suffix,
                                     overviews=True)
            print('Validation')
            img = rasterio.open(img_name)
            profile_result = re.sub('\\s+', ' ', str(img.profile))
            result_overviews = [img.overviews(i) for i in img.indexes]
            row, col = img.index(-1719911,412986)
            pixel_result = float(img.read(1)[row, col])
            prediction_overviews = [[2]]
            img.close()

        assert profile_result == self.profile_prediction,\
            f"Profiles values are different, it's ({profile_result})"\
                " should be­ ({self.profile_prediction})"
        assert pixel_result == self.pixel_prediction,\
            f"Pixel values are different, it's ({pixel_result})"\
                " should be­ ({self.pixel_prediction})"
        assert result_overviews == prediction_overviews,\
            f"Pixel values are different, it's ({result_overviews})"\
                " should be­ ({prediction_overviews})"
                
    # def test_wcs_coverage_extract_resolution(self):
    #     """Test and example wcs_coverage_extract"""
    
    #     with TemporaryDirectory() as temp_dir:
    #         print('Preparation')
    #         img_name=pathlib.Path(os.path.join(temp_dir,"{}_sample-{}.tif".format(
    #             self.study_area,self.suffix)))
    #         print('Execution')
    #         DEX.wcs_coverage_extract(bbox_as_dict=self.bbox_as_dict,
    #                                  crs=self.crs,
    #                                  cellsize=None,
    #                                  protocol=self.protocol,
    #                                  lid=self.lid,
    #                                  level=self.level,
    #                                  srv_id=self.srv_id,
    #                                  cwd=temp_dir,
    #                                  study_area=self.study_area,
    #                                  suffix=self.suffix)
    #         print('Validation')
    #         resolution_prediction = 20
    #         with rasterio.open(img_name) as img:
    #             resolution_result = math.sqrt(img.res[0]*img.res[1])

    #     assert resolution_result == resolution_prediction,\
    #         f"cellsize values are different, it's ({resolution_result})"\
    #             " should be­ ({resolution_prediction})"


