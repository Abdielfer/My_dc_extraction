# -*- coding: utf-8 -*-
"""
Functional Testing module for extract_cog.py
"""
import pathlib
import shutil
import sys

import rasterio
from rasterio.transform import Affine

_CHILD_LEVEL = 2
DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
sys.path.append(DIR_NEEDED)
import ccmeo_datacube.extract_cog as exc

class TestExtractCog():
    """
    class test for function extract_cog.extract_cog()
    """
    
    
    def test_extract_cog_aws(self, tmp_path):
        """
        test function for extract_cog if collection is a dataset on aws bucket
        """
        print('Preparation')
        # tmp_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_functional'

        params={'collections':'landcover',
                'bbox':'2150469.4724999964, 144975.05299999937, 2200469.4724999964, 194975.05299999937',
                'bbox_crs':'EPSG:3979',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'bilinear',
                'out_dir':tmp_path,
                'suffix':None,
                'debug':False, 
                'overviews':False}
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) != 0, "Output list should be bigger than 0"
        result_raster = rasterio.open(result[0]) #to have the extraction from landcover-2010-classification.tif
        result_profile = result_raster.profile
        result_raster.close()
        
        prediction_profile = {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 0.0, 
                              'width': 1667, 'height': 1668, 'count': 1, 
                              'crs': rasterio.crs.CRS.from_epsg(3979), 
                              'transform': Affine(30.0, 0.0, 2150460.0, 0.0, -30.0, 195000.0), 
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True, 
                              'compress': 'lzw', 'interleave': 'band'}
        
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
            
        
    def test_extract_cog_aws_ovw(self, tmp_path):
        """
        test function extract_cog for collection is a dataset on aws and create overviews
        """
        print('Preparation')
        # tmp_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_functional'

        params={'collections':'landcover',
                'bbox':'2150469.4724999964, 144975.05299999937, 2200469.4724999964, 194975.05299999937',
                'bbox_crs':'EPSG:3979',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'bilinear',
                'out_dir':tmp_path,
                'suffix':None,
                'debug':False, 
                'overviews':True}
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) != 0, "Output list should be bigger than 0"
        
        result_raster = rasterio.open(result[0]) #to have the extraction from landcover-2010-classification.tif
        prediction_overviews = [2, 4]
        assert result_raster.overviews(1) == prediction_overviews,\
            f"Message It's ({result_raster.overviews(1)}) should be­ ({prediction_overviews}) "
        result_raster.close()
        
        
    def test_extract_cog_aws_bbox_4326(self, tmp_path):
        """
        test function extract_cog for bbox crs diff from dataset crs and dataset on aws
        """
        print('Preparation')
        # tmp_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_functional'
        params={'collections':'landcover',
                'bbox':'-116,48,-115,50',
                'bbox_crs':'EPSG:4326',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'suffix':None,
                'debug':False, 
                'overviews':False}
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) != 0, "Output list should be bigger than 0"
        
        # result_raster = rasterio.open(result[0]) #to have the extraction from landcover-2010-classification.tif
        # result_profile = result_raster.profile
        # result_raster.close()
        
        # prediction_profile = {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 0.0, 
        #                       'width': 4662, 'height': 7806, 'count': 1, 
        #                       'crs': rasterio.crs.CRS.from_epsg(3979), 
        #                       'transform': Affine(30.0, 0.0, -1544970.0, 0.0, -30.0, 356400.0), 
        #                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 
        #                       'compress': 'lzw', 'interleave': 'band'}
        
        # assert result_profile == prediction_profile,\
        #     f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
       
            
    def test_extract_cog_minicube(self, tmp_path):
        """
        test function for extract_cog if multiple collection from aws with same input crs are extracted
        """
        # tmp_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_functional'

        params={'collections':'landcover, hrdem-lidar',
                'bbox':'2150469.4724999964, 144975.05299999937, 2151469.4724999964, 145975.05299999937',
                'bbox_crs':'EPSG:3979',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'bilinear',
                'out_dir':tmp_path,
                'suffix':None,
                'debug':False, 
                'overviews':False}
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) != 0, "Output list should be bigger than 0"
        # result_raster = rasterio.open(result[0]) #to have the extraction from landcover-2010-classification.tif
        # result_profile = result_raster.profile
        # result_raster.close()
        
        # prediction_profile = {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 0.0, 
        #                       'width': 34, 'height': 35, 'count': 1, 
        #                       'crs': rasterio.crs.CRS.from_epsg(3979), 
        #                       'transform': Affine(30.0, 0.0, 2150460.0, 0.0, -30.0, 145980.0), 
        #                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 
        #                       'compress': 'lzw', 'interleave': 'band'}
        
        # assert result_profile == prediction_profile,\
        #     f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
    
    
    # def test_extract_cog_minicube_align():
    #     """
    #     test function extract_cog for extraction of wcs and cog from aws and insure that they are aligned
    #     """
        
    
    # def test_extract_cog_minicube_diff_crs(self, tmp_path):
    #     """
    #     test function for exract_cog if multiple collection with not the same crs are extracted
    #     """
        
    #     print('Preparation')
    #     tmp_path = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/test_functional/validation2'

    #     params ={'collections':'worldview-3-ortho-pansharp:N, hrdem-lidar:dsm',
    #             'bbox':'1782400.25674, -77925.54244244, 1784347.4553676, -75607.5444225',
    #             'bbox_crs':'EPSG:3979',
    #             'resolution':None,
    #             'out_crs':'EPSG:2960',
    #             'method':'nearest',
    #             'out_dir':tmp_path,
    #             'suffix':None,
    #             'debug':False, 
    #             'overviews':False}
        
    #     print('Execution')
    #     result = exc.extract_cog(**params)
    #     print('Validation')
        
    def test_extract_cog_not_collection(self, tmp_path):
        """
        test function extract_cog with a collection that does not exist
        """
        print('Preparation')
        params={'collections':'toto',
                'bbox':'-116,48,-115,50',
                'bbox_crs':'EPSG:4326',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'suffix':None,
                'debug':False, 
                'overviews':False}
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
   
        assert not result,\
            f"Collection does not exist, result is ({result}) should be None"      
    
    
    # def test_extract_cog_resample(self):
    #     """
    #     test function for extract_cog if resampling is asked
    #     """
    #Might not be needed... Since some of the other test are doing resampling anyway
    
    
    def test_extract_cog_wcs(self, tmp_path):
        """
        test function for extract_cog if collection is the wcs
        """
        print('Preparation')
        # tmp_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_functional'

        params={'collections':'hrdem-wcs',
                'bbox':'2150469.4724999964, 144975.05299999937, 2200469.4724999964, 194975.05299999937',
                'bbox_crs':'EPSG:3979',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'suffix':None,
                'debug':False, 
                'overviews':False} #This parameter is not used in the extract, need to be added to function
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) != 0, "Output list should be bigger than 0"
        result_raster = rasterio.open(result[0]) #to have the extraction from landcover-2010-classification.tif
        result_profile = result_raster.profile
        result_raster.close()
        
        prediction_profile = {'driver': 'GTiff', 'dtype': 'float32', 'nodata': -32767.0, 
                              'width': 1697, 'height': 1697, 'count': 1, 
                              'crs': rasterio.crs.CRS.from_epsg(3979), 
                              'transform': Affine(30.0, 0.0, 2150454.4724999964, 0.0, -30.0, 195870.05299999937),
                              'blockxsize': 512, 'blockysize': 512, 'tiled': True, 
                              'compress': 'lzw', 'interleave': 'band'}
        
        assert result_profile == prediction_profile,\
            f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
    
    
    def test_extract_cog_wcs_ovw(self, tmp_path):
        """
        test function for extract_cog if collection is the wcs
        """
        print('Preparation')
        # tmp_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\test\test_functional'

        params={'collections':'hrdem-wcs',
                'bbox':'2150469.4724999964, 144975.05299999937, 2230469.4724999964, 204975.05299999937',
                'bbox_crs':'EPSG:3979',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'suffix':None,
                'debug':False, 
                'overviews':True} #This parameter is not used in the extract, need to be added to function
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) != 0, "Output list should be bigger than 0"
        
        result_raster = rasterio.open(result[0]) #to have the extraction from landcover-2010-classification.tif
        prediction_overviews = [2, 4, 8]
        assert result_raster.overviews(1) == prediction_overviews,\
            f"Message It's ({result_raster.overviews(1)}) should be­ ({prediction_overviews}) "
        result_raster.close()
        
    
    def test_extract_cog_mosaic_not_orderby(self, tmp_path):
        """
        test function for extract_cog() if mosaic=True but orderby is not valid
        """
        print('Preparation')
        # tmp_path = '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_functional_extract_cog'
        orderby = "toto"
        params={'collections':'hrdem-lidar:dsm',
                'bbox':'2150469.4724999964, 144975.05299999937, 2230469.4724999964, 204975.05299999937',
                'bbox_crs':'EPSG:3979',
                'resolution':30,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'mosaic':True,
                'orderby':orderby} 
        
        print('Execution')
        try:
            result = exc.extract_cog(**params)
            print('Validation')
            
            assert not result,\
                f"Result is ({result}) should be None, since order by {orderby} is not supported"
        except:
            print('Working')
    
    def test_extract_cog_mosaic_not_resolution(self, tmp_path):
        """
        test function for extract_cog() if mosaic=True but resolution=None (of not defined)
        """
        print('Preparation')
        # tmp_path = '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_functional_extract_cog'

        params={'collections':'hrdem-lidar:dsm',
                'bbox':'2150469.4724999964, 144975.05299999937, 2230469.4724999964, 204975.05299999937',
                'bbox_crs':'EPSG:3979',
                'resolution':None,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'mosaic':True} 
        
        print('Execution')
        try: 
            result = exc.extract_cog(**params)
            print('Validation')
            assert not result,\
            f"Result is ({result}) should be None since no resolution was provided by user with mosaic=True"
        except:
            print('Working')
        
    
    def test_extract_cog_mosaic(self, tmp_path):
        """
        test fonction for extract_cog() if mosaic=True and all parameters are rightfully sets
        """
        print('Preparation')
        # tmp_path = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\mosaic\local\extract_cog_test_functional'

        params={'collections':'hrdem-lidar:dsm',
                'bbox':'2103041, 187788, 2108041, 192788',
                'bbox_crs':'EPSG:3979',
                'resolution':10,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'mosaic':True,
                'orderby':'date',
                'desc':True} 
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) == 1, "Output list should be of lenght 1"
        #Open result file
        # result_raster = rasterio.open(list(result[0].keys())[0]) 
        # result_profile = result_raster.profile
        # result_raster.close()
        
        # prediction_profile = {'driver': 'GTiff', 'dtype': 'float32', 'nodata': -32767.0, 
        #                       'width': 501, 'height': 502, 'count': 1, 'crs': rasterio.crs.CRS.from_epsg(3979), 
        #                       'transform': Affine(10.0, 0.0, 2103040.0, 0.0, -10.0, 192790.0), 
        #                       'blockxsize': 512, 'blockysize': 512, 'tiled': True, 
        #                       'compress': 'lzw', 'interleave': 'band'}
        
        # assert result_profile == prediction_profile,\
        #     f"Message It's ({result_profile}) should be­ ({prediction_profile}) "
     
    
    
    def test_extract_cog_mosaic_multiple_elevation_collections(self, tmp_path):
        """
        test function for extract_cog() if mosaic=True and multiple elevation 
        collections are asked at once
        """
        print('Preparation')
        # tmp_path = '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/pytest/test_functional_extract_cog'

        params={'collections':'hrdem-lidar:dsm, hrdem-lidar:dtm',
                'bbox':'2103041, 187788, 2108041, 192788',
                'bbox_crs':'EPSG:3979',
                'resolution':10,
                'out_crs':'EPSG:3979',
                'method':'nearest',
                'out_dir':tmp_path,
                'mosaic':True,
                'orderby':'date',
                'desc':True} 
        
        print('Execution')
        result = exc.extract_cog(**params)
        print('Validation')
        
        assert len(result) == 2, "Output list should be of lenght 2"

    def test_extract_cog_datetime_filter_range_to(self):
        """Test the datetime filter on extract cog"""
        test_date = '2016-01-01'
        params = _datetime_params()

        # Should return 2010, 2015
        files = exc.extract_cog(datetime_filter=f'../{test_date}',**params)
        assert len(files) == 2
        shutil.rmtree(params['out_dir'],ignore_errors=True)
            
    def test_extract_cog_datetime_filter_range_from(self):
        """Test the datetime filter on extract cog"""
        test_date = '2016-01-01'
        params = _datetime_params()

        # Should only return 2020 
        files = exc.extract_cog(datetime_filter=f'{test_date}/..',**params)
        assert len(files) == 1
        shutil.rmtree(params['out_dir'],ignore_errors=True)
    
    def test_extract_cog_datetime_filter_range(self):
        """Test the datetime filter on extract cog"""
        test_from = '2016-01-01'
        test_to = '2021-01-01'
        params = _datetime_params()

        # Should only return 2020 
        files = exc.extract_cog(datetime_filter=f'{test_from}/{test_to}',**params)
        assert len(files) == 1
        shutil.rmtree(params['out_dir'],ignore_errors=True)
    
    
    def test_extract_cog_resolution_filter(self):
        """Test the resolution_filter input variable """
        resolution_filter = "2"
        params = _resolution_params()
        files = exc.extract_cog(resolution_filter=resolution_filter,**params)
        assert len(files) == 1
        shutil.rmtree(params['out_dir'],ignore_errors=True)  
    def test_extract_cog_resolution_range(self):
        """Test the resolution filter for a range of resolution"""
        resolution_filter = "1:2"
        params = _resolution_params()
        files = exc.extract_cog(resolution_filter=resolution_filter,**params)
        assert len(files) == 4
        shutil.rmtree(params['out_dir'],ignore_errors=True)
    def test_extract_cog_resolution_range_from(self):
        """Test the resolution_filter for a range from a specific resolution to infiniti"""
        resolution_filter = "2:"
        params = _resolution_params()
        files = exc.extract_cog(resolution_filter=resolution_filter,**params)
        assert len(files) == 1
        shutil.rmtree(params['out_dir'],ignore_errors=True)
    def test_extract_cog_resolution_range_to(self):
        """Test the resolution_filter for a range from 0 to specific resolution"""
        resolution_filter = ":1"
        params = _resolution_params()
        files = exc.extract_cog(resolution_filter=resolution_filter,**params)
        assert len(files) == 3
        shutil.rmtree(params['out_dir'],ignore_errors=True)
        
    def test_extract_cog_geom_file_one_poly(self):
        """Test the extraction with a geopackage"""
        
        params = _geom_file_params()
        geopackage = pathlib.Path(__file__).parent/'data/test_single_poly.gpkg'
        files = exc.extract_cog(geom_file=geopackage, **params)
        assert len(files) == 4, print(files)
    def test_extract_cog_geom_file_multi_poly(self):
        """Test the extraction with a geopackage that as multiple polygon"""
        params = _geom_file_params()
        geopackage = pathlib.Path(__file__).parent/'data/test_multi_poly.gpkg'
        field_value = 'bbox'
        field_id = 'nom'
        files = exc.extract_cog(geom_file=geopackage,field_value=field_value,field_id=field_id,**params)
        assert len(files) == 4, print(files)
    def test_extract_cog_geom_file_multi_poly_int(self):
        """Test the extraction with a geopackage that as multiple polygon"""
        params = _geom_file_params()
        geopackage = pathlib.Path(__file__).parent/'data/test_multi_poly.gpkg'
        field_value = 1
        field_id = 'nom_int'
        files = exc.extract_cog(geom_file=geopackage,field_value=field_value,field_id=field_id,**params)
        assert len(files) == 4, print(files)
    def test_extract_cog_geom_file_geojson(self):
        """Test the extraction with a geojson"""
        params = _geom_file_params()
        geojson = pathlib.Path(__file__).parent/'data/test_single_poly_json.geojson'
        files = exc.extract_cog(geom_file=geojson,**params)
        assert len(files) == 4, print(files)
    # Not supported yet, but tbd
    # def test_extract_cog_mosaic_trans_collections():
    #     """
    #     test function for extract_cog() if mosaic=True and user wants to merge multiple collection together
    #     exemple : best of elevation, or something like that
    #     Still needs to be discussion o how we want to implemente those functionalities
    #     """
    #     print('Preparation')
    #     print('Execution')
    #     print('Validation')
            

def _datetime_params():
    tmp_path = pathlib.Path(__file__).parent.joinpath('test_temp')
    if not tmp_path.is_dir():
        tmp_path.mkdir(parents=True,exist_ok=True)
        
    params = {'collections':'landcover',
              'bbox':'-75,45,-74.9,45.1',
              'bbox_crs':'EPSG:4326',
              'out_dir':tmp_path}
    return params

def _resolution_params():
    tmp_path = pathlib.Path(__file__).parent.joinpath('test_temp')
    if not tmp_path.is_dir():
        tmp_path.mkdir(parents=True,exist_ok=True)
        
    params = {'collections':'hrdem-lidar:dsm',
              'bbox':'-73.2667474529999936,45.8250585779999966,-73.2073895590000063,45.8754104470000001',
              'bbox_crs':'EPSG:4326',
              'out_dir':tmp_path}
    return params


def _geom_file_params():
    tmp_path = pathlib.Path(__file__).parent.joinpath('test_temp')
    if not tmp_path.is_dir():
        tmp_path.mkdir(parents=True,exist_ok=True)
        
    params = {'collections':'hrdem-lidar:dsm',
              'out_dir':tmp_path}
    return params
