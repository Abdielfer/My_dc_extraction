# -*- coding: utf-8 -*-
"""
Created on Thu May 18 13:33:01 2023

@author: ccrevier
"""
from pydantic import BaseModel, ValidationError, validator, root_validator, Field
import ccmeo_datacube.extract_cog_validator as exc_validator
import ccmeo_datacube.extract_cog as exc
#To test the extract_cog_validator.py
params = {'collections':'hrdem-lidar:dsm',
            # 'bbox':'-73.2667474529999936,45.8250585779999966,-73.2073895590000063,45.8754104470000001',
            # 'bbox_crs':'EPSG:4326',
            # 'field_value':1,
            # 'field_id':"nom_int",
            'geom_file':r'C:\Users\ccrevier\Documents\Datacube\geoproc\dc_extract\extract_cog\tests\data\test_single_poly.gpkg',
            # 'resolution':"2",
            # 'method':'allo',
            # 'out_crs':'EPSG:3979',
            'out_dir':'this_is_a_string',
            # 'suffix':'suffix',
            # 'overviews':False,
            # 'datetime_filter':'2023-04-23',
            # 'resolution_filter':'2',
            # 'overviews':True,
            # 'mosaic':True,
            # 'orderby':'date', 
            # 'desc':True
            }

#here are the parameters
# params = {'collections':collections,
#             'bbox':bbox,
#             'bbox_crs':bbox_crs,
#             'field_value':field_value,
#             'field_id':field_id,
#             'geom_file':geom_file,
#             'resolution':resolution,
#             'method':method,
#             'out_crs':out_crs,
#             'out_dir':out_dir,
#             'suffix':suffix,
#             'datetime_filter':datetime_filter,
#             'resolution_filter':resolution_filter,
#             'overviews':overviews,
#             'debug':debug,
#             'mosaic':mosaic, 
#             'orderby':orderby, 
#             'desc':desc}
try:
    validated_settings = exc_validator.ExtractCogSetting(**params)
    # validated_settings=exc.extract_cog(**params)
    print(validated_settings)
except ValidationError as e:
    print(e)