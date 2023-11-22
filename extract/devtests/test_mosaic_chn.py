#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 13:52:27 2023

@author: chc002
Calls the extract_mosaic_by_window_wrapper with all the right parameters
"""

import extract_mosaic_by_window_wrapper as emw

tmp_path = '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/Julien_big_bbox/HPC-vis/by_window/job_julien'
bbox = '1775625.8392919037,-61408.370247168554,1896969.1451886918,86894.29517093688'
params={'collections': 'hrdem-lidar:dtm',
                'bbox':bbox,
                'bbox_crs':'EPSG:3979',
                'resolution':4,
                'out_crs':'EPSG:3979',
                'method':'bilinear',
                'resolution_filter':'1',
                'out_dir':tmp_path,
                'overviews':False,
				'mosaic':True,
				'orderby':'date',
				'desc':True}

result = emw.extract_mosaic_by_window(**params)