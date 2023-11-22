# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 12:58:47 2023

@author: ccrevier
"""

#test the new functionality to filter on resolution 
import pandas
def test_asset_urls_resolution_filter(self):
    """Tests that the resolution filter returns proper asset list"""
    # Should return Nothing, no landcover date matches this
    bbox = '-75,45,-74,46'
    bbox_crs = 'EPSG:4326'

    # Assumption landcover collection contains 2010, 2015, 2020
    # Should return no urls, no landcover item has this datetime
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
    params = {'collection_asset':df_collections,'bbox':bbox,'bbox_crs':bbox_crs}

    df =  dce.asset_urls(resolution_filter=nourl_resolution,**params)
    urls = [u for u in df.url]
    print(urls)
    assert len(urls) == 0
    assert len(dce.asset_urls(resolution_filter=specific_resolution,**params)) == 4
    assert len(dce.asset_urls(resolution_filter=valid_range1,**params)) == 19
    assert len(dce.asset_urls(resolution_filter=valid_range2,**params)) == 19
    assert len(dce.asset_urls(resolution_filter=valid_range3,**params)) == 19
    assert len(dce.asset_urls(resolution_filter=valid_range4,**params)) == 4
    