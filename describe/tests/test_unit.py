"""
Unit tests for describe

Using local conda env datacube-scrape

TODO
----
Validate counts of collections,assets,items in gpkg based on
pagination(hrdem-lidar)
pagination of collections with multipage mock

"""
# Python standard library
import json
import os
import pathlib
import shutil
import sys

from unittest.mock import Mock,MagicMock,patch

# Custom packages
import geopandas
import pandas
import pytest
from shapely.geometry import Polygon

SUB_DIRS = 2
ROOT_DIR = pathlib.Path(__file__).parents[SUB_DIRS]
print(f'ROOT_DIR {ROOT_DIR}')
if ROOT_DIR not in sys.path:
    sys.path.insert(0,str(ROOT_DIR))

import ccmeo_datacube.describe as d

def data_dir():
    """The 'data' dir under tests"""
    return pathlib.Path(__file__).parent/'data'

def results_dir():
    """The results dir under tests"""
    return data_dir()/'results'

@pytest.fixture
def test_gpkg():
    """Temporary geopackage that is passed in then the parent directory is removed"""
    rd = results_dir()
    if not rd.is_dir():
        rd.mkdir(parents=True,exist_ok=True)
    r = results_dir()/'test_geopackage.gpkg'
    yield r
    # Remove
    shutil.rmtree(rd)

@pytest.fixture(scope='function')
def search_200_with_next():
    """
    Collections:
    monthly-vegetation-parameters-20m-V1, 15 items, 9 assets
    flood-susceptibility, 1 item, 2 assets
    msi 14 items, 2 assets
    """
    # Standard single page response from search
    jf = data_dir()/'stac_search_result.json'
    jfp = jf.open()
    j = json.load(jfp)
    jfp.close()
    yield j

@pytest.fixture(scope='function')
def search_200_no_next():
    """
    Collections:
    monthly-vegetation-parameters-20m-V1, 15 items, 9 assets
    flood-susceptibility, 1 item, 2 assets
    msi 14 items, 2 assets
    """
    # Standard single page response from search
    jf = data_dir()/'stac_search_result.json'
    jfp = jf.open()
    j = json.load(jfp)
    jfp.close()
    yield j

@pytest.fixture(scope='function')
def collection_200():
    # Standard single page response from search
    jf = data_dir()/'stac_collection_result.json'
    jfp = jf.open()
    j = json.load(jfp)
    jfp.close()
    yield j

@pytest.fixture(scope='function')
def collections_200():
    # Standard single page response from collections
    # 17 collections in local version
    jf = data_dir()/'stac_collections_page1of1.json'
    jfp = jf.open()
    j = json.load(jfp)
    jfp.close()
    yield j

@pytest.fixture(scope='function')
def mock_response_search_with_next(search_200_with_next):
    mock_response1 = MagicMock()
    mock_response1.json.return_value = search_200_with_next
    mock_response1.status_code = 200
    yield mock_response1

@pytest.fixture(scope='function')
def mock_response_search_no_next(search_200_no_next):
    """Contains two collections, XX items for first collection YY items for second collection, one asset per collection"""
    mock_response2 = MagicMock()
    mock_response2.json.return_value = search_200_no_next
    mock_response2.status_code = 200
    yield mock_response2

@pytest.fixture(scope='function')
def mock_response_collection(collection_200):
    mock_response3 = MagicMock()
    mock_response3.json.return_value = collection_200
    mock_response3.status_code = 200
    yield mock_response3

@pytest.fixture(scope='function')
def mock_response_collections(collections_200):
    mock_response4 = MagicMock()
    mock_response4.json.return_value = collections_200
    mock_response4.status_code = 200
    yield mock_response4

@pytest.fixture(scope='function')
def mock_geodataframe():
    poly = Polygon([[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]])
    vals = [('a1','b1','c1',poly),('a2','b2','c2',poly),('a3','b3','c3',poly)]
    gdf = geopandas.GeoDataFrame(data=vals,columns=['a','b','c','geometry'])
    yield gdf

@pytest.fixture(scope='function')
def mock_dataframe():
    vals = [('a1','b1','c1'),('a2','b2','c2'),('a3','b3','c3')]
    df = pandas.DataFrame(data=vals,columns=['a','b','c'])
    yield df

@pytest.fixture(scope='function')
def mock_collection_to_gpkg(mock_geodataframe,mock_dataframe):
    with patch('ccmeo_datacube.describe.collections_to_gdf') as mock_c2gd:
        # Return the collection geodataframe, the asset dataframe and the collection_asset geodataframe
        mock_c2gd.return_value = (mock_geodataframe,mock_dataframe,mock_geodataframe)
        yield mock_c2gd


@pytest.fixture(scope='function')
def mock_search_pages():
    # patch _search_pages to return 2 pages to _items_assets_to_gdf
    with patch('ccmeo_datacube.describe._search_pages') as mock_sp:
        mock_sp.return_value = ['page1','page2']
        yield mock_sp

@pytest.fixture(scope='function')
def mock_search_requests_get(mock_response_search_with_next,mock_response_search_no_next,mock_response_collection):
     # patch requests module with two local pages of returns
    with patch('ccmeo_datacube.describe.requests.get') as mock_requests_get:
        # pass first two pages to _items_assets_to_gdf, next three pages to _col_id_to_gdf for two collections flood_susceptability and msi
        mock_requests_get.side_effect = [mock_response_search_with_next,
                                         mock_response_search_no_next,
                                         mock_response_collection,
                                         mock_response_collection,
                                         mock_response_collection]
        yield mock_requests_get
                
class TestCollections():
    """All collections test and associate functions
    
    collections: For each STAC API root url get the dfs, concat the results, create geopackage and optionally geojson
    -> collections_to_gdf:  query the API, pass results for parsing, return gdf of collections, collection_asset and dataframe of asset
    -> collections_to_gdf -> _parse_collection: Parse the collection from STAC API collection page and return a list
    -> collections_to_gdf -> _parse_items_assets: Parse the items and assets from STAC API search or items page and return as a list
    -> collection_to_gdf -> _get_next_page: Use the next link in STAC API to get the next page link
                
    -> _create_db: Creates a geopackage from geodataframe(collection, item, collection_asset) and dataframe(asset)
    -> _write_to_json: Writes dataframes to geojosn files
    """

    def test_collections_to_gdf(self,mock_response_collections):
        with patch('ccmeo_datacube.describe.requests') as mock_requests:
            mock_requests.get.return_value = mock_response_collections
            gdf_c,df_a,gdf_ca = d.collections_to_gdf('url')
            assert len(gdf_c) == 17

        
    def test_collections(self,test_gpkg,mock_collection_to_gpkg):
        d.collections(test_gpkg)
        assert mock_collection_to_gpkg.called
        assert test_gpkg.is_file()

class TestSearch():
    """All of the Search and associated functions tests
    
    search: For each STAC API root url, creates collection, item, asset, collection_asset geopackage and optional geojson

    -> search_to_gdf: Uses item and asset df to create collection and collection_asset df returns all df
    -> search_to_gdf -> _item_assets_to_gdf: for each page, query STAC API, if 200 pass results to be parsed, convert lists to dataframes and return item and asset dataframes
    -> search_to_gdf -> _item_assets_to_gdf -> _search_pages: Use the next link in STAC API to get the total number of pages
    -> search_to_gdf -> _item_assets_to_gdf -> _search_pages -> _get_next_page: Use the next link in STAC API to get the next page link
    -> search_to_gdf -> _item_assets_to_gdf -> _parse_item_assets: Parse the items and assets from STAC API search or items page and return as a list

    -> search_to_gdf -> _col_id_to_gdf: for each colllection_id, query STAC API collections, pass result to be parsed, convert list to dataframe, return dataframe
    -> search_to_gdf -> _col_id_to_gdf -> _parse_collection: Parse the collection from STAC API collection page and return a list
            
    -> _create_db: Creates a geopackage from geodataframe(collection, item, collection_asset) and dataframe(asset)
    -> _write_to_json: Writes dataframes to geojosn files
    """

    def test_search_to_gdf(self,mock_search_pages,mock_search_requests_get):
        """
        2 pages with same collections:
        monthly-vegetation-parameters-20m-V1, 15 items, 9 assets
        flood-susceptibility, 1 item, 2 assets
        msi 14 items, 2 assets
        => 3 collections, 60 items, 13 assets, 27 collection_assets(?) not sure about this one
        """
        # gdf_c,gdf_i,gdf_a,gdf_ca = d.search_to_gdf()
        gdf_i,gdf_a= d._items_assets_to_gdf()
        gdf_c,gdf_ca = d.search_to_gdf('https://datacube.services.geo.ca/api',gdf_a)
        print(len(gdf_c))
        assert mock_search_requests_get.call_count == 5
        assert len(gdf_c) == 3
        assert len(gdf_i) == 60
        assert len(gdf_a) == 13
        # TODO figure out if this 27 is a product of using the same values
        assert len(gdf_ca) == 27 # Should be 13 as well??



    def test_search_gpkg(self,test_gpkg,mock_search_pages,mock_search_requests_get):
        datetime = '2014-01-01T00:00:00Z/2021-01-01T00:00:00Z'
        bbox = '-75,45,-74,46'
        collections = 'landcover'
        d.search(test_gpkg,datetime_filter=datetime,collections=collections,bbox=bbox)
        assert test_gpkg.is_file()
        # TODO need to verify if mock is called
        assert mock_search_requests_get.called

class Debug():
    """Carry over tests and debugs, needs to be cleaned or deleted"""
    def debug_test_geojson_collection_asset(self,test_gpkg,mock_collection_to_gpkg):
        """Only required for debugging geojson"""
        d.collections(test_gpkg,geojson=True)
        
        # The name of the geojson file is based on name and path of gpkg
        g_name = test_gpkg.with_suffix('.geojson')
        g_name = g_name.with_stem('dce_collection_asset')
        assert g_name.is_file()
        
    def debug_test_geojson_search_item(self,test_gpkg,mock_collection_to_gpkg):
        """Only required for debugging geojson"""
        datetime = '2014-01-01T00:00:00Z/2021-01-01T00:00:00Z'
        bbox = '-75,45,-74,46'
        collections = 'landcover'
        d.search(test_gpkg,bbox=bbox,dt=datetime,collections=collections,geojson=True)

        # The name of the geojson file is based on name and path of gpkg
        g_name = test_gpkg.with_suffix('.geojson')
        g_name = g_name.with_stem('dce_item')
        assert g_name.is_file()

    def debug_test_search_to_gdf_with_filter(self,mock_collection_to_gpkg):
        """GDF level search not always required, except for debug
        Tests search with a filter, creates a STAC subset of items, assets and collections"""
        
        #TODO test the fields, values etc returned
        datetime = '2014-01-01T00:00:00Z/2021-01-01T00:00:00Z'
        bbox = '-75,45,-74,46'
        collections = 'landcover'
        c,i,a,ca = d.search_to_gdf(bbox=bbox,dt=datetime,cols=collections)
        print(f'ROOT_DIR {ROOT_DIR}')
        print(len(c),len(i),len(a))
        # assert mock_requests.get.called
        # assert 0 == 1
        assert len(c) == 16
        assert len(i) == 95
        assert len(a) == 46

    def debug_collection_to_gdf(self,mock_collection_to_gpkg):
        """GDF level Test creation of gdfs not required unless debugging"""
        
        c,a,ca = d.collections_to_gdf()
        print(f'ROOT_DIR {ROOT_DIR}')
        print(len(c),len(a))
        # assert mock_requests.get.called
        # assert 0 == 1
        assert len(c) == 16
        assert len(a) == 46

    def debug_search_pages(self):
        url = 'https://datacube.services.geo.ca/api/search?limit=30&next=eyJ0aW1lc3RhbXBBdExlYXN0IjoiMjAyMi0wNy0wNVQyMDo1ODozMC44NTQ3NDRaIiwic2VyaWFsSWRHcmVhdGVyVGhhbiI6NTM0fQ=='
        pages = d._search_pages(url)