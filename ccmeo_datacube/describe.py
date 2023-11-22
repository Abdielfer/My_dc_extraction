# -*- coding: utf-8 -*-
"""
Scrapes STAC and returns a Geopackage

Geopackage has 4 tables
-------------------------------------
- dce_collection_asset : the STAC asset types per collection.
                         Provides details on collections and assets.
                         Allows user to define the collections parameter 
                         for extract_cog. The geometry is the collection bbox
- dce_assets : the STAC assets, an attribute table describing assets
- dce_items : the STAC items.  This table is only created from a search.
                               The geometry is the item geometry.
- dce_collection : the STAC collections.  The geometry is the collection bbox.

GeoJSON has four files
----------------------
Files named after and contain information from each table.

Example - basic
---------------
import dc_extract.describe.describe as d

# Create geopackage with collection and asset information
# The default STAC api url list is for datacube prod
d.collections()

# Create a geopackage with collection, item and asset information.
# Use the optional filters
datetime = '2014-01-01T00:00:00Z/2021-01-01T00:00:00Z'
bbox = '-75,45,-74,46'
collections = 'landcover'
d.search(bbox=bbox,datetime_filter=datetime,collections=collections)

"""
import argparse
import os
import pathlib
import sys
from typing import Tuple

# Python custom packages
import geopandas
import pandas
import requests
import shapely
import sqlite3

_CHILD_LEVEL = 1
_DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
if _DIR_NEEDED not in sys.path:
    sys.path.insert(0,_DIR_NEEDED)
    
from ccmeo_datacube.utils import nrcan_requests_ca_patch, print_time, valid_rfc3339

@print_time
@nrcan_requests_ca_patch
def collections(out_file:str='./data/collections/dce.gpkg',
              urls:list= ['https://datacube.services.geo.ca/api'],
              geojson:bool=False):
    
    """
    Scrapes '/collections' endpoint result to collection and asset pgpk.

    Parameters
    ----------
    out_file : str, optional
        The geopackage name and path. The default is './data/dc_extract.gpkg'.
     urls : list, optional
         The STAC API root urls.
         The default is ['https://datacube.services.geo.ca/api']
    geojson : bool, optional
        If geojson output should be created. The default is False.

    Returns
    -------
    None.
    
    Example
    -------
    import dc_extract.describe.describe as d
    d.collections()
    # Geopackage is created with collection, asset tables
    # and a collection_asset view
    """

    # Set up the file names and destination directory
    
    # Set dc_extrac/describe as cwd
    os.chdir(pathlib.Path(__file__).parent)

    # Prepare data dir / subdir and files as required
    name = pathlib.Path(out_file)
    parent = name.parent
    if not parent.is_dir():
        parent.mkdir(parents=True,exist_ok=True)

    # If gpkg exists delete it
    if name.is_file():
      name.unlink(missing_ok=True)

    # Set up seed name for geojson files
    g_name = name.with_suffix('.geojson')

    print('Scraping STAC',file=sys.stdout)

    gdf_c,df_a,gdf_ca = collections_to_gdf(urls[0])
    for url in urls[1:]:
        # Get the default collection, item and asset geodataframes
        c_temp,a_temp,ca_temp = collections_to_gdf(url)

        # Concantenate old and new values
        gdf_c = pandas.concat([gdf_c,c_temp])
        df_a = pandas.concat([df_a,a_temp])
        gdf_ca = pandas.concat([gdf_ca,c_temp])

    print(f'Writing to file {name.absolute()}',file=sys.stdout)
    _create_db(name,gdf_c,df_a,gdf_ca) 
 
    if geojson:
        _write_to_json(g_name,name,gdf_c,df_a,gdf_ca)
        
    print(f'Done.  Data is here : {name.absolute().parent}',file=sys.stdout)
    

@print_time
@nrcan_requests_ca_patch
def search(out_file:str='./data/search/dce.gpkg',
           urls:list= ['https://datacube.services.geo.ca/api'],
           bbox:str=None,
           datetime_filter:str=None,
           collections:str=None,
           geojson:bool=False):
    
    """
    Scrapes '/search endpoint to collection, item, and asset geopackage.

    Parameters
    ----------
    out_file : str, optional
        The geopackage name and path. The default is './data/dc_extract.gpkg'.
     urls : list, optional
         The STAC API root urls.
         The default is ['https://datacube.services.geo.ca/api']
    bbox : str, optional
        The BBOX search filter in EPSG:4326. The default is None.
    datetime_filter : str, optional
        The Datetime search filter in STAC API format ISO 8601.
        The default is None.
    collections : str, optional
        A csv of collections in STAC API format.
        The default is None.
    geojson : bool, optional
        If geojson output should be created. The default is False.

    Returns
    -------
    None.
    
    Example
    -------
    import dc_extract.describe.describe as d
    d.search()
    # Geopackage is created with search result collections, items and assets
    # geopackage and a collection_asset view
    """

    # Set up the file names and destination directory
    
    # Set dc_extrac/describe as cwd
    os.chdir(pathlib.Path(__file__).parent)

    # Prepare data dir / subdir and files as required
    name = pathlib.Path(out_file)
    parent = name.parent
    if not parent.is_dir():
        parent.mkdir(parents=True,exist_ok=True)

    # If gpkg exists delete it
    if name.is_file():
      name.unlink(missing_ok=True)

    # Set up seed name for geojson files
    g_name = name.with_suffix('.geojson')
    
    print('Scraping STAC')

    list_gdf_collection = []
    list_gdf_item = []
    list_df_asset = []
    list_gdf_collection_asset = []
    # Get the first values
    for url in urls :
        gdf_item,df_asset = _items_assets_to_gdf(url,bbox,datetime_filter,collections)
        if gdf_item is not None and df_asset is not None:
            gdf_c,gdf_ca = search_to_gdf(url,df_asset)
            list_gdf_collection.append(gdf_c)
            list_gdf_item.append(gdf_item)
            list_df_asset.append(df_asset)
            list_gdf_collection_asset.append(gdf_ca)

    if list_gdf_collection :
        gdf_collection = pandas.concat(list_gdf_collection)
        gdf_i = pandas.concat(list_gdf_item)
        df_a = pandas.concat(list_df_asset)
        gdf_collection_asset = pandas.concat(list_gdf_collection_asset)

        print(f'Writing to file {name.absolute()}')
        _create_db(name,gdf_collection,df_a,gdf_collection_asset,gdf_i) 

        if geojson:
            _write_to_json(g_name,name,gdf_collection,df_a,gdf_collection_asset,gdf_i)
            
        print(f'Done.  Data is here : {name.absolute().parent}')

    return 

def search_to_gdf(url,df_asset):
    """
    Converts STAC API search results to collection, item, asset geodataframes

    Parameters
    ----------
    url : str, optional
        root url to the stac api. 
        The default is 'https://datacube.services.geo.ca/api'.
    df_asset : str, optional
        The dataframe of asset
    
    Returns
    -------
    gdf_collection : geopandas.GeoDataFrame
        A geodataframe of STAC collection information.
    gdf_collection_asset : geopandas.GeoDataFrame
        A geodataframe of STAC collection and asset per collection information.
    """
    
    # Unique list of collection_ids
    df_cid = geopandas.GeoDataFrame(df_asset['collection_id'].drop_duplicates(),
                                                columns=['collection_id'])
    # Scrape collection info
    gdf_collection = _col_id_to_gdf(df_cid,url)

    # Create collection_asset gdf by merging collection and asset on collection_id
    gdf_collection_asset = gdf_collection.merge(df_asset,on='collection_id')
    
    return gdf_collection,gdf_collection_asset

def collections_to_gdf(url:str='https://datacube.services.geo.ca/api'):
    """
    Converts STAC API collections results to collection, item, asset geodataframes

    Parameters
    ----------
    url : str, optional
        root url to the stac api. 
        The default is 'https://datacube.services.geo.ca/api'.
    

    Returns
    -------
    gdf_collection : geopandas.GeoDataFrame
        A geodataframe of STAC collection information.
    gdf_asset : geopandas.GeoDataFrame
        A geodataframe of STAC asset information, the geometry is null.
    gdf_collection_asset : geopandas.GeoDataFrame
        A geodataframe of STAC collection and asset per collection information.

    """
    collections = []
    assets=[]

    # Get a list of collections from /collections endpoint
    collections_url = f'{url}/collections'
    next_page = collections_url
    returned = 0
    matched = 0
    while next_page:
        r = requests.get(next_page)
        if r.status_code == 200:
            j = r.json()
            # For each collection pull out the collection and asset descriptions
            for coll in j['collections']:
                collection = _parse_collection(coll,url)
                collections.append(collection)
                # Get STAC items
                item_url = f'{url}/collections/{coll["id"]}/items'
                print(f'Scraping assets from {item_url}')
                ri = requests.get(item_url)
                if ri.status_code == 200:
                    ji = ri.json()
                    # Parse out asset description from first item
                    items,t_assets = _parse_items_assets(ji,url,just_one=True)
                    # Append each asset per item to asset list
                    for t_asset in t_assets:
                        assets.append(t_asset)
            

            links = j['links']
            next_page = _get_next_page(links)

        else:
            next_page = None

    
    # Create asset geodataframe
    item_header,asset_header = _ia_headers()

    df_a = pandas.DataFrame(assets,columns=asset_header)
    # df_a['geometry'] = None
    # gdf_asset = geopandas.GeoDataFrame(df_a,geometry='geometry',crs='EPSG:4326')

    # Create collection geodataframe
    header = _c_headers()
    gdf_c = geopandas.GeoDataFrame(collections,columns=header,geometry='geometry',crs='EPSG:4326')
    gdf_ca = gdf_c.merge(df_a,on='collection_id')
    
    return gdf_c,df_a,gdf_ca

def _search_pages(url:str)->list:
    """
    A valid list of urls based on stac api link['next'] for the search endpoint
    
    Franklin STAC API generates a next link even when there is no next page
    (https://datacube.services.geo.ca/api/collections/landcover/items)

    This pagenator verifies the validity of the next link and returns a list
    of valid pages.

    Parameters
    ----------
    url : str
        The stac api endpoint.

    Returns
    -------
    pages : list
        A list of valid page urls to paginate through.
    
    Example
    -------
    url = 'datacube.services.geo.ca/collections/msi/items'
    pages = stac_api_paginate(url)
    for page in pages:
        r = requests.get(page)
        ...

    """
    # Get a list of collections from /collections endpoint
    pages = []
    next_page = url
    returned = 0
    matched = 0
    print(f'Next page {next_page}')
    while next_page:
        print(f'_search_pages {next_page}')
        r = requests.get(next_page)
        if r.status_code == 200:
            pages.append(next_page)
            j = r.json()                     

            # Test the returns total against total matched
            returned += j['context']['returned']
            matched = j['context']['matched']
            if returned < matched:
                links = j['links']
                next_page = _get_next_page(links)
            else:
                next_page = None
        else:
            next_page = None
                            
    r.close()
    return pages

def _items_assets_to_gdf(url:str='https://datacube.services.geo.ca/api',
                        bbox:str=None,
                        dt:str=None,
                        cols:str=None):
    """
    Converts FeatureCollection (search return) STAC to a GeoDataFrame

    Parameters
    ----------
    url : str
        The root STAC API url.
    bbox : str, optional
        BBOX to use as search filter. The default is None.
    dt : str, optional
        Datetime to use as search filter in STAC API formate.
        The default is None.
    cols : str, optional
        A csv of collections in STAC API format.
        The default is None.

    Returns
    -------
    gdf_item : geopandas.GeoDataFrame
        A geodataframe of STAC items information.
    gdf_asset : geopandas.GeoDataFrame
        A geodataframe of STAC asset information, the geometry is null.

    """
        

    items = []
    assets = []
    
    # Append search endpoint and parameters
    search_url = f'{url}/search'
    params = []
    if bbox:
        params.append(f'bbox={bbox}')
    
    if dt:
        datetime_filter = valid_rfc3339(dt)
        if datetime_filter:
            params.append(f'datetime={datetime_filter}')
        else:
            print(f"Invalid datetime filter {dt}")
    if cols:
        params.append(f'collections={cols}')
    if len(params) > 0:
        search_url += f'?{"&".join(params)}'
    
    print(f'Search URL from _items_assets_to_gdf {search_url}')
        # TODO add post functionality to stac api pages
    
    # Do pagination
    pages = _search_pages(search_url)
    for page in pages:
        print(f'Test {page}')
        # Load the STAC API return from search endpoint (items)
        r = requests.get(page) 
        if r.status_code == 200:
            j = r.json()
            temp_is,temp_as = _parse_items_assets(j,url)
            # Append each item to items list
            for temp_i in temp_is:
                items.append(temp_i)
            # Append each asset per item to assets list
            for temp_a in temp_as:
                assets.append(temp_a)
    
    item_header,asset_header = _ia_headers()
    if items :
        # Create item geodataframe
        gdf_item = geopandas.GeoDataFrame(items,
                                        columns=item_header,
                                        geometry='geometry',
                                        crs='EPSG:4326')
        
        # Create asset dataframe from unique asset values
        df_au = pandas.DataFrame(assets,columns=asset_header).drop_duplicates()


        return gdf_item,df_au
    else:
        print(f'No items for api requests : {search_url} ')
        return None,None

def _col_id_to_gdf(coll_df:pandas.DataFrame,url:str):
    """
    Converts collection_ids to a GeoDataFrame     

    Parameters
    ----------
    coll_df : pandas.DataFrame
        A dataframe with unique column values.
    url : str
        The root url.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame with collection level geometry.

    """
    collections = []
    gdf = None
    for cid in coll_df['collection_id'].values:
        c_url = f'{url}/collections/{cid}'
        c = requests.get(c_url)
        if c.status_code == 200:
            c_j = c.json()
            collection_record = _parse_collection(c_j,url)
            collections.append(collection_record)
    if len(collections) > 0:
        header = _c_headers()
        gdf = geopandas.GeoDataFrame(collections,columns=header,
                                 geometry='geometry',
                                 crs='EPSG:4326')
    return gdf

def _parse_collection(j:dict,url:str)->dict:
    """Parses STAC collection results for collection information
    
    The result type Collection is found at the following STAC API endpoints
    /collections (as a list of type collections)
    /collection/{collection_id} (as a single collection)
    """
    cid = j['id']
    try:
        desc = j['description']
    except:
        desc = None
    try:
        title = j['title']
    except:
        title = None
    try:
        keywords = ','.join(j['keywords'])
    except:
        keywords = None
    
    coords = j['extent']['spatial']['bbox'][0]
    bbox = shapely.geometry.box(*(i for i in coords))
    # TODO add temporal extent extraction
    start_dt = j['extent']['temporal']['interval'][0][0]
    end_dt = j['extent']['temporal']['interval'][0][1]
    
    collection_record =[cid,desc,title,keywords,bbox,start_dt,end_dt,url]
    
    return collection_record


def _parse_items_assets(j:dict,url:str,just_one:bool=False)->Tuple[list,list]:
    """Parses STAC FeatureCollection results for item and asset information

    Parameters
    ----------
    j: dict
        The STAC API result as dictionary.
    url: str
        The url to include in the return.
    just_one: bool
        The flag if only a single feature / item should be parsed.
        The default is False.
    
    Return
    -------
    item: list
        The parsed item values as a list.
    assets: list
        The parsed asset values as a list.
    
    the result type FeatureCollection is found at the 
    following STAC API endpoints as a list of type features
    /search
    /collections/{collection_id}/items
    """
    
    asset_template={}
    items = []
    assets = []
    
    if just_one:
        # Only parse the first item
        try:
            features = [j['features'][0]]
        except:
            # No items in collection ... welcome to stage :)
            return items,assets
    else:
        features = j['features']
    # For each item, pull out the items and assets            
    for item in features:
        # For each item parameter pull out the values
        item_id = item['id']
        item_geom = shapely.geometry.shape(item['geometry'])
        item_bbox = ','.join([str(i) for i in item['bbox']])
        item_date = item['properties']['datetime']
        collection_id = item['collection']
        current_assets = item['assets']
        for ak,av in current_assets.items():
            asset = av.copy()
            # Remove href
            try:
                del asset['href']
            except:
                pass
            asset_template.update({ak:asset})

        # Write values per item
        item_record = [item_id,collection_id,item_bbox
                       ,item_geom,item_date,url]
        items.append(item_record)

        # Add the asset template for each collection
        # Add collection_id
        parsed_asset = []
        for key,value in asset_template.items():
            uid = f'{collection_id}_{key}'
            # Parse the asset once per collection
            if uid not in parsed_asset:
                collection_asset = value.copy()
                asset_title = None
                asset_description = None
                asset_roles = None
                for ck,cv in collection_asset.items():
                    if ck == 'title':
                        asset_title = cv
                    if ck == 'description':
                        asset_description = cv
                    if ck == 'roles':
                        # Convert list to csv
                        asset_roles = ','.join(cv)
                
                asset_record = [uid,key,collection_id,asset_title,
                                asset_description,asset_roles,url]
                assets.append(asset_record)
                asset_template = {}
                parsed_asset.append(uid)
    return items,assets

def _ia_headers():
    """The headers used to define columns of item and asset gdfs"""
    
    #TODO, integrate config file that will modify headers and fields parsed
    # Headers to pass to df
    item_header = ['item_id','collection_id','item_bbox','geometry',
                   'item_date','stac_url']
    asset_header = ['collection_asset','asset_key','collection_id','asset_title',
                    'asset_description','asset_roles','stac_url']
    return item_header,asset_header

def _c_headers():
    """The headers used to define columns of the collection gdf"""
    header = ['collection_id','description',
              'title','keywords',
              'geometry','start_datetime','end_datetime','stac_url']
    return header

def _get_next_page(links:list):
    """Returns the next page link or None from STAC API links list"""
    next_page = None
    for link in links:
        if link['rel'] == 'next':
            next_page = link['href']
    return next_page

def _create_db(name:str,gdf_c:geopandas.GeoDataFrame,
               df_a:pandas.DataFrame,
               gdf_ca:geopandas.GeoDataFrame,
               gdf_i:geopandas.GeoDataFrame=None)->str:
    """Creates tables from dataframes and views from sql"""
    # Write out to gpkg
    print('Writing collection table')
    gdf_c.to_file(name, layer='dce_collection', driver="GPKG",mode='w')
    print('Writing collection asset table')
    gdf_ca.to_file(name,layer='dce_collection_asset', driver="GPKG",mode='w')

    print('Writing asset table')
    conn = sqlite3.connect(name)
    df_a.to_sql(name='dce_asset',con=conn,if_exists='replace')
    conn.commit()
    conn.close()
    
    if isinstance(gdf_i,geopandas.GeoDataFrame):
        print('Writing item table')
        gdf_i.to_file(name, layer='dce_item', driver="GPKG",mode='w')

    # TODO decide what else to do with gpkg
    return

def _write_to_json(g_name:str,
                   name:str,
                   gdf_c:geopandas.GeoDataFrame,
                   df_a:pandas.DataFrame,
                   gdf_ca:geopandas.GeoDataFrame,
                   gdf_i:geopandas.GeoDataFrame=None):
    """
    Writes geodataframes to geojson, including collection_asset view

    Parameters
    ----------
    g_name : str
        The geojson name and path.    
    name : str
        The geopackage name and path.
    gdf_c : geopandas.GeoDataFrame
        The collection gdf.
    gdf_a : pandas.DataFrame
        The asset gdf.
    gdf_ca : geopandas.GeoDataFrame
        The collection gdf.
    gdf_i : pandas.GeoDataFrame, optional
        The item gdf. The default is None.

    Returns
    -------
    None.

    """

    # Write out geojson
    print('Writing collection geojson')
    gdf_c.to_file(g_name.with_stem('dce_collection'), driver="GeoJSON")

    print('Writing collection_asset to geojson')
    gdf_ca.to_file(g_name.with_stem('dce_collection_asset'), driver="GeoJSON")

    if isinstance(gdf_i,geopandas.GeoDataFrame):
        print('Writing item geojson')
        gdf_i.to_file(g_name.with_stem('dce_item'), driver="GeoJSON")

    # Write out json
    print('Writing asset json')
    df_a.to_json(g_name.with_stem('dce_asset').with_suffix('.json'),
                 orient='records')
    return

#functions wrapper for CLI
def wrapper_collections(args):
    output_path=args.output_path
    urls=args.urls
    list_urls =[]
    for url in urls.split(','):
        list_urls.append(url)
    
    collections(out_file=output_path, urls=list_urls)
    return
    
def wrapper_search(args):
    output_path=args.output_path
    urls=args.urls
    list_urls =[]
    for url in urls.split(','):
        list_urls.append(url)
        
    bbox=args.bbox
    datetime=args.datetime_filter
    collections=args.collections
    geojson=args.geojson
    
    search(out_file=output_path, 
           urls=list_urls, 
           bbox=bbox, 
           datetime_filter=datetime, 
           collections=collections, 
           geojson=geojson)
    return

# CLI
def _handle_cli():
    """Processes CLI arguments and passes to appropriate function(s)"""
    desc = 'Creates a gpkg of available collections and their specific assets'
    parser = argparse.ArgumentParser(description=desc)
    subparsers = parser.add_subparsers(dest='collections or search', help='Level of description', required=True)

    #to call the describe.collections()
    parser_collections = subparsers.add_parser('collections', help='Provides a gpkg of the collection and asset level description only')

    parser_collections.add_argument('-output_path',
                        type=str,
                        default='./data/collections/dce.gpkg',
                        help='The output directory and name of the gpkg. The default is None, which gets reassigned to dc_extract/describe/data/collections/dce.gpkg.')
    parser_collections.add_argument('-urls',
                        type=str,
                        default='https://datacube.services.geo.ca/api',
                        help="List of STAC API urls to scrape. The default is None, which scrapes the collecitons available inside the datacube prod ['https://datacube.services.geo.ca/api'].")
    parser_collections.set_defaults(func=wrapper_collections)
    
    #To call the decribe.search()
    parser_search = subparsers.add_parser('search', help='Provides a gpkg of the collection, asset and item level description for a subset of collections')

    parser_search.add_argument('-output_path',
                        type=str,
                        default='./data/search/dce.gpkg',
                        help='The output directory and name of the gpkg. The default is None, which gets reassigned to dc_extract/describe/data/collections/dce.gpkg.')
    parser_search.add_argument('-urls',
                        type=str,
                        default='https://datacube.services.geo.ca/api',
                        help='List of STAC API urls to scrape. The default is None, which scrapes the collecitons available inside the datacube prod.')
    parser_search.add_argument('-bbox',
                        type=str,
                        default=None,
                        help='The BBOX search filter in EPSG:4326. The default is None.')
    parser_search.add_argument('-datetime_filter',
                        type=str,
                        default=None,
                        help='The Datetime search filter in STAC API format ISO 8601. The default is None.')
    parser_search.add_argument('-collections',
                        type=str,
                        default=None,
                        help='A csv of collections in STAC API format. The default is None.')
    parser_search.add_argument('-geojson',
                        type=bool,
                        default=False,
                        help='If geojson output should be created. The default is False.')
    parser_search.set_defaults(func=wrapper_search)
    
    
    args = parser.parse_args()
    args.func(args)
    
    return


if __name__ == '__main__':
    _handle_cli()