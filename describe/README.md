# Describe

The describe module allows you to get the coverage extent of collections and items available on the datacube cloud by creating a geopackage (and optional geojson) from a STAC API endpoint results.

> If you are using the [GQIS plugin](https://git.geoproc.geogc.ca/datacube/extraction/plugin-extract), the describe tools are **Get Collection Catalog** (describe.collections(**params)) and **Get Collection Catalog with filters** (describe.search(**params))

# Example code
```python
import dc_extract.describe.describe as d
out_file = {path_and_name_of_geopackage} # Optional defaults to dc_extract/describe/data/{collections_or_search}/dce.gpkg
urls = {List of STAC API urls to scrape} # Optional defaults to datacube prod
# collections and asset descriptions only
d.collections(out_file=out_file,urls=urls)

# Once user has supplied optional filter parameters
bbox = {csv_str_lower_left_upper_right} # Optional defaults to None, no bbox filter applied
datetime = {datetime_str_in_ISO_8601_with_Z} # Optional defaults to None, no bbox filter applied
collections = <csv_collection_ids> #example : 'landcover'
d.search(out_file,urls,bbox={bbox},datetime_filter={datetime},collections=collections)
```
# Description of the 2 modules
## describe.collections()
Gives you information at **collection level**. The geopackage contains the first 3 tables describe in the [GPDK Data model](https://git.geoproc.geogc.ca/datacube/extraction/dc_extract/-/tree/main/describe?ref_type=heads#gpkg-data-model).  
Provides an overview of all the collection available inside the specified STAC API. The information contained in the tables can help you better define your calls for the describe.search() or for extract.extract_cog()
## describe.search()
Gives you information at the **collection and item level**. The geopackage contains all the tables described in the [GPDK Data model](https://git.geoproc.geogc.ca/datacube/extraction/dc_extract/-/tree/main/describe?ref_type=heads#gpkg-data-model). Provides an overview of the coverage of each items available in the specified STAC API, with information beneficial to create your extract.extract_cog() call. 

# GPKG Data model
The geopackage has 4 tables :

_1. dce_collections_  

Polygon layer of the extent of each collections. Also contains some information scrape from the collection STAC like : 
- collection_id;
- start and end datetime;
- ...  

_2. dce_assets_  

Table (no geometry) that contains assets information for each collection in the dce_collections layer. 

_3. dce_collection_assets_  

Polygon layer view that combine the information of the collection and asset for each collection in the dce_collections layer.

_4. dce_items_

Polygon layer that contains geometry for each items inside each collection in the dce_collections layer. Is only available when running the describe.search() module. 


> For more details about the tables, you can have a look at the [ER model](describe/images/ERModel.PNG)
## Depricate documentation
See [code path](https://gccode.ssc-spc.gc.ca/datacube/dc_extract/-/blob/main/describe/doc_md/code_paths.md#code-paths) for more details

