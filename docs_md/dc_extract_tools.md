## Main CCMEO datacube extraction tools

###### Below are general descriptions of the main functionnalities available in the dc_extract tools with path :

### Browse available dataset 
---
>ccmeo_datacube.describe()

Get catalog of available collection inside the datacube platform. 
Can be use to get collection inside the entire datacube or for a region of interest. The catalog includ information about the collections and assets available for extraction following the STAC specification. 

### Dataset extraction 
---
>ccmeo_datacube.extract_cog()

Extraction of a subset of a datasets based on a region of interest

Available processing :
- Reprojection;
- Resampling

### Extraction of a DEM (DSM or DTM) mosaic 
--- 
>ccmeo_datacube.extract_cog(collection_id='hrdem-lidar', mosaic=True)

Extraction of a subset of the elevation collection ('hrdem-lidar') based on a region of interest into a mosaic. Ability to create multiple mosaic at the same time (ex: dtm and dsm) if needed.  
File can be order by :  
- Datetime (ascending or descending);
- Resolution (ascending or descending).  

Default mosaic creation will use the latest files in priority (date descending) using the reverse painter logic to populate the nodata value with following data. If order by resolution is chosen, latest date will be in priority within the same resolution.

**THINGS TO CONSIDER :**  

- Input parameter `resolution` is mandatory inside the `extract_cog(mosaic=True, resolution=X)`;
- Input paramter `out_crs` is mandatory inside the `extract_cog(mosaic=True, out_crs='EPSG:XXXX')`
- Provide asset_id (ex. `'hrdem-lidar:dsm,hrdem-lidar:dtm'`) to have distinct mosaic from both asset type, otherwise, both asset type will be used to create the mosaic.

_ATM mosaic tools are only supported for elevation collections._  
_ATM mosaic tools can only merge assets from the same collection (intra-collection merging)_
