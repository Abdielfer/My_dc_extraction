#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 13:26:22 2023
DESCRIPTION:
------------
Pydantic validator for input paramter of the extract_cog()

Good to know : 
    From : https://docs.pydantic.dev/latest/usage/validators/#root-validators
    As with field validators, root validators can have pre=True, 
    in which case they're called before field validation occurs 
    (and are provided with the raw input data), or pre=False (the default), 
    in which case they're called after field validation.

    Field validation will not occur if pre=True root validators raise an error. 
    As with field validators, "post" (i.e. pre=False) root validators by default
    will be called even if prior validators fail; this behaviour can be changed 
    by setting the skip_on_failure=True keyword argument to the validator. The 
    values argument will be a dict containing the values which passed field validation 
    and field defaults where applicable.

Copyright:
---------
    Developed by Charlotte Crevier
    Crown Copyright as described in section 12 of
    Copyright Act (R.S.C., 1985, c. C-42)
    © Her Majesty the Queen in Right of Canada,
    as represented by the Minister of Natural Resources Canada, 2022

"""
from typing import Dict, Optional, Union
from pydantic import BaseModel, ValidationError, field_validator, model_validator, Field, ConfigDict
from pydantic_core.core_schema import ValidationInfo
import pandas
import geopandas
import rasterio
import pathlib
import re
import datetime as root_datetime
from datetime import datetime

class ExtractCogSetting(BaseModel):
    collections: str
    bbox: Optional[str]=None
    bbox_crs: Optional[str]=None 
    field_value: Optional[Union[str, int, float]]=None
    field_id: Optional[str]=None
    geom_file: Optional[Union[str, pathlib.Path]]=None
    #greater or equel to 0 (insure the value is not negative)
    resolution: Optional[int] = Field(ge=0, default=None) #not sure because it is required if mosaic is True
    method: Optional[str] = 'bilinear'
    out_crs: Optional[str]= None #Not sure, because it is required if mosaic is True
    out_dir: Union[str, pathlib.Path]
    suffix: Optional[str]=None
    datetime_filter: Optional[str]=None
    resolution_filter: Optional[str]=None
    overviews: Optional[bool]= False
    debug: Optional[bool]= False
    mosaic: Optional[bool]= False
    orderby: Optional[str]= None
    desc: Optional[bool]= True
    
    #User cannot includ other values, ValidationError will occur
    model_config = ConfigDict(extra='forbid')
    
    
    @field_validator("orderby")
    def orderby_is_valid(cls, method: Optional[str]) -> Optional[str]:
        # print('orderby_is_valid')
        allowed_set = {"date", "resolution"}
        if method :
            if method not in allowed_set:
                raise ValueError(f'InputParameterError : orderby must be in {allowed_set}, got "{method}"')
            else:
                return method
    
    @field_validator("out_crs") #If out_crs is provided, validate that out_crs is not gepgraphic and return out_crs as rasterio.crs.CRS
    def outcrs_is_not_geographic(cls, crs: Optional[str]) -> Optional[str]:
        # print('outcrs_is_not_geographic')
        if crs :
            crs_CRS = rasterio.crs.CRS.from_string(crs)
            if crs_CRS.is_geographic:
                raise ValueError('InputParameterError : Extraction is only available with projected output crs. '\
                                f'Please provid a projected output crs, not {crs}.')
            else:
                return crs
    
    @field_validator("resolution_filter")
    def resolution_filter_range_is_valid(cls, res_range: Optional[str]) -> Optional[str]:
        # print('resolution_filter_range_is_valid')
        if res_range :
            if ':' in res_range:
                res_min, res_max = res_range.split(':')
                if res_min and res_max and res_min >= res_max:
                    raise ValueError("InputParameterError : resolution_filter range must be format 'min:max'")
                else:
                    return res_range
            else:
                return res_range
    
    @field_validator("collections")
    def asset_specified_by_user(cls, collections: str) -> str:
        # print('asset_specified_by_user')
        list_collections = collections.split(',')
        for col in list_collections:
            if ':' not in col:
                print(f'WARNING : no asset_id specified for collection "{col}", extraction will includ all items with asset type "data".')
        return collections
    
    @field_validator("bbox")
    def coordinates_order_is_valid(cls, bbox : str) -> str:
        # print('coordinates_order_is_valid')
        #Valid coordinate order inside bbox is : xmin, ymin, xmax, ymax
        if bbox:
            errors = ''
            xmin, ymin, xmax, ymax = bbox.split(',')
            if float(xmin) > float(xmax) :
                errors += f"xmin {xmin} > xmax {xmax}; "
            if float(ymin) > float(ymax) :
                errors += f"ymin {ymin} > ymax {ymax}. "
            if errors:
                errors += "bbox coordinates are not in the right order."
                raise ValueError(errors)
            else:
                return bbox
    
    @field_validator("geom_file")
    def geom_valid(cls, geom_file : str) -> str:
        # print('geom_valid')
        #Valid that the extension of the file is .gpkg or geojson, which are the two supported atm
        if geom_file:
            ext = pathlib.Path(geom_file).suffix
            allowed_ext = {".geojson", ".gpkg"}
            if ext not in allowed_ext:
                raise ValueError(f'InputParameterError : "{ext}" is not a supported file extension for extraction extent ("geom_file").')
            #Valid that geopandas can open the file
            try:
                gdf_geom = geopandas.read_file(geom_file)
            except:
                raise ValueError(f'InputParameterError : Not able to open the provided "geom_file" file : {geom_file}.')
        
            return geom_file
    
    @model_validator(mode='before')
    def field_id_in_geopackage(cls, values:Dict) -> str:
        # print('field_id_in_geopackage')
        #Validate that the field id exist in the geopackage
        field_id = values.get('field_id')
        if field_id:
            geom_file = values.get('geom_file')
            gdf_geom = geopandas.read_file(geom_file)
            if field_id in gdf_geom.columns:
                return values
            else:
                raise ValueError(f"InputParameterError : 'field_id' {field_id} does not exist in 'geom_file' : {geom_file}. Please provid a valid field_id")
        else:
            return values
            # return field_id
    
    @field_validator("geom_file")
    def geom_count_is_one(cls, geom_file : str, values: ValidationInfo)-> str:
        # print('geom_count_is_one')
        #Valid that there is only one geom , because atm this is what is supported
        if geom_file :
            gdf_geom = geopandas.read_file(geom_file)
            field_value = values.data['field_value']
            field_id = values.data['field_id']
            if field_value and field_id:
                number_of_geom = len(gdf_geom.loc[gdf_geom[field_id]==field_value])
                if number_of_geom > 1:
                    raise ValueError(f'InputParameterError : Provide unique "field_value" for field "{field_id}". "field_value"="{field_value}" correspond to "{len(gdf_geom.loc[gdf_geom[field_id]==field_value])}" entry.')
                elif number_of_geom == 0:
                    raise ValueError(f'InputParameterError : No geometry for field_name "{field_id}" == "{field_value}". Check if you have the right field type.')
                else:
                    return geom_file
            elif len(gdf_geom) > 1 :
                raise ValueError(f'InputParameterError : Only extraction with single geometry is supported. Specify "field_id" and "field_value" if geopackage has multiple geometry. File provided contains "{len(gdf_geom)}" geometry.')
            else:
                return geom_file
    
    @field_validator("method")
    def resampling_value_is_valid(cls, resampling_method:Optional[str]):
        # print('resampling_value_is_valid')
        # if resampling_method:
        try:
            #For futur dev, potentially change the resampling method input in main code from str to rasterio.enums.resampling 
            value = [r for r in rasterio.enums.Resampling if r.name == resampling_method][0]
        except IndexError:
            print(f"WARNING : Input resampling method '{resampling_method}' does not exist, set resampling method to 'Bilinear'.")
            resampling_method = 'bilinear'
            # value = rasterio.enums.Resampling.bilinear
        return resampling_method
    
    @field_validator("datetime_filter")
    def datetime_filter_is_valid(cls, datetime_filter: Optional[str]) -> Optional[str]:
        # print('datetime_filter_is_valid')
        # Validation for RFC 3339 datetime format, if bad format date_filter is None
        if datetime_filter:
            datetime_filter = valid_rfc3339(datetime_filter)
            if not datetime_filter:
                print('WARNING : The datetime filter ("datetime_filter") passed in is invalid and will not be used.')
            return datetime_filter
    
    @model_validator(mode='before') #Can remove the default parameter for orderby, but maybe we don't want that
    def set_orderby_given_mosaic(cls, values: Dict) -> Dict:
        # print('set_orderby_given_mosaic')
        mosaic = values.get("mosaic")
        method = values.get("orderby")
        if method is None and mosaic is True:
            values["orderby"] = "date"
            #TODO : add a wrning for default value
        return values
    
    @model_validator(mode='before') #Can remove the default parameter for desc, but maybe we don't want that
    def set_desc_given_mosaic(cls, values: Dict) -> Dict:
        # print('set_desc_given_mosaic')
        mosaic = values.get("mosaic")
        method = values.get("desc")
        if method is None and mosaic is True:
            values["desc"] = True
            #TODO add a warking for default value
        return values
    
    @model_validator(mode='before') #mode='before' to run this the model validation before the field validation
    def out_crs_if_mosaic(cls, values: Dict) -> Dict:
        # print('out_crs_if_mosaic')
        mosaic = values.get("mosaic")
        crs = values.get("out_crs")
        if crs is None and mosaic is True:
            raise ValueError('InputParameterError : Output crs ("out_crs") is required when using mosaic functionnality.')
        else:
            return values
    
    @model_validator(mode='before') 
    def res_if_mosaic(cls, values: Dict) -> Dict:
        # print('res_if_mosaic')
        mosaic = values.get("mosaic")
        res = values.get("resolution")
        if res is None and mosaic is True:
            raise ValueError('InputParameterError : Output resolution ("resolution") is required when using mosaic functionnality.')
        else:
            return values
    
    @model_validator(mode='before')
    def bbox_or_geom_file(cls, values: Dict) -> Dict:
        # print('bbox_or_geom_file')
        bbox = values.get("bbox")
        geom = values.get("geom_file")
        if bbox and geom:
            raise ValueError('InputParameterError : "bbox" and "geom_file" cannot be used together, you need to specify one or the other.')
        elif not bbox and not geom:
            raise ValueError('InputParameterError : At least one of "bbox" or "geom_file" is required to execute the extraction.')
        else:
            return values
    
    @model_validator(mode='before') 
    def bbox_crs_if_bbox(cls, values: Dict) -> Dict:
        # print('bbox_crs_if_bbox')
        bbox = values.get("bbox")
        bbox_crs = values.get("bbox_crs")
        if bbox_crs is None and bbox:
            raise ValueError('InputParameterError : "bbox_crs" is required when defining output extent with "bbox".')
        else:
            return values
    
    @model_validator(mode='before') 
    def bbox_crs_when_geom_file(cls, values: Dict) -> Dict:
        # print('bbox_crs_when_geom_file')
        geom = values.get("geom_file")
        bbox_crs = values.get("bbox_crs")
        if bbox_crs and geom:
            print('WARNING : "bbox_crs" is define, but will not be used when input extent is gpkg or geojson ("geom_file").')
        return values
    
    @model_validator(mode='before') 
    def field_id_and_field_value_without_geom(cls, values: Dict) -> Dict:
        # print('field_id_and_field_value_without_geom')
        #Inform that it won't be used if no geom define
        field_value = values.get("field_value")
        field_id = values.get("field_id")
        geom_file = values.get("geom_file")
        if (field_value or field_id) and geom_file is None:
            print('WARNING : "field_value" and "field_id" are only used with "geom_file". Will not be considered for the extraction.')
        return values
    
    @model_validator(mode='before') 
    def field_id_and_field_value(cls, values: Dict) -> Dict:
        # print('field_id_and_field_value')
        #Validate that both parameters are givin together
        field_value = values.get("field_value")
        field_id = values.get("field_id")
        if (field_value is None and field_id) or (field_id is None and field_value):
            raise ValueError('InputParameterError : "field_value" and "field_id" should be used together.')
        else:
            return values
    
def valid_rfc3339(dt:str)->Union[str,None]:
    """ Converts the input to match RFC 3339

        Best effort conversions are done for valid ISO 8601
        The RFC 3339 filters will match those excepted by STAC API item-search

        STAC API item-search datetime filter formats
        --------------------------------------------
        A date-time: "2018-02-12T23:20:50Z"
        A closed interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
        Open intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"

        Parameters
        ----------
        dt: str
            The datetime string to be verified
            RFC 3339 formats are required.  Best effort conversion
            A date-time: "2018-02-12T23:20:50Z"
            A closed interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            Open intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"

        Returns
        -------
        a valid RFC 3339 datetime string or None
    """

    # Split input based on '/'
    # Pass back valid RFC 3339 or None
    parts = dt.split('/')
    if len(parts) > 1:
        dt_from = parts[0]
        dt_to = parts[1]
        if dt_from == '..':
            dt_to = _valid_rfc3339(dt_to)
        elif dt_to == '..':
            dt_from = _valid_rfc3339(dt_from)
        else:
            dt_to = _valid_rfc3339(dt_to)
            dt_from = _valid_rfc3339(dt_from)
        
        if dt_to and dt_from:
            return dt_from + '/' + dt_to
        else:
            return None
    else:
        # Single date check it is valid or return None
        return _valid_rfc3339(dt)


def _valid_rfc3339(dt):
    """Verifies it is a valide ISO 8601 date, then converts to RFC 3339"""

    udt = None
    # Matches any +HH:00 or -HH:00 timezone corrections
    match = r'[+|-]\d{2}:'
    r = re.search(match,dt)

    try:
        if r:
            # Local time,+HH:MM or -HH:MM returned fromisoformat, convert to UTC
            utc = datetime.fromisoformat(dt).astimezone(root_datetime.timezone.utc)
            # Convert to RFC 3339 string
            udt = utc.isoformat()[:-6] + 'Z'
        else:
            # Assume Universal time
            udt = datetime.fromisoformat(dt.replace('Z','')).isoformat() + 'Z'
        return udt
    except:
        return None 

