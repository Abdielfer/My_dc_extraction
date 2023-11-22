# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 09:24:01 2023

@author: ccrevier

Test the implementation of input validator for extract_cog using pydantic method class
Librairy pydantic is available inside the datacube-extract-copy conda environment 
datacube-extract-copy : copy of the datacube-extract env. with pydantic added 

list of the things we want to validate from extract_cog():
1. Collection is a string (should be validated inside dex.collection_str_to_df())
2. datetime_filter is valid (a function already exist that validate this, need to be 
                             deconstructed to use the pydantic logic)
Inside subfunctions _mosaic():
- Validate the type of each input value
1. orderby is str
2. resolution is int (could it be float? need to validate inside the code)
3. list_resolutions is list
4. bbox is str
5. bbox_crs is str
6. method is str
7. asset is str
8. out_crs is str
9. overview is bool 
10. mosaic is bool
- Define static default (for optional parameters)
- Constraint some values to a list of available value:
(Could use an enum type instead of a set of string?)
1. orderby available values are ['date', 'resolution']
2. method available values are [nearest, bilinear, cubic, cubic_spline, lanczos, average,
                                mode, gauss, max, min, med, q1, q3, sum, rms]

- More complex validation:

1. Resolution cannot be None, is a int
2. out_crs cannot be None, is a string (potentially perform type modification to
                                        convert to rasterio.crs.CRS.from_string())
3. out_crs is not geographic 
4. If mosaic is provided but not orderby, define order by as 'date'
5. If mosaic is provided but not desc, define desc as True

"""
from typing import Dict, Optional, Union
from pydantic import BaseModel, ValidationError, validator, root_validator, Field 
import pandas
import rasterio
import pathlib
import re
import datetime as root_datetime
from datetime import datetime
#creation of multiple validation function, could be combine in the futur

#The validation will occure before the function _mosaic is called, so we 
#won't need to pass collection and asset since we will be able to print the message
#at line 420 outside of the subfunction

#For the default value, I think we should continue to put them inside the function call instead
#of inside the validator to facilitate the user knowing the default values 




class ExtractCogSetting(BaseModel):
    collections: str
    bbox: str
    bbox_crs: str
    method: Optional[str] = 'bilinear'
    #greater or equel to 0 (insure the value is not negative)
    resolution: Optional[int] = Field(ge=0) #not sure because it is required if mosaic is True
    out_crs: Optional[str]=None #Not sure, because it is required if mosaic is True
    out_dir: Union[str, pathlib.Path]
    suffix: Optional[str]
    datetime_filter: Optional[str]
    overviews: Optional[bool]= False
    debug: Optional[bool]= False
    mosaic: Optional[bool]= False
    orderby: Optional[str]= None
    desc: Optional[bool]= True
    
    #User cannot includ other values, ValidationError will occur
    class Config:
        extra = "forbid"
    
    
    @validator("orderby")
    def orderby_is_valid(cls, method: Optional[str]) -> Optional[str]:
        allowed_set = {"date", "resolution"}
        if method is not None and method not in allowed_set:
            raise ValueError(f"InputParameterError : orderby must be in {allowed_set}, got '{method}'")
        return method
    
    @validator("out_crs") #If out_crs is provided, validate that out_crs is not gepgraphic and return out_crs as rasterio.crs.CRS
    def outcrs_is_not_geographic(cls, crs: Optional[str]) -> Optional[str]:
        if crs :
            crs = rasterio.crs.CRS.from_string(crs)
            if crs.is_geographic:
                raise ValueError('InputParameterError : Extraction is only available with projected output crs. '\
                                f'Please provid a projected output crs, not {crs}.')
        return crs
    
    @validator("bbox")
    def coordinates_order_is_valid(cls, bbox : str) -> str:
        #Valid coordinate order inside bbox is : xmin, ymin, xmax, ymax
        errors = ''
        xmin, ymin, xmax, ymax = bbox.split(',')
        if float(xmin) > float(xmax) :
            errors += f"xmin {xmin} > xmax {xmax}; "
        if float(ymin) > float(ymax) :
            errors += f"ymin {ymin} > ymax {ymax}. "
        if errors:
            errors += "bbox coordinates are not in the right order."
            raise ValueError(errors)
        
        return bbox
    
    @validator("method")
    def resampling_value_is_valid(cls, resampling_method: Optional[str]):
        # if resampling_method:
        try:
            #For futur dev, potentially change the resampling method input in main code from str to rasterio.enums.resampling 
            value = [r for r in rasterio.enums.Resampling if r.name == resampling_method][0]
        except IndexError:
            print(f"WARNING : Input resampling method '{resampling_method}' does not exist, set resampling method to 'Bilinear'")
            resampling_method = 'bilinear'
            # value = rasterio.enums.Resampling.bilinear
        return resampling_method
    
    @validator("datetime_filter")
    def datetime_filter_is_valid(cls, datetime_filter: Optional[str]) -> Optional[str]:
        # Validation for RFC 3339 datetime format, if bad format date_filter is None
        if datetime_filter:
            datetime_filter = valid_rfc3339(datetime_filter)
            if not datetime_filter:
                print('WARNING: The datetime filter passed in is invalid and will not be used')
        return datetime_filter
        
    @root_validator() #Can remove the default parameter for orderby, but maybe we don't want that
    def set_orderby_given_mosaic(cls, values: Dict) -> Dict:
        mosaic = values.get("mosaic")
        method = values.get("orderby")
        if method is None and mosaic is True:
            values["orderby"] = "date"
        return values
    
    @root_validator() #Can remove the default parameter for desc, but maybe we don't want that
    def set_desc_given_mosaic(cls, values: Dict) -> Dict:
        mosaic = values.get("mosaic")
        method = values.get("desc")
        if method is None and mosaic is True:
            values["desc"] = True
        return values
    
    @root_validator(pre=True) #pre=True to run this function first on the input parameters before it is pass to outcrs_is_not_geographic()
    def out_crs_if_mosaic(cls, values: Dict) -> Dict:
        mosaic = values.get("mosaic")
        crs = values.get("out_crs")
        if crs is None and mosaic is True:
            raise ValueError('InputParameterError : Output crs (out_crs) is required when using mosaic functionnality.')
        return values
    
    @root_validator() 
    def res_if_mosaic(cls, values: Dict) -> Dict:
        mosaic = values.get("mosaic")
        res = values.get("resolution")
        if res is None and mosaic is True:
            raise ValueError('InputParameterError : Output resolution is required when using mosaic functionnality.')
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

#To test the function above
params = {'collections':'hrdem-lidar',
          # 'resolution':4,
            # 'method':'allo',
           'bbox':'1508821, -107045, 1713821, -102045',
          # 'bbox_crs':'EPSG:3979',
          'overviews':False,
           # 'mosaic':True,
          'out_dir':'this_is_a_string',
           'out_crs':None,
           'datetime_filter':'2023-04-23'
          }
try:
    validated_settings = ExtractCogSetting(**params)
    print(validated_settings)
except ValidationError as e:
    print(e)
    
# print(validated_settings)