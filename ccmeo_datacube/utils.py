# -*- coding: utf-8 -*-

'''
DESCRIPTION:
------------
Combines all the utilities that are used by both extract and describe. 
Is the first attempt to remove duplication of code and reuse more foward and unitary function troughout the code

Developed by:
-------------
  Norah Brown - Natural Resources Canada,
  Charlotte Crevier - Natural Resources Canada,
  Marc-André Daviault - Natural Resources Canada,
  Jean-François Bourgon - Natural Resources Canada
  Crown Copyright as described in section 12 of Copyright Act (R.S.C., 1985, c. C-42)
  © Her Majesty the Queen in Right of Canada, as represented by the Minister
  of Natural Resources Canada, 2022
'''

# Python standard library
import datetime as root_datetime
from datetime import datetime
from functools import wraps
import re
import sys
from typing import Union

# Custom import
if sys.platform == 'win32':
    # For the python package is installed
    import nrcan_ssl.ssl_utils as ssl_utils

# --decorators--
def print_time(f):
    def pt_wrapper(*args,**kwargs):
        """Prints start time, end time and total time to execute a function.
        for calls to functions using
        it as a decorator

        Example
        -------
        # Execution time returned
        @print_time
        def function_name(*args,**kwargs):
            ...
            return result

        result,execution_time = function_name()

        """
        start = datetime.now()
        print(f'Start: {start}')
        print(''.rjust(75, '-'))
        result = f(*args,**kwargs)
        end = datetime.now()
        print(''.rjust(75, '-'))
        print(f'End: {datetime.now()}')
        total = (end-start).total_seconds()
        print(f'Total time {total} seconds')
        return result
    return pt_wrapper

def nrcan_requests_ca_patch(f):
    @wraps(f)
    def ca_wrapper(*args,**kwargs):
        """Using the python package of nrcan_ssl, temporarly set the right certificat"""
        if sys.platform == 'win32':
            ssl = ssl_utils.SSLUtils(verbose=False,keep_temp=False)
            ssl.set_nrcan_ssl()

            
            # Execute decorated function
            result = f(*args,**kwargs)

            # Set env var back to original value
            ssl.unset_nrcan_ssl()
        else:
            print('In a linux system, no setting of REQUESTS_CA_BUNDLE')
            # Execute decorated function
            result = f(*args,**kwargs)

        return result
    return ca_wrapper

# --utils functions--

def valid_rfc3339(dt:str)->Union[str,None]:
    # Will probably be deprecate once the pydantic models are in place for both extract_cog and describe.
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