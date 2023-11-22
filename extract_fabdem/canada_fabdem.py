#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 13:21:27 2022

@author: nob000
"""
import sys

# Import based on sys.path
# datacube custom modules
# need to figure out relative to script
for test in sys.path:
    if 'usecase/extract' in test:
        local_reference = True
        break
    else:
        local_reference = False
if local_reference:
    import extract_fabdem as ef
else:
    import extract.extract_fabdem as ef

    

# defaults to window write, bilinear resampling, blocksize 1028, and compression LZW
file = ef.fabdem3979_canada_mosaic()
print(f'created {file}, now building overviews')
ef.build_overviews(file,resampling='nearest')
print(f'Overviews created for {file}')