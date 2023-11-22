#!/usr/bin/python3
# extract test
## references
##   https://www.kite.com/python/answers/how-to-count-frequency-of-unique-values-in-a-numpy-array-in-python
import pathlib
import sys

_child_level = 1
dir_needed = str(pathlib.Path(__file__).parents[_child_level].absolute())
sys.path.append(dir_needed)

import ccmeo_datacube.extract as dce
import os
import subprocess
import shlex
import sys
from datetime import datetime
#Test cases as defs
## import extract_test as et
## minx,maxx,miny,maxy,crs,cellsize=et.getTestVals()
## et.testExtractByCellsParameters(minx,maxx,min,maxy,cellsize)
## or et.testExtractByCellsParameters(*et.getTestVals())


def testScript(study_area='test', outroot='.',subdir='test',script_path='.',
               bbox="-486516.2,1231541.0,-419184.0,1345927.2",
               crs="EPSG:3979",dtype=32,precision=2,csize=16,
               resample='nearest',level='stage'):
    # smaller study area for testing locally
    #bbox="-486516.2,1231541.0,-419184.0,1345927.2"
    # larger study area for testing on HPC
    #bbox="-269985.0,90015.0,-265875.0,95895.0"
    if outroot == '.':
        outroot = os.getcwd()
    if script_path == '.':
        script_path = os.getcwd()
    outpath = e.check_outpath(os.path.join(outroot,subdir))
    ferr = open(os.path.join(outpath,'test_error.log'),'a')
    fout = open(os.path.join(outpath,'test_output.log'),'a')
    now = datetime.now()
    ferr.write('Start {} \n'.format(now))
    fout.write('Start {} \n'.format(now))
    cmd = 'python {} '.format(os.path.join(script_path,'extract.py'))
    cmd += "-id={} -coll=cdem -assettype=dem ".format(study_area)
    cmd += "-goaldtype={} -goalprecision={} -goalcsize={} ".format(dtype,precision,csize)
    cmd += "-resample={} ".format(resample)
    cmd += "-bbox={} ".format(bbox) 
    cmd += "-crs={} -outpath={} -level={} -keeptemp=True".format(crs,outpath,level)
    print("sending command through subprocess.run {}".format(cmd))
    args = shlex.split(cmd)
    out = subprocess.run(args,stdout=fout,stderr=ferr)
    print(out)
    now = datetime.now()
    ferr.write('Finish {} \n'.format(now))
    fout.write('Finish {} \n'.format(now))
    ferr.close()
    fout.close()
    return

def testUseGeoJsonFeatureCollection(fname):
    b,c = e.getBoundsFromFeatureCollection(fname)
    return b,c
def testGetResample(text='bilinear'):
    num = e.getResampleEnum(text)
    return "resample enum for text {} is {}".format(text,num)

def getTestVals(size='small3979'):
    if 'small3979' in size:
        #northish area that spancs dtm/dsm boundary
        minx = -486516.2
        maxx = -419184.0
        miny = 1231541.0
        maxy = 1345927.2
        crs = 'EPSG:3979'
        cellsize = 30
    elif '3979' in size:
        # tbox='-269985.0,90015.0,-265875.0,95895.0'
        minx = -269985.0
        maxx = 90015.0
        miny = -265875.0
        maxy = 95895.0
        crs = 'EPSG:3979'
        cellsize = 30
    elif 'small4326' in size:
        # tbox='-110,47,-108,67'
        minx = -110.0
        maxx = -108.0
        miny = 47.0
        maxy = 67.0
        crs = 'EPSG:4326'
        cellsize = 30
    elif '4326' in size:
        # tbox='-124,60,-110,90'
        minx = -124.0
        maxx = -110.0
        miny = 50.0
        maxy = 90.0
        crs = 'EPSG:3979'
        cellsize = 30
    else:
        minx = -486516.2
        maxx = -419184.0
        miny = 1231541.0
        maxy = 1345927.2
        crs = 'EPSG:3979'
        cellsize = 30
    return minx,maxx,miny,maxy,crs,cellsize


def testExtractByCellsParameters(minx,maxx,miny,maxy,crs,cellsize):
    ## test if getWcsRequestByCellSize properly converts
    ## extract bbox to fit even number of cells
    lengthx = e.getLength(minx,maxx)
    lengthy = e.getLength(miny,maxy)
    cellsx = e.getCells(lengthx,cellsize)
    cellsy = e.getCells(lengthy,cellsize)
    even_cells_x = e.getEvenCellSizeLength(lengthx,cellsize)
    even_cells_y = e.getEvenCellSizeLength(lengthy,cellsize)
    print("minx {}, miny {}, maxx {}, maxy {}, crs {}, cellsize {}"
          .format(minx,miny,maxx,maxy,crs,cellsize))
    print("lengthx {}, lengthy {}, cellsx {}, cellsy {}, even_cells_x {} even_cells_y {}"
          .format(lengthx,lengthy,cellsx,cellsy,even_cells_x,even_cells_y))
    bbox=e.getPolyFromTbox("{},{},{},{}".format(minx,miny,maxx,maxy))
    print("***Calling getWcsRequestByCellSize")
    u,newbox = e.getWcsRequestByCellSize(bbox,crs,
                                    cellsize,protocol='https',
                                    lid='dtm',level='stage',
                                    srv_id='elevation')
    print("***Return from getWcsRequestByCellSize")
    print("u {}".format(u))
    print("newbox {}".format(newbox))

    return

def testExtracts(study_area='test'):
    ## #get dict bbox definition using testing values
    e = dce.DatacubeExtract()
    out_dir = study_area
    out_dir = e.check_outpath(out_dir)
    file_name = 'cog_stac_test.json'
    minx,maxx,miny,maxy,crs,cellsize = getTestVals()
    tbox="{},{},{},{}".format(minx,miny,maxx,maxy)
    g = e.poly_to_dict((e.tbox_to_poly(tbox)))
    print("testing WCS")
    e.getExtractFromWcs('cdem','dem',out_dir,study_area,g,crs,cellsize,'http','dtm','dsm','stage','elevation','./main.log','./error.log')
    #print("testing STAC")
    #e.getExtractFromStac('cdem','dem',out_dir,study_area,g,crs,'./main.log','./error.log')
    #e.makeVectorFile(g,crs,out_dir,file_name)
    return

def getGeoJsonBbox(out_dir='.'):
    ## #get dict bbox definition using testing values
    if out_dir == '.':
        out_dir = os.getcwd()
    file_name = 'test.json'
    minx,maxx,miny,maxy,crs,cellsize=getTestVals()
    tbox = "{},{},{},{}".format(minx,miny,maxx,maxy)
    g = e.getDictFromPoly((e.getPolyFromTbox(tbox)))
    e.makeVectorFile(g,crs,out_dir,file_name)
    return

def testGetTransform():
    #3979
    minx,maxx,miny,maxy,crs,cellsize=getTestVals()
    tbox = "{},{},{},{}".format(minx,miny,maxx,maxy)
    g = e.getDictFromPoly((e.getPolyFromTbox(tbox)))
    g3979 = g
    g4326 = e.getTransformFromDictToDict(g,crs,'EPSG:4326')
    gre = e.getTransformFromDictToDict(g4326,'EPSG:4326','EPSG:3979')
    print("****3979 {}".format(g))
    print("g3979 {}".format(g3979))
    print("g4326 {}".format(g4326))
    print("Reprojected {}".format(gre))
    #4326
    minx,maxx,miny,maxy,crs,cellsize=getTestVals(size='small4326')
    tbox = "{},{},{},{}".format(minx,miny,maxx,maxy)
    g = e.getDictFromPoly((e.getPolyFromTbox(tbox)))
    g4326 = g
    g3979 = e.getTransformFromDictToDict(g,crs,'EPSG:3979')
    gre = e.getTransformFromDictToDict(g3979,'EPSG:3979','EPSG:4326')
    print("****4326 {}".format(g))
    print("g3979 {}".format(g3979))
    print("g4326 {}".format(g4326))
    print("Reprojected {}".format(gre))

def testDtmDsmAreas():
    
    minx,maxx,miny,maxy,crs,cellsize = getTestVals()
    tbox = "{},{},{},{}".format(minx,miny,maxx,maxy)
    g = e.getDictFromPoly((e.getPolyFromTbox(tbox)))
    crs_wcs = 'EPSG:3979'
    outpath = e.check_outpath(os.path.join('.','extract_test'))
    if '3979' in crs:
        g3979 = g
    else:
        g3979 = e.getTransformFromDictToDict(g,crs,'EPSG:3979')
    f_dtm = e.makeWcsBboxFile(level='prod',lid='dtm',crs='EPSG:3979',outpath=outpath)
    f_dsm = e.makeWcsBboxFile(level='prod',lid='dsm',crs='EPSG:3979',outpath=outpath)
    print("*** dtm coverage {}".format(f_dtm))
    print("*** dsm coverage {}".format(f_dsm))
    # define areas that are dtm + dsm (use dsm) and areas where there is no dtm (use dsm)
    inter,diff = e.getGeoJsonBboxDifferences(f_dtm,f_dsm)
    f_dtm_zone = 'dtm_dsm_intersection.json' # intersection is area want to request from dtm
    f_dsm_zone = 'dtm_dsm_difference.json' # difference is area want to request from dsm
    f_original = 'original_request3979.json'
    f_dtm_zone = e.makeVectorFile(inter,crs_wcs,outpath,f_dtm_zone)
    f_dsm_zone = e.makeVectorFile(diff,crs_wcs,outpath,f_dsm_zone)
    f_original = e.makeVectorFile(g3979,crs,outpath,f_original)
    # intersect request area with dtm zone and dtm zone to get the values to pass to wcs getCoverage request
    request_dtm,diff_dtm = e.getGeoJsonBboxDifferences(f_original,f_dtm_zone)
    request_dsm,diff_dsm = e.getGeoJsonBboxDifferences(f_original,f_dsm_zone)
    f_request_dtm = 'request_dtm_intersection.json'
    f_request_dsm = 'request_dsm_intersection.json'
    f_request_dtm = e.makeVectorFile(request_dtm,crs,outpath,f_request_dtm)
    f_request_dsm = e.makeVectorFile(request_dsm,crs,outpath,f_request_dsm)
    print("*** request in dtm zone {}".format(request_dtm))
    print("*** type request_dtm {}".format(type(request_dtm)))
    print("*** request in dsm only zone {}".format(request_dsm))
    print("*** type request_dsm {}".format(type(request_dsm)))
    return

def getSysPath():
    return sys.path
