# import dc_extract
import os
import time
import hydra 
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf
import util as Udc
import logging 
import multiprocessing

# from wbw_test import checkIn as chIn   ### IMPORTANT ###: DO NOT USE. If two instance of the license are created, it can kill my license. Thank you!!
KMP_DUPLICATE_LIB_OK=True

from osgeo import gdal
from osgeo.gdalconst import *

def logger(cfg: DictConfig, nameByTime):
    '''
    You can log all you want here!
    '''
    logging.info(f"Excecution number: {nameByTime}")
    logging.info(f"Output directory :{cfg['output_dir']}")
    # logging.info(f"dc_search inputs: {cfg.dc_Extract_params.dc_search}")
    # logging.info(f"dc_description inputs: {cfg.dc_Extract_params.dc_describeCollections}")
    # logging.info(f"dc_extract inputs: {cfg.dc_Extract_params.dc_extrac_cog}")

def runFunctionInLoop(csvList, function):
    '''
    Given a list <csvList>, excecute the <function> in loop, with one element from the csv as argument, at the time.  
    '''
    listOfPath = Udc.createListFromCSV(csvList)
    for path in listOfPath:
        if os.path.exists(path):
            with Udc.timeit():
                function(path)
        else:
            print(f"Path not found -> {path}")

def customFunction(pathList):
    pass

def maxParalelizer(function, args):
    '''
    Same as paralelizer, but optimize the pool to the capacity of the current processor.
    '''
    pool = multiprocessing.Pool()
    start_time = time.perf_counter()
    result = pool.map(function,args)
    finish_time = time.perf_counter()
    print(f"Program finished in {finish_time-start_time} seconds")
    print(result)

@hydra.main(version_base=None, config_path=f"config", config_name="mainConfigPC")
def main(cfg: DictConfig):
    
    # nameByTime = U.makeNameByTime()
    # logger(cfg,nameByTime)
    Udc.dc_extraction(cfg)
    # U.multiple_dc_extract_ByPolygonList(cfg)

    
if __name__ == "__main__":
    with Udc.timeit():
        main()  
