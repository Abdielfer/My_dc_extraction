# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 14:17:45 2023

@author: ccrevier
"""

#Code for concurent processing wth rasterio copied fromthe rasterio doc. 
"""thread_pool_executor.py
Operate on a raster dataset window-by-window using a ThreadPoolExecutor.
Simulates a CPU-bound thread situation where multiple threads can improve
performance.
With -j 4, the program returns in about 1/4 the time as with -j 1.
"""

import concurrent.futures
import multiprocessing
import threading
import pathlib
import sys

import rasterio
from rasterio._example import compute

root = pathlib.Path(__file__).parents[2]
if str(root) not in sys.path:
    sys.path.insert(0,str(root))
    
import ccmeo_datacube.extract as dce  

@dce.print_time
def main(infile, outfile, num_workers=4):
    """Process infile block-by-block and write to a new file
    The output is the same as the input, but with band order
    reversed.
    """

    with rasterio.open(infile) as src:

        # Create a destination dataset based on source params. The
        # destination will be tiled, and we'll process the tiles
        # concurrently.
        profile = src.profile
        profile.update(blockxsize=256, blockysize=256, tiled=True)

        with rasterio.open(outfile, "w", **profile) as dst:
            windows = [window for ij, window in dst.block_windows()]

            # We cannot write to the same file from multiple threads
            # without causing race conditions. To safely read/write
            # from multiple threads, we use a lock to protect the
            # DatasetReader/Writer
            read_lock = threading.Lock()
            write_lock = threading.Lock()

            def process(window):
                with read_lock:
                    src_array = src.read(window=window)

                # The computation can be performed concurrently
                # result = compute(src_array)

                with write_lock:
                    dst.write(src_array, window=window)

            # We map the process() function over the list of
            # windows.
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=num_workers
            ) as executor:
                executor.map(process, windows)


@dce.print_time
def main_without_concurent(infile, outfile):

    with rasterio.Env():

        # Open the source dataset.
        # for infile in list_files:
        with rasterio.open(infile) as src:
            meta = src.meta
            meta.update(blockxsize=256, blockysize=256, tiled='yes')
          
            with rasterio.open(outfile, 'w', **meta) as dst:
                for rc,window in dst.block_windows(1): 
                    data = src.read(window=window)
                    dst.write(data, window=window)



if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Concurrent raster processing demo")
    parser.add_argument("input", metavar="INPUT", help="Input file name")
    # parser.add_argument("output", metavar="OUTPUT", help="Output file name")
    parser.add_argument(
        "-j",
        metavar="NUM_JOBS",
        type=int,
        default=4,#multiprocessing.cpu_count(),
        help="Number of concurrent jobs",
    )
    args = parser.parse_args()
    output = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\CHN-issue\MemoryError\concurent_test\hrdsm_mosaic_from_window_15_test_concurent.tif'
    main(args.input, output, args.j)
    # main_without_concurent(args.input,output )
    
    
    
# example
# python test_concurent_processing.py C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\CHN-issue\MemoryError\input_test_tif\hrdsm_mosaic_from_window_25.tif
  