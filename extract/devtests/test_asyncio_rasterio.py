# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 13:25:22 2023

@author: ccrevier
Reference : https://github.com/rasterio/rasterio/blob/main/examples/async-rasterio.py
"""

"""async-rasterio.py

Operate on a raster dataset window-by-window using asyncio's event loop
and thread executor.

Simulates a CPU-bound thread situation where multiple threads can improve
performance.
"""

import asyncio
import pathlib
import numpy as np
import rasterio
import sys

root = pathlib.Path(__file__).parents[2]
if str(root) not in sys.path:
    sys.path.insert(0,str(root))
    
import ccmeo_datacube.extract as dce  

# from rasterio._example import compute
@dce.print_time
def main(list_files, with_threads=False):

    with rasterio.Env():

        # Open the source dataset.
        for infile in list_files:
            with rasterio.open(infile) as src:
    
                # Create a destination dataset based on source params. The
                # destination will be tiled, and we'll "process" the tiles
                # concurrently.
    
                meta = src.meta
                meta.update(blockxsize=256, blockysize=256, tiled='yes')
                outfile = pathlib.Path(infile).parent.joinpath(f'{pathlib.Path(infile).stem}_out{pathlib.Path(infile).suffix}')
                # print(outfile)
                with rasterio.open(outfile, 'w', **meta) as dst:
    
                    loop = asyncio.get_event_loop()
    
                    # With the exception of the ``yield from`` statement,
                    # process_window() looks like callback-free synchronous
                    # code. With a coroutine, we can keep the read, compute,
                    # and write statements close together for
                    # maintainability. As in the concurrent-cpu-bound.py
                    # example, all of the speedup is provided by
                    # distributing raster computation across multiple
                    # threads. The difference here is that we're submitting
                    # jobs to the thread pool asynchronously.
    
                    
                    async def process_window(window):
    
                        # Read a window of data.
                        data = src.read(window=window)
    
                        # We run the raster computation in a separate thread
                        # and pause until the computation finishes, letting
                        # other coroutines advance.
                        #
                        # The _example.compute function modifies no Python
                        # objects and releases the GIL. It can execute
                        # concurrently.
                        # result = np.zeros(data.shape, dtype=data.dtype)
                        # if with_threads:
                        #     yield from loop.run_in_executor(
                        #                         None, compute, data, result)
                        # else:
                        #     compute(data, result)
    
                        dst.write(data, window=window)
    
                    # Queue up the loop's tasks.
                    tasks = [asyncio.Task(process_window(window))
                             for ij, window in dst.block_windows(1)]
                    # print(tasks)
                    # Wait for all the tasks to finish, and close.
                    loop.run_until_complete(asyncio.wait(tasks))
        loop.close()


@dce.print_time
def main_without_asyncio(list_files, with_threads=False):

    with rasterio.Env():

        # Open the source dataset.
        for infile in list_files:
            with rasterio.open(infile) as src:
                meta = src.meta
                meta.update(blockxsize=256, blockysize=256, tiled='yes')
                outfile = pathlib.Path(infile).parent.joinpath(f'{pathlib.Path(infile).stem}_out{pathlib.Path(infile).suffix}')
                # print(outfile)
                with rasterio.open(outfile, 'w', **meta) as dst:
                    for rc,window in dst.block_windows(1): 
                        data = src.read(window=window)
                        dst.write(data, window=window)
    



# def asyncio_test(outfile, out_profile, list_of_params):

#     with rasterio.Env():

#         # Open the destination dataset
#         with rasterio.open(outfile, mode="w+", **out_profile) as dst:
#         # with rasterio.open(infile) as src:
#             for params in list_of_params:
#                 file = params['file']
#                 extract_params = params['params']
                
                
#                 with rasterio.open(file) as src:
#                     dt = src.dtypes[0]
#                     in_nodata = src.nodata
                    

#             # Create a destination dataset based on source params. The
#             # destination will be tiled, and we'll "process" the tiles
#             # concurrently.

           
           

#                     loop = asyncio.get_event_loop()

#                 # With the exception of the ``yield from`` statement,
#                 # process_window() looks like callback-free synchronous
#                 # code. With a coroutine, we can keep the read, compute,
#                 # and write statements close together for
#                 # maintainability. As in the concurrent-cpu-bound.py
#                 # example, all of the speedup is provided by
#                 # distributing raster computation across multiple
#                 # threads. The difference here is that we're submitting
#                 # jobs to the thread pool asynchronously.

                
#                     async def process_window(window):
                        
#                         input_extent = rasterio.windows.bounds(window,
#                                               transform=src.transform)
#                         dst_window = rasterio.windows.from_bounds(*input_extent, dst.transform)
#                         # Read a window of data.
#                         # window_arr = vrt.read(band,window=src_window)
#                         window_arr = src.read(window=window)
                        
#                         existing_arr = dst.read(window=dst_window)
                        
#                         # print(f'Update values in mosaic with value from : {file}')
#                         # Make an existing window no_data mask
#                         no_data_mask = (existing_arr == dst.nodata)
#                         #Modify window_arr no data to out_img to data
#                         window_arr[window_arr == in_nodata] = dst.nodata
                        
#                         # LOG.debug(f'no_data_mask shape {no_data_mask.shape}')
#                         new_data = existing_arr
                        
#                         new_data[no_data_mask] = window_arr[no_data_mask]
#                         # We run the raster computation in a separate thread
#                         # and pause until the computation finishes, letting
#                         # other coroutines advance.
#                         #
#                         # The _example.compute function modifies no Python
#                         # objects and releases the GIL. It can execute
#                         # concurrently.
#                         # result = np.zeros(data.shape, dtype=data.dtype)
#                         # if with_threads:
#                         #     yield from loop.run_in_executor(
#                         #                         None, compute, data, result)
#                         # else:
#                         #     compute(data, result)
    
#                         dst.write(new_data, window=window)

#                     # Queue up the loop's tasks.
#                     tasks = [asyncio.Task(process_window(window))
#                               for ij, window in src.block_windows(1)]
    
#                     # Wait for all the tasks to finish, and close.
#                     loop.run_until_complete(asyncio.wait(tasks))
#             loop.close()

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(
        description="Concurrent raster processing demo")
    parser.add_argument(
        '-input',
        metavar='INPUT',
        help="Input file name")
    # parser.add_argument(
    #     'output',
    #     metavar='OUTPUT',
    #     help="Output file name")
    parser.add_argument(
        '--with-workers',
        action='store_true',
        help="Run with a pool of worker threads")
    args = parser.parse_args()
    print(type(args.input))
    inputs = args.input.split(',')
    # main(args.input, args.output, args.with_workers)
    # main(inputs, args.with_workers)
    main_without_asyncio(inputs)
