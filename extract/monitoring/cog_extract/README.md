# Performance of the cog extraction using different library and methods
Backgroud:\
dc_extract tools allow to extract, among other thing, parts of cog file available inside the AWS bucket. 
The rasterio library was originally used to extract region of interest, but processing time were pretty long, so alternative methods were explore to increase extraction capacity and improve the processing time. The preliminary exploration allowed to implement new extraction method using other python library.
Other dev should be done following those new promissing method to make sure the tools are used at their full potential. 

**Test to compare the performance (running time) between extraction methods implemented.**\
4 main methods -- correspondance on graph :
1. Window read with rasterio and internal tilling, no clip needed because window based read of raster, write with rasterio and internal tilling -- _extract_by_window_
2. Read with rasterio, clip with rasterio.mask, write with rasterio (extract.clip_to_grid(xarray=False) ) -- _rasterio_
3. Read with rioxarray, clip with rio.clip_box, write with rio.to_raster -- _window_rioxarray_
4. Using Warped vrt from gdal and rioxarray -- _warpedVRT_

Output is a log file and a csv table containning the running time of each run.\
Multiple bbox size are tested (5km, 10km, 20km, 40km, 50km, 75km and 100km) for each method.\

![Barplot runtime for bbox extent](./images/run_time_barplot.png)

Observations :
 - bbox as tap window does not change anything on the running time when using rioxarray (Not shown in the graph)
 - Window read-write with rasterio gives output file size always bigger as the other methods\


Futur explorations:
- Compare the change of pixel size (resampling) for the same region of interest (changing number of pixel)
- Track the computing ressources used during processus to be able to assigned efficient number of ressource (HPC?)
- Explore the use of gdal env variable 
- Validate that the fast running time for warpedvrt after the first extraction is not due to caching 

## Performance of the cog extraction using WarpedVRT and rasterio.Env()

2 things were explored :
(1) caching effect on the running time
(2) The value of adding env. variable GDAL_NUM_TREADS='ALL_CPUS' 

Backgroud :\
(1) We want to validate that the fast processing time is not only due to the caching of the image by GDAL\
(2) We want to validate if the environnement variable GDAL_NUM_TREADS='ALL_CPUS' allow to do multithreading when using it during warpedVRT processes. Preliminary exploration during a hackathon had proven that the use of this variable on local work station (i.e. laptop) did not improve running time. 

Test :\
(1) Validate the caching effect
Iterative run of the warpedVRT extraction 7 time using code extract/devtests/test_extract_vrt.extract_vrt_tap() (with the GDAL_NUM_TREADS='ALL_CPUS')
![Scatterplot runtime for 100x100km bbox extent](./images/ALL_CPUS.png)

_Conclusion : Cache seems to be playing a part in the running time_

(2) Validate the use of GDAL_NUM_TREADS='ALL_CPUS'
- Iterative run of the warpedVRT extraction 7 time using code extract/devtests/test_extract_vrt.extract_vrt_tap() **with the GDAL_NUM_TREADS='ALL_CPUS'** 
- Iterative run of the warpedVRT extraction 7 time using code extract/devtests/test_extract_vrt.extract_vrt_tap() **with the GDAL_NUM_TREADS=1**

![Scatterplot runtime for 100x100km bbox extent](./images/ONE_CPU.png)

_Conclusion : GDAL_NUM_TREADS='ALL_CPUS' helps running time_\
_Comparison of the 2_ \
![Scatterplot runtime for 100x100km bbox extent](./images/COMPARED.png)



