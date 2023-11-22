# Contains developpement tests on dc_extract.extract functions 

Work in progress..

## Tests for developpement 

### test_io_threading.py
Test to implement multithreading inside the extract workflow. (read-clip-write)

### test_block_window.py
Tests to implement the reading and writing of raster using internal tilling and windows to decrease running time. 

### test_clip_to_grid.py
Bunch of test that can be used to validates the output of the extract.DatacubeExtract.clip_to_grid() function

### test_extract.py
Bunch of test that recreates the alignement error between extraction when input data crs are not the same but output crs are the same (implying the need to reproject one of the dataset)

### test_clip_window_geom.py
Start of the exploration of the validation of the reprojection

### test_extract_old.py

...
