# CCMEO datacube extraction tools
Extraction tools for the ccmeo data cube. 
Allows the extraction of data for a study area in [COG](https://www.cogeo.org/) file format. \
Main uses are :
- Creation of a static catalog of the available collection inside the datacube using the [describe modules](https://git.geoproc.geogc.ca/datacube/extraction/dc_extract/-/tree/main/describe?ref_type=heads#describe)
- Extraction of subset of dataset from the ccmeo datacube for a region of interest
- Creation of mosaic from HRDEM for a region of interest

>Descriptions of the main tools can be found [here](docs_md/dc_extract_tools.md)  

**{+ dc_extract tags ≤ 1.6 works with python 3.9  +}  
{+Futur version (dc_extract tags ≥ 2.0) works with python 3.10 +}**

---

<!--TODO : add a table of content ...
 # **Table of content** [[_TOC_]] -->

<details><summary>Open for table of content</summary>

[[_TOC_]]
</details>


# User guide
## Installation
For installation, see [docs_md/Installation.md](./docs_md/Installation.md)

## Python _Quick Start_
Example of the most used feature of the tools with basic parameters. More in depth definition of the tools can be found in the next section.   

```python
import ccmeo_datacube.extract_cog as exc

# Extraction of "cog chips"
params = {'collections':'hrdem-lidar:dtm',
            'bbox':'1800761, -20364,1803678,-14234', 
            'bbox_crs':'EPSG:3979',
            'out_dir':r'./to_delete'}

exc.extract_cog(**params)
```
```python
import ccmeo_datacube.describe as d

# Extraction of the datacube catalog of collection into gpkg
params = {'out_file':r'.\dc-catalog.gpkg'}

d.collections(**params)
```

## Using the extraction tools
For description of the **describe** modules, see [/describe/README.md](./describe/README.md)  
For description of the **data extraction** modules, see [/extract/README.md](./extract/README.md) or [/extract_cog/README.md](./extract_cog/README.md)  

The extraction tools can be use different way: 

##### **Command line interface (CLI)**
---
_Example_ when cloning the repo
```bash
# Activate the conda environment
conda activate datacube-extract-3-10
# Set path to the extract_cog folder
set path_extract_cog=<path_to_dc_extract_folder>/ccmeo_datacube
# Call the dc_extract CLI
python %path_extract_cog%/extract_cog.py <extract_cog_params>
```
_Example_ with package
```bash
# Activate the conda environment where you installed the package
conda activate <conda env. name>
# Call the CLI help to get information on command
ccmeo_datacube-describe collections -h
# or
ccmeo_datacube-extract_cog -h
```
##### **Python module**
---
Examples are in the _Python Quick Start_ section.

> **If you decide to use spyder as IDE:**   
> Steps to install a conda env. with spyder and set up the spyder-kernels can be found [here](https://gccode.ssc-spc.gc.ca/datacube/documentation/-/tree/master/Spyder). If you are working on HPC, both environment are already available. 

##### **QGIS plugin**
>To use the extraction tools inside the plugin, see the [plugin-extract git repo](https://git.geoproc.geogc.ca/datacube/extraction/plugin-extract) for more informations.

The only dependency needed to run the dc-extract tools inside the **Extract plugin** is the datacube-extract conda env. 

##### **OCG API**
TBD

### Contribute with your ideas?
You want to contribute to the development or provide feedback on the project? \
You can create [issues](https://git.geoproc.geogc.ca/datacube/extraction/dc_extract/-/issues) in the gitlab project for :
- Problem/error in code or output result
- Ideas to enhence the tools
- Other ...

# Developper guide
To contribute to the code, see [docs_md/Contribute.md](./docs_md/Contribute.md)

# Files and folders
###THIS SECTION IS UNDER CONSTRUCTION###\
### Folders
Main folders
- **./ccmeo_datacube** : Principal function of the toolbox. 
- **./extract** : Folder for tests, and developpment link to the ccmeo_datacube/extract module
- **./extract_cog** : Folder for tests and development link to the wrapper around the ccmeo_datacube/extract.py functions allowing for the extraction of cogs for a region of interest and for the creation of a mosaic
- **./describe** : Folder for tests and development link the ccmeo_datacube/describe module to create static catalog of available collection and dataset inside the datacube as gpkg or geojson file

# Support 
Main developper team :\
charlotte.crevier@nrcan-rncan.gc.ca \
norah.brown@nrcan-rncan.gc.ca\
marc-andre.daviault@nrcan-rncan.gc.ca

### Communicate a problem?
You want to communicate a problem you encountered when running the code? \
You can create [issues](https://git.geoproc.geogc.ca/datacube/extraction/dc_extract/issues) in the gitlab project. \
When your issue is related to an **error encounter while using the code**, please use the issue template **error issue** to communicate your problem, and follow the instructions inside the template.

>When creating issues, **please** be as specific as possible and use label related to the your demand. 
Label definitions can be found [here](https://git.geoproc.geogc.ca/datacube/extraction/dc_extract/-/labels).
