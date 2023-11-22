# User guide
As of version 3.0.0 you can install the tools with its dependancies as a python package. Earlier version of the tools only support suing it as module from [cloning the code repo](#clone-the-git-repo). The 2 ways of installing the tools are decscribed below. 
## Install using the python package
Tag version >= 3.0.0 allow to install the tools and all its dependancies from a local python package with pip.  The same steps are valid for windows and HPC.   
To install the tools, run this command inside your conda env. :
```bash
python -m pip install ccmeo_datacube@git+https://git.geoproc.geogc.ca/datacube/extraction/dc_extract
```
If you don't have a conda env. create one first, and install the tools inside of it. 
```bash
conda create --name <env. name> python=3.10
conda activate <env. name>
python -m pip install ccmeo_datacube@git+https://git.geoproc.geogc.ca/datacube/extraction/dc_extract
```
**The python package is only compatible with python 3.10
### Avantages
No need to managed a conda env. with installation of all the dependencies. It is a faster way to install the tools.
### Warnings 
**Compatibility with conda env. limited** : It should be the last thing you install in your conda env. if you use a custom one. 
## Clone the git repo
If you are working on your **local computer** : 
1. Go to your working directory
2. Right click inside folder
3. `git Bash Here`
4. Follow the steps below

If you are working on **HPC** :
1. Go to your working directory
2. Before using your git command, you can run this line to save your credentials information : `git config credential.helper store`
4. Follow the steps below

### 1. Clone dc_extract git repo
Inside the git command window:   
```bash
git clone https://git.geoproc.geogc.ca/datacube/extraction/dc_extract.git
```
> **[-Information-]** : If you are using your own custom conda env. with all the same dependencies:
> 
>Install the nrcan_ssl package with the following line inside your env. Just know that you cannot perform a conda install inside a conda env. after you perform a pip install. 
> ```bash
> python -m pip install nrcan_ssl@git+https://git.geoproc.geogc.ca/datacube/utilities/nrcan_ssl
> ```

### 2. Create and activate conda environment (**datacube-extract-3-10**)
This conda environment contains all of the librairy dependencies to be able to run the dc_extract tools. \
_If you are working outside of HPC, or if you want to create your own conda environment, follow the steps bellow._

Inside a command prompt window (could be anaconda prompt): 
```bash
#On windows
conda env create -f datacube-extract-3-10_windows.yml
#On linux (HPC)
conda env create -f datacube-extract-3-10_linux.yml

#For less dependency, on both windows or linux (not guaranteed too work)
conda env create -f datacube-extract-3-10.yml
```
_If you are working on HPC, the conda environment is already installed. Follow those steps to activate it inside the HPC command prompt:_
```bash
#Activate env on HPC
source /space/partner/nrcan/geobase/work/opt/miniconda-datacube/etc/profile.d/conda.sh
conda activate datacube-extract-3-10
```

## Whats next? 
Other things you can do...

### Keeping your dc_extract module up to date with latest release
1. Go to your /dc_extract local repo
2. Right click inside folder 
3. `git Bash Here`

Inside the git command window:
```bash
#branch name could be main or specific branch created from tag
git checkout <branch_name>
git pull
```

### Setting your local repo to a specific tag version
1. Go to your /dc_extract local repo
2. Right click inside folder 
3. `git Bash Here`  

Inside the git command window:
```bash
git fetch --all --tags
```
```bash
#Example 1 : Get the latest tag
tag=$(git describe --tags `git rev-list --tags --max-count=1`)
#Checkout the tag version inside a new branch (latest)
git checkout $tag -b latest
```
```bash
#Example 2 : get a specific tag version (ex. v1.03)
git checkout tags/v1.03 -b v1.03-branch
```
```bash
# Check your current version
git describe --tags
```

### Add this repo as a submodule to your gitlab project 
```bash
# Create or go to your workspace / git repo root
cd <your_local_repo>

# Add dc_extract as a git submodule inside your git project
git submodule add https://git.geoproc.geogc.ca/datacube/extraction/dc_extract.git
# Intialise the submodule in the HEAD
git submodule init
# Update the submodule / subdirectory with the remote repo contents
git submodule update
```

### Update dc_extract submodule to latest commited version
```bash
# Create or go to your workspace / git repo root
cd <your_local_repo>
#Update to latest version
git submodule update --remote
```

### Migration to geoproc :
To point your local repo to geoproc instead of gccode, follow these steps :
```bash
# Go to repo root
cd  <local-dc_extract-repo>
# Check the remote url
git remote -v
# Reset the remote repo url
# git remote set-url <remote_repo_name> <new_repo_url> <old_repo_url>
git remote set-url origin https://git.geoproc.geogc.ca/datacube/extraction/dc_extract.git  https://gccode.ssc-spc.gc.ca/datacube/dc_extract.git

### Point your remote submodule to geoproc instead of gccode
Adapt those steps to the dc_extract repo : https://git.geoproc.geogc.ca/geosys/base-wrapper/geosys_api#pointing-your-remote-submodule-to-geoproc-instead-of-gccode
