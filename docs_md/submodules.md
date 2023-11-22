# Basic submodule instruction
~~## Creating a git submodule of dc_extract in another git repo~~

### Clone git repo including dc_extract submodule
```bash
git clone --recurse-submodules <git repo link>
#ex. git clone --recurse-submodules https://git.geoproc.geogc.ca/datacube/extraction/dc_extract.git
```

### Create conda environment (datacube-extract)
```bash
#On windows
conda env create -f datacube-extract_windows.yml
#On linux (HPC)
conda env create -f datacube-extract_linux.yml

#For less dependency, on both windows or linux
conda env create -f datacube-extract.yml
```

### Adds submodule to your local repo 
```Bash
# Create or go to your workspace / git repo root
cd <your_local_repo>

### Add dc_extract as a git submodule
git submodule add https://gccode.ssc-spc.gc.ca/datacube/dc_extract.git
# Intialise the submodule in the HEAD
git submodule init
# Update the submodule / subdirectory with the remote repo contents
git submodule update
```

### Update submodule from remote repo
```Bash
# git submodule update --remote should be equivalent of following two commands
git submodule init
git submodule update
```

### Checkout to a local branch
```Bash
# Submodule is a detached HEAD so then 
cd <your_local_repo>
cd dc_extract
git branch <subrepo_main>
git checkout <subrepo_main>
# Commit changes to submodule 
git commit -a -m "Updated dc_extract submodule"
# The first 8 characters of the geosys_api commit hash become the @<id> for the geosys_api file in main remote repo
```

### Define submodule version based one based on a tag
```Bash
# Navigate to your root repo then the dc_extract submodule
cd <your_local_repo>
cd dc_extract

# Fetch the tags from the remote repo
git fetch --all --tags

# List all tags
git tag --list

# Create and checkout your branch that will hold the tagged version
# git checkout  tags/<tag_version_x> -b <local_branch_tag_version_x>
# For checking out tag v1.00 to local repo branch v1.00branch
git checkout tags/v1.00 -b v1.00branch

# Commit the change to the root project
cd ..
git commit -am "dc_extract submod checked out to v1.00"

#Update the remote root repo
git push

```
## Testing local branches for pushing upstream and pulling downstream

### Pushing mods upstream
```Bash
cd <your_local_repo>
cd <submodule_folder>
git branch <submod-main>
git checkout <submod-main>
# make mods
# pushes a version of the <submod-main> branch to repo
git push origin <submod-main>
# pushes the changes to main repo
git push origin <submod-main>:main
```


### Set up a conda environment with dc_extract as an existing git submodule
```bash
conda create -n <env_name> python=3.9
conda activate <env_name>

# Create or go to your workspace
mkdir c:\\<path_to_your_workspace>
cd c:\\<path_to_your_workspace>
```

## Code examples
All examples are part of docstring documentation for module and defs\
html version of the help is available inside /docs_html/{package}.html

```conda activate <env>```

```python
import extract as ex
from extract.extract_cog import extract_cog as exc
help(ex)
help(exc)
```
