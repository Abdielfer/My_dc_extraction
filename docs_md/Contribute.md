# Developper guide

- Follow steps 1-3 or the [**User guide**](./Installation.md) to set up your conda environment; 
- If you are working with SPYDER , install the spyder conda environment as describe [here](https://gccode.ssc-spc.gc.ca/datacube/documentation/-/tree/master/Spyder);
- Install **pytest** inside your conda environment
## Good practices to contribute to the development
###THIS SECTION IS UNDER CONSTRUCTION###\

This project follow the co-development guidelines described here : [Guidelines](https://gccode.ssc-spc.gc.ca/datacube/documentation/-/blob/master/python/mr_workflow.md)
### Create and checkout to a local branch
```bash
# Create branch in git bash
git branch <new_branch_name>
# Move to specific branch in git bash
git checkout <new_branch_name>
# Commit changes 
git commit -a -m "Updated dc_extract submodule"
# Push your branch to the remote repo (only need to do it once)
git push -u origin <new_branch_name>
```

### Create a merge request to add your modification to the main
1. Inside the web UI, create a `branch` and `Merge request` from your `issue`;
2. Assign a reviewer to review your code;
3. Do the modification to the code; 
4. Insure you commited et pushed all changes inside your branch;
5. Wait for comments;
6. Merge your changes after reviewer approved your changes.