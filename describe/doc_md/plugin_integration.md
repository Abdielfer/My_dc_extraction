# Plugin integration (ideas)
Two options for initial scraping of stac for the plugin, example results for `STAC API` `prod` and `stage` under `describe/data`
1. Option 1 : Scrape everything (not recommended)
1. Option 2 : Scrape only the collection and asset descriptions (recommended)

## Option 1 : Scrape everything (not recommended)
Scrape everything before any selection is done using search with no filters (not recommended)
<details><summary>Option 1</summary>
    ![option1](/uploads/414b599dfffa20ed0639efbb7aefa539/image.png)
</details>

## Option 2 : Scrape only the collection and asset descriptions (recommended)
Option 2 : only scrape collections and asset types / descriptions before any selection is done, then use search with filter parameters defined by user (recommended)
<details><summary>Option 2</summary>
    ![option2](/uploads/9b1af5abcdf8ac44631f438f6f82eba7/image.png)
</details>
