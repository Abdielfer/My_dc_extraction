import os, ntpath
import glob
import pathlib
import shutil
from time import strftime
from typing import Tuple, List
import pandas as pd
import numpy as np
from numpy import linspace
import matplotlib.pyplot as plt
import torch
import rasterio as rio
from rasterio.plot import show_hist
# from rasterio.enums import Resampling
from datetime import datetime
from whitebox.whitebox_tools import WhiteboxTools, default_callback
# import whitebox_workflows as wbw   
from torchgeo.datasets.utils import download_url
from osgeo import gdal,ogr, osr
from osgeo import gdal_array
from osgeo.gdalconst import *
import geopandas as gpd
gdal.UseExceptions()

import pcraster as pcr
from pcraster import *

from omegaconf import DictConfig, OmegaConf
from hydra.utils import instantiate
import yaml

### General applications ##
class timeit(): 
    '''
    to compute execution time do:
    with timeit():
         your code, e.g., 
    '''
    def __enter__(self):
        self.tic = datetime.now()
    def __exit__(self, *args, **kwargs):
        print('runtime: {}'.format(datetime.now() - self.tic))

def seconds_to_datetime(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours}:{minutes}:{seconds}'

def makeNameByTime():
    name = strftime("%y%m%d%H%M")
    return name

### Configurations And file management
def importConfig():
    with open('./config.txt') as f:
        content = f.readlines()
    # print(content)    
    return content

def getLocalPath():
    return os.getcwd()

def makePath(str1,str2):
    return os.path.join(str1,str2)

def ensureDirectory(pathToCheck:os.path)->os.path:
    if not os.path.isdir(pathToCheck): 
        os.mkdir(pathToCheck)
        print(f"Confirmed directory at: {pathToCheck} ")
    return pathToCheck

def relocateFile(inputFilePath, outputFilePath):
    '''
    NOTE: @outputFilePath must contain the complete filename
    Sintax:
     @shutil.move("path/to/current/file.foo", "path/to/new/destination/for/file.foo")
    '''
    shutil.move(inputFilePath, outputFilePath)
    return True

def makeFileCopy(inputFilePath, outputFilePath):
    try:
        shutil.copy(inputFilePath, outputFilePath)
        return outputFilePath
    except shutil.SameFileError:
        print("Source and destination represents the same file.")
    except PermissionError:
        print("Permission denied.")
    except:
        print("Error occurred while copying file.")

def removeFile(filePath):
    try:
        os.remove(filePath)
        return True
    except OSError as error:
        print(error)
        print("File path can not be removed")
        return False

def removeFilesBySubstring(parentPath,subStr:str=''):
    list = listALLFilesInDirBySubstring_fullPath(parentPath,subStr)
    for i in list:
        removeFile(i)

def createTransitFolder(parent_dir_path, folderName:str = 'TransitDir'):
    path = os.path.join(parent_dir_path, folderName)
    ensureDirectory(path)
    return path

def clearTransitFolderContent(path:str, filetype = '/*'):
    '''
    NOTE: This well clear dir without removing the parent dir itself. 
    We can replace '*' for an specific condition ei. '.tif' for specific fileType deletion if needed. 
    @Arguments:
    @path: Parent directory path
    @filetype: file type to delete. @default ='/*' delete all files. 
    '''
    files = glob.glob(path + filetype)
    for f in files:
        os.remove(f)
    return True

def extractNamesListFromFullPathList(pathlist, initialValues:list=[])-> list:
    '''
    From the list of path extract the names, then split by underscore 
    character and returns a list of the last substring from each element in the input.
    '''
    if initialValues:
        listOfNames = initialValues
    else:
        listOfNames =[]
    for path in pathlist:
        _,tifName,_ = get_parenPath_name_ext(path)
        listOfNames.append(tifName.split("_")[-1])
    return listOfNames

def listFreeFilesInDirByExt(cwd:str, ext = '.tif'):
    '''
    @ext = *.tif by default.
    NOTE:  THIS function list only files that are directly into <cwd> path. 
    '''
    cwd = os.path.abspath(cwd)
    # print(f"Current working directory: {cwd}")
    file_list = []
    for (root, dirs, file) in os.walk(cwd):
        for f in file:
            # print(f"File: {f}")
            _,_,extent = get_parenPath_name_ext(f)
            if extent == ext:
                file_list.append(f)
    return file_list

def listFreeFilesInDirByExt_fullPath(cwd:str, ext = '.csv') -> list:
    '''
    @ext = *.csv by default.
    NOTE:  THIS function list only files that are directly into <cwd> path. 
    '''
    cwd = os.path.abspath(cwd)
    # print(f"Current working directory: {cwd}")
    file_list = []
    for (root,_, file) in os.walk(cwd, followlinks=True):
        for f in file:
            # print(f"Current f: {f}")
            _,extent = splitFilenameAndExtention(f)
            # print(f"Current extent: {extent}")
            if ext == extent:
                file_list.append(os.path.join(root,f))
    return file_list

def listFreeFilesInDirBySubstring_fullPath(cwd:str, substring = '') -> list:
    '''
    @substring: substring to be verify onto the file name. 
    NOTE:  THIS function list only files that are directly into <cwd> path. 
    '''
    cwd = os.path.abspath(cwd)
    # print(f"Current working directory: {cwd}")
    file_list = []
    for (root,_, file) in os.walk(cwd, followlinks=True):
        for f in file:
            if substring.lower() in f.lower():
                file_list.append(os.path.join(root,f))
    return file_list

def listALLFilesInDirByExt(cwd, ext = '.csv'):
    '''
    @ext = *.csv by default.
    NOTE:  THIS function list ALL files that are directly into <cwd> path and children folders. 
    '''
    fullList: list = []
    for (root, _, _) in os.walk(cwd):
         fullList.extend(listFreeFilesInDirByExt(root, ext)) 
    return fullList

def listALLFilesInDirByExt_fullPath(cwd, ext = '.csv'):
    '''
    @ext: NOTE <ext> must contain the "." ex: '.csv'; '.tif'; etc...
    NOTE:  THIS function list ALL files that are directly into <cwd> path and children folders. 
    '''
    fullList = []
    for (root, _, _) in os.walk(cwd):
        # print(f"Roots {root}")
        localList = listFreeFilesInDirByExt_fullPath(root, ext)
        # print(f"Local List len :-->> {len(localList)}")
        fullList.extend(localList) 
    return fullList

def listALLFilesInDirBySubstring_fullPath(cwd, substring = '.csv')->list:
    '''
    @substring: substring to be verify onto the file name.    NOTE:  THIS function list ALL files that are directly into <cwd> path and children folders. 
    '''
    fullList = []
    for (root, _, _) in os.walk(cwd):
        # print(f"Roots {root}")
        localList = listFreeFilesInDirBySubstring_fullPath(root, substring)
        # print(f"Local List len :-->> {len(localList)}")
        fullList.extend(localList) 
    return fullList

def createListFromCSV(csv_file_location, delim:str =','):  
    '''
    @return: list from a <csv_file_location>.
    Argument:
    @csv_file_location: full path file location and name.
    '''       
    df = pd.read_csv(csv_file_location, index_col= None, header=None, delimiter=delim)
    out = []
    for i in range(0,df.shape[0]):
        out.append(df.iloc[i,:]) 
    return out

def createCSVFromList(pathToSave: os.path, listData:list):
    '''
    This function create a *.csv file with one line per <lstData> element. 
    @pathToSave: path of *.csv file to be writed with name and extention.
    @listData: list to be writed. 
    '''
    parentPath,name,_ = get_parenPath_name_ext(pathToSave)
    textPath = makePath(parentPath,(name+'.csv'))
    with open(textPath, 'w') as output:
        for line in listData:
            output.write(str(line) + '\n')
    # read_file = pd.read_csv (textPath)
    # print(f'Creating CSV at {pathToSave}')
    # read_file.to_csv (pathToSave, index=None)
    # removeFile(textPath)
    return True

def createListFromCSVColumn(csv_file_location, col_idx, delim:str =','):  
    '''
    @return: list from <col_id> in <csv_file_location>.
    Argument:
    @csv_file_location: full path file location and name.
    @col_idx : number or str(name)of the desired collumn to extrac info from (Consider index 0 <default> for the first column, if no names are assigned in csv header.)
    @delim: Delimiter to pass to pd.read_csv() function. Default = ','.
    '''       
    x=[]
    df = pd.read_csv(csv_file_location,index_col=None, delimiter = delim)
    if isinstance(col_idx,str):  
        colIndex = df.columns.get_loc(col_idx)
    elif isinstance(col_idx,int): 
        colIndex = col_idx
    fin = df.shape[0] ## rows count.
    for i in range(0,fin): 
        x.append(df.iloc[i,colIndex])
    return x
 
def createListFromExelColumn(excell_file_location,Sheet_id:str, col_idx:str):  
    '''
    @return: list from <col_id> in <excell_file_location>.
    Argument:
    @excell_file_location: full path file location and name.
    @col_id : number of the desired collumn to extrac info from (Consider index 0 for the first column)
    '''       
    x=[]
    df = pd.ExcelFile(excell_file_location).parse(Sheet_id)
    for i in df[col_idx]:
        x.append(i)
    return x

def splitFilenameAndExtention(file_path):
    '''
    pathlib.Path Options: 
    '''
    fpath = pathlib.Path(file_path)
    extention = fpath.suffix
    name = fpath.stem
    return name, extention 

def createShpList(parentDir)-> os.path:
    listOfPath = listALLFilesInDirByExt_fullPath(parentDir,ext='.shp')
    OutCSVPath = os.path.join(parentDir,'listOfShpFiles.csv')
    createCSVFromList(OutCSVPath,listOfPath)
    return OutCSVPath 

def remove_duplicates_ordered(input_list)->list:
    seen = set()
    return [x for x in input_list if not (x in seen or seen.add(x))]

def replaceExtention(inPath,newExt: str)->os.path :
    '''
    Just remember to add the poin to the new ext -> '.map'
    '''
    dir,fileName = ntpath.split(inPath)
    _,actualExt = ntpath.splitext(fileName)
    return os.path.join(dir,ntpath.basename(inPath).replace(actualExt,newExt))

def get_parenPath_name_ext(filePath):
    '''
    Ex: user/folther/file.txt
    parentPath = pathlib.PurePath('/src/goo/scripts/main.py').parent 
    parentPath => '/src/goo/scripts/'
    parentPath: can be instantiated.
         ex: parentPath[0] => '/src/goo/scripts/'; parentPath[1] => '/src/goo/', etc...
    '''
    parentPath = pathlib.PurePath(filePath).parent
    name,ext = splitFilenameAndExtention(filePath)
    return parentPath, name, ext
  
def addSubstringToName(path, subStr: str, destinyPath = None) -> os.path:
    '''
    @path: Path to the raster to read. 
    @subStr:  String o add at the end of the origial name
    @destinyPath (default = None)
    '''
    parentPath,name,ext= get_parenPath_name_ext(path)
    if destinyPath != None: 
        return os.path.join(destinyPath,(name+subStr+ext))
    else: 
        return os.path.join(parentPath,(name+subStr+ ext))

def replaceName_KeepPathAndExt(path, newName: str) -> os.path:
    '''
    @path: Path to the raster to read. 
    @subStr:  String o add at the end of the origial name
    @destinyPath (default = None)
    '''
    parentPath,_,ext= get_parenPath_name_ext(path)
    return os.path.join(parentPath,(newName+ext))

def overWriteHydraConfig(hydraYML, newParams) -> bool:
    with open(hydraYML, 'r') as f:
        data = yaml.safe_load(f)

    # Add new parameter
    for key, value in newParams.items():
        data[key] = value

    # Write data back to the file
    try:
        with open(hydraYML, 'w') as f:
            yaml.safe_dump(data, f)
            print("File written successfully")
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    
    return True

def updateDict(dic:dict, args:dict)->dict:
    outDic = dic
    for k in args.keys():
        if k in dic.keys():
            outDic[k]= args[k]
    return outDic

def createDataframeFromArray(data, columns, csvPath:os.path= None)->pd.DataFrame:
    '''
    The function takes as input a numpy array and creates a pandas dataframe with the specified column names. 
    @data: numpy array
    @columns: list of column names
    @file_path: path to the CSV file. (Optional)
    @return: pd.DataFrame

    OPTIONAL: Save the DataFrame as csv file if csvPath is defined
    '''
    # print(f'Column names are : {len(columns)}')
    # print(f'Array shape : {data.shape}')
    df = pd.DataFrame(data, columns=columns)
    if csvPath:
        df.to_csv(csvPath, index=False)
    return df

def isCoordPairInArray(arr, pair) ->bool:
    '''
    Verify if a pair of coordinates values exist in the array. We asume that column[0] contain x_coordinates
    and column[1] contain y_coordinates
    The goal is to check if the coordinates of a map already exist in an array of map samples. 
    @pair: np.array like [x,y]
    @Return:Bool: True if pair is found, False otherwise.
    '''
    x, y = pair
    out = np.any(arr[np.where(arr[:,0]==x),1] == y)
    return out



###################            
### General GIS ###
###################

def plotImageAndMask(img, mask,imgName:str='Image', mskName: str= 'Mask'):
    # colList = ['Image','Mask']
    image = img.detach().numpy() if torch.is_tensor(img) else img.numpy().squeeze()
    mask_squeezed = mask.detach().numpy() if torch.is_tensor(mask) else mask.numpy().squeeze()
    fig, axs = plt.subplots(1,2, figsize=(10,5), sharey=True)
    axs[0].imshow(image, cmap='Greys_r')
    axs[0].set(xlabel= imgName)
    axs[1].imshow(mask_squeezed, cmap='Greys_r')
    axs[1].set(xlabel= mskName)
    plt.rcParams['font.size'] = '15'
    fig.tight_layout()
 
def imageToTensor(img,DTYPE:str = 'float32'):
    imagTensor = img.astype(DTYPE)
    # imagTensor = np.transpose(imagTensor, (2, 0, 1)).astype(DTYPE)
    imagTensor = torch.tensor(imagTensor)
    return imagTensor

def reshape_as_image(arr):
    '''
    From GDL
    Parameters
    ----------
    arr : arr as image in raster order (bands, rows, columns)
    return: array-like in the image form of (rows, columns, bands)
    '''       
    return np.ma.transpose(arr, [1, 2, 0]).astype('float32')

def reshape_as_raster(arr):
    '''  
    From GDL
        swap the axes order from (rows, columns, bands) to (bands, rows, columns)
    Parameters
    ----------
    arr : array-like in the image form of (rows, columns, bands)
    return: arr as image in raster order (bands, rows, columns)
    '''
    return np.transpose(arr, [2, 0, 1])

def plotHistComparison(DEM1,DEM2, bins:int = 50):
    # Reding raster 1:
    data_DEM1,_= readRasterWithRasterio(DEM1)  # Return an Array
    data_DEM1 = np.resize(data_DEM1,(1))
    # Reding raster 2:
    data_DEM2,_= readRasterWithRasterio(DEM2)  # Return an Array
    # data_DEM2 = np.resize(data_DEM2,(1))
    # Setting plot
    n_bins = bins
    fig, ax = plt.subplots(1,sharey=True, tight_layout=True)
    # x=np.array((data_DEM1[0],data_DEM2[0]))
    
    ax.hist(data_DEM1, n_bins, density=True, histtype='step', label=['cdem'],stacked=True, fill=False)
    # ax.hist(data_DEM2[0], n_bins, density=True, histtype='step', label=colors,stacked=True, fill=False)
    ax.legend(prop={'size': 10})
    ax.set_title('cdem_16m vs srdem_8m') 
    
    fig.tight_layout()
    plt.show()
    pass

def getNeighboursValues(raster)-> np.array:
    '''
    Inspect the 8 neighbours of each pixel to list their values. If the pixel being NoData, the neighbour list is empty. 
    @raster: os.path to the raster to inst=pect.
    @return: array of lists. 
    '''
    # Convert raster to NumPy array
    arr,profil = readRasterWithRasterio(raster)
    NOData = profil['nodata']
    # Get dimensions of array
    rows, cols = arr.shape

    # Create empty array to store neighbours
    neighbours = np.empty((rows, cols), dtype=object)

    # Iterate over each cell in array
    for i in range(rows):
        for j in range(cols):
            # Get value of current cell
            val = arr[i, j]

            # Check if value is NaN or NoData
            if np.isnan(val) or val == NOData:
                neighbours[i, j] = []
            else:
                # Get indices of neighbouring cells
                indices = [(i-1, j-1), (i-1, j), (i-1, j+1),
                           (i, j-1),             (i, j+1),
                           (i+1, j-1), (i+1, j), (i+1, j+1)]

                # Get values of neighbouring cells
                vals = [arr[x, y] for x, y in indices if 0 <= x < rows and 0 <= y < cols]
                # Add values to neighbours array
                neighbours[i, j] = vals

    return neighbours

def crop_TifList_WithMaskList(cfg: DictConfig, maskList:os.path):
    '''
    Given a list of polygons, the algorith find all tif files in the wdir and IF any names match ocurre, the tif is 
    cropped with the corresponding mask.
    '''
    wdir = cfg['output_dir']
    maskList = createListFromCSV(maskList)
    tifList = listFreeFilesInDirByExt_fullPath(wdir,'.tif')
    for i in tifList:
        _,tifName,_ = get_parenPath_name_ext(i)
        for j in maskList:
            _,maskName,_ = get_parenPath_name_ext(j)
            if maskName in tifName:
                outPath = os.path.join(wdir,maskName+'_clip.tif')
                print('-----------------------Cropping --------------------')
                clipRasterByMask(i,j,outPath)
                print(f'{outPath}')
                print('-----------------------Cropped --------------------  \n')
    print("All done --->")        
    return True


#######################
### Rasterio Tools  ###
#######################

def readRasterWithRasterio(rasterPath:os.path) -> tuple[np.array, dict]:
    '''
    Read a raster with Rasterio.
    return:
     Raster data as np.array
     Raster.profile: dictionary with all rater information
    '''
    inRaster = rio.open(rasterPath, mode="r")
    profile = inRaster.profile
    rasterData = inRaster.read()
    # print(f"raster data shape in ReadRaster : {rasterData.shape}")
    return rasterData, profile

def read_tiff_advanced(file_path: str) -> Tuple[np.ndarray, str, str]:
    try:
        with rio.open(file_path) as dataset:
            image_data = dataset.read()
            file_extension = dataset.profile['driver']
            crs = dataset.crs.to_string()
        return image_data, file_extension, crs
    except Exception as e:
        print(f"The TIFF in the path {file_path} is corrupted.")
        return None, None, None

def createRaster(savePath:os.path, data:np.array, profile, noData:int = None):
    '''
    parameter: 
    @savePath: Most contain the file name ex.: *name.tif.
    @data: np.array with shape (bands,H,W)
    '''
    B,H,W = data.shape[-3],data.shape[-2],data.shape[-1] 
    # print(f"C : {B}, H : {H} , W : {W} ")
    profile.update(dtype = rio.uint16, nodata = noData, blockysize = profile['blockysize'])
    with rio.open(
        savePath,
        mode="w",
        #out_shape=(B, H ,W),
        **profile
        ) as new_dataset:
            # print(f"New Dataset.Profile: ->> {new_dataset.profile}")
            new_dataset.write(data)
            print("Created new raster>>>")
    return savePath

def stackBandsInMultibandRaster(input_paths, output_path):
    '''
    Given a series of raster in the <input_path>, the algorithm create a multiband raster to the <output_path>.
    @input_path: List of path to a single band rasters.
    @output_path: Output path to save the multiband raster. 
    '''
    src_files_to_mosaic = []
    i = 0
    for path in input_paths:
        # print(f'band {i} : {path}')
        i+=1
        src = rio.open(path)
        src_files_to_mosaic.append(src)

    mosaic, out_trans = rio.merge.merge(src_files_to_mosaic)
    out_meta = src.meta.copy()
    out_meta.update({"driver": "GTiff",
                     "count": len(src_files_to_mosaic),
                     "height": mosaic.shape[1],
                     "width": mosaic.shape[2],
                     "transform": out_trans})

    with rio.open(output_path, "w", **out_meta) as dest:
        for i, file in enumerate(src_files_to_mosaic):
            dest.write(file.read(1), i+1)

def plotHistogram(raster, CustomTitle:str = None, bins: int=50, bandNumber: int = 1):
    if CustomTitle is not None:
        title = CustomTitle
    else:
        title = f"Histogram of band : {bandNumber}"    
    data,_ = readRasterWithRasterio(raster)
    
    show_hist(source=data, bins=bins, title= title, 
          histtype='stepfilled', alpha=0.5)
    return True

def replaceRastNoDataWithNan(rasterPath:os.path,extraNoDataVal: float = None)-> np.array:
    rasterData,profil = readRasterWithRasterio(rasterPath)
    NOData = profil['nodata']
    rasterDataNan = np.where(((rasterData == NOData)|(rasterData == extraNoDataVal)), np.nan, rasterData) 
    return rasterDataNan

def updateNoDataValue_Rio(input_path, output_path, nodata_value):
    with rio.open(input_path) as src:
        profile = src.profile
        profile.update(nodata=nodata_value)
        with rio.open(output_path, 'w', **profile) as dst:
            for i in range(1, src.count + 1):
                data = src.read(i)
                dst.write(data, i)

def computeRasterStats(rasterPath:os.path):
    '''
    Read a reaste and return: 
    @Return
    @rasMin: Raster min.
    @rasMax: Raster max.
    @rasMean: Rater mean.
    @rasMode: Raster mode.
    @rasSTD: Raster standard deviation.
    @rasNoNaNCont: Raster count of all NOT NoData pixels
    '''
    rasDataNan = replaceRastNoDataWithNan(rasterPath)
    rasMin = np.nanmin(rasDataNan)
    rasMax = np.nanmax(rasDataNan)
    rasMean = np.nanmean(rasDataNan)
    rasSTD = np.nanstd(rasDataNan)
    rasNoNaNCont = np.count_nonzero(rasDataNan != np.nan)
    # Compute mode
    vals,counts = np.unique(rasDataNan, return_counts=True)
    index = np.argmax(counts)
    rasMode = vals[index]
    return rasMin, rasMax, rasMean,rasMode, rasSTD, rasNoNaNCont

def computeRasterMinMax(rasterPath:os.path):
    '''
    Read a reaste and return: 
    @Return
    @rasMin: Raster min.
    @rasMax: Raster max.
    '''
    rasDataNan = replaceRastNoDataWithNan(rasterPath)
    rasMin = np.nanmin(rasDataNan)
    rasMax = np.nanmax(rasDataNan)
    return rasMin, rasMax

def computeRasterQuantiles(rasterPath, q:list=[0.25, 0.945]):
    rasDataNan = replaceRastNoDataWithNan(rasterPath)
    # rasDataNan,_ = readRasterWithRasterio(rasterPath)
    quantiles = np.nanquantile(rasDataNan, q)
    return quantiles

def computeRasterValuePercent(rasterPath, value:int=1)-> float:
    '''
    Compute the percent of pixels of value <value: default =1> in a raster. 
    @rasterPath: Path to the raster to be analyzed.
    @value: Value to verify percent in raster. Default = 1. 
    @return: The computed percent of <value> within the nonNoData values in the input raster.  
    '''
    rasDataNan = replaceRastNoDataWithNan(rasterPath)
    rasNoNaNCont = np.count_nonzero(rasDataNan != np.nan)
    valuCont = np.count_nonzero(rasDataNan == value)
    return (valuCont/rasNoNaNCont)*100

def normalize_raster(inputRaster):
    outputRaster = addSubstringToName(inputRaster, '_norm')
    with rio.open(inputRaster) as src:
        profile = src.profile
        with rio.open(outputRaster, 'w', **profile) as dst:
            for i in range(1, src.count + 1):
                data = src.read(i)
                normalized_data = (data - data.min()) / (data.max() - data.min())
                dst.write(normalized_data, i)
    return outputRaster

def randomSamplingMultiBandRaster(rasterIn,ratio:float=1, maxSampling:int = 150000)-> np.array:
    '''
    Given a multiband raster, the algorith, takes a random number of saples and retur in np.array format. 
    The samples are ONLY VALID points according the criteria of been not NODataValue. (bandsArray[0]!= NoData and bandsArray[0] != np.NAN)
    The samples are not repeated coordinates pairs.
    @rasterIn: the path to the input raster.
    @ratio: float (0 to 1): the percentage of existing pixels in the <rasterIn> to be sampled (1=full raster, 0=No samples). 
    @return: np.array with a series of samplin points.
    '''
    sampleCont =0    
    arrayDataset = []
    with rio.open(rasterIn) as src:
        # Read data from bands into an array
        data = src.read()
        NoData = src.profile['nodata']
        W,H = src.width,src.height
        totalSamples = int((W*H)*ratio)
        if totalSamples > maxSampling:
            totalSamples = maxSampling
        print(f'Number of samples to take {totalSamples}')
        arrayDataset = np.zeros((totalSamples,(src.count+2)))
        while sampleCont<totalSamples:
            # Generate random index within the raster limits
            i = np.random.randint(0,H)
            j = np.random.randint(0,W)
            ## extract corrdinates at index (i,j)
            xy = np.array(src.xy(i,j))
            ## Extract banda values as vector
            bandsArray = data[:, i, j]
            # Check if neither value is NoData OR NaN in the first band (DEM)
            if (bandsArray[0]!= NoData and bandsArray[0] != np.NAN and bandsArray[0] != 0) and not isCoordPairInArray(arrayDataset,xy):
                # Add the sample to the dataset
                arrayDataset[sampleCont] = np.concatenate((xy, bandsArray))
                sampleCont+=1
                if sampleCont%1000 == 0:
                    print(f'{sampleCont-1} found') 
    return arrayDataset

def getRasterSampleFromPoint(raster,xy):
    '''
    Extract a sample from aa raster given a pari of coordinates xy.
      NOTE: This function do not make any veryfication.Important to ensure xy coordinates pair comes from the same crs of the raster.
    @xy: array of coordinates like [x,y].
    @raster: os.path to the raster to sample
    @return: a 1D array with the values of all the bands in the <raster> at the coordinates <xy>.
    '''
    x,y = xy
    with rio.open(raster) as src:
        # Read data from bands into an array
        data = src.read()
        bandsArray = data[:, x, y]
    return bandsArray

def transformShp_IDValue(shpFile, targetField, baseField):
# Open the shapefile
    driver = ogr.GetDriverByName('ESRI Shapefile')
    shapefile = driver.Open(shpFile, 1)  # 1 means open in update mode
    # Get the layer
    layer = shapefile.GetLayer()
    # Iterate over each feature in the layer
    for feature in layer:
        # Get the value of the field you want to base your changes on
        base_value = feature.GetField(baseField)
        # Determine the <new_value> based on the <base_value>
        # This is where you put your transformation logic
        new_value = str(base_value)  # replace this with your transformation function
        # Set and Update the value of the Layer's field.
        feature.SetField(targetField, new_value)
        layer.SetFeature(feature)

    # Close the shapefile
    shapefile = None


######################
####   GDAL Tools  ###
######################

class RasterGDAL():
    '''
    Some info about GDAL deo Transform
    adfGeoTransform[0] /* top left x */
    adfGeoTransform[1] /* w-e pixel resolution */
    adfGeoTransform[2] /* rotation, 0 if image is "north up" */
    adfGeoTransform[3] /* top left y */
    adfGeoTransform[4] /* rotation, 0 if image is "north up" */
    adfGeoTransform[5] /* n-s pixel resolution */
    
    '''
    def __init__(self, rasterPath) -> None:
        gdal.AllRegister() # register all of the drivers
        gdal.DontUseExceptions()
        self.inputPath = rasterPath
        self.ds = gdal.Open(rasterPath)
        if self.ds is None:
            print('Could not open image')
            sys.exit(1)   
        # get image size
        self.rows = self.ds.RasterYSize
        self.cols = self.ds.RasterXSize
        self.NumOfBands = self.ds.RasterCount
        # get georeference info
        transform = self.ds.GetGeoTransform()
        self.xOrigin = transform[0]
        self.yOrigin = transform[3]
        self.pixelWidth = transform[1]
        self.pixelHeight = transform[5]
        self.projection = self.ds.GetProjection()
        self.MetaData = self.ds.GetMetadata()
        self.band1 = self.ds.GetRasterBand(1)
        self.NoData = self.band1.GetNoDataValue()

    def setDirGDAL(self, path ):
        os.chdir()
    
    def getRasterDataset(self):
        return self.ds 
   
    def getRasterNpArray(self, maskNoData:bool = True)-> np.array:
        arr = self.ds.ReadAsArray()
        if maskNoData:
            arr = np.ma.masked_equal(arr, self.NoData)
        return arr
    
    def computePixelOffset(self,x,y):
        # compute pixel offset
        xOffset = int((x - self.xOrigin) / self.pixelWidth)
        yOffset = int((y - self.yOrigin) / self.pixelHeight)
        return xOffset, yOffset

    def closeRaster(self):
        self.ds = None

    def translateRaster(self, outpPath, format:str = "GeoTiff"):
        """
        Ref: https://gdal.org/api/python/osgeo.gdal.html#osgeo.gdal.Translate
        """
        gdal.Translate(outpPath,self.ds,format=format)
        return True

    def saveTiffAsPCRaster(self):
        outpPath = ntpath.basename(self.inputPath).replace('.tif','.map') 
        gdal.Translate(outpPath,self.ds,format='PCRaster')
        return outpPath

    def printRaster(self):
        print("---- Image size ----")
        print(f"Row : {self.rows}")
        print(f"Cols : {self.cols}")
        print(f"xOrigin : {self.xOrigin}")
        print(f"yOrigin : {self.yOrigin}") 
        print(f"NumOfBands : {self.NumOfBands}")
        print(f"pixelWidth : {self.pixelWidth}")
        print(f"pixelHeight : {self.pixelHeight}")
        print(f"projection : {self.projection}")
        print(f"MetaData : {self.MetaData}")

def replace_no_data_value(dataset_path, new_value:float = -9999):
    dataset = gdal.Open(dataset_path, gdal.GA_Update)
    for i in range(1, dataset.RasterCount + 1):
        band = dataset.GetRasterBand(i)
        old_value = band.GetNoDataValue()
        band = dataset.GetRasterBand(i)
        band_array = band.ReadAsArray()
        band_array[band_array == old_value] = new_value
        band.WriteArray(band_array)
        band.SetNoDataValue(new_value)
    dataset.FlushCache()
    
def translateToTiff(inPath) -> bool:
    """
    Write a *tif raster from an appropriate(GDAL accepted formats) raster input. The function return a raster with the same characteristic,
      with NoDataValue seted to -9999.
    @inPath: os.path: Path to the input raster
    @Return: os.path: Path to the *.tif raster. 
    
    """
     # Open the input raster
    input_raster = gdal.Open(inPath)
   
    # Transform the dataset NoData value into -9999
    input_band = input_raster.GetRasterBand(1)
    input_band.SetNoDataValue(-9999)

    # Create an empty raster file with the same CRS as the input raster
    output_path = os.path.splitext(inPath)[0] + '.tif'
    driver = gdal.GetDriverByName('GTiff')
    output_raster = driver.CreateCopy(output_path, input_raster, 0)

    # Write the dataset with the transformed NoData into the empty raster
    output_band = output_raster.GetRasterBand(1)
    output_band.WriteArray(input_band.ReadAsArray())

    # Close the rasters
    input_raster = None
    output_raster = None

    return output_path

def translateToPCRaster(inputPath) -> str:
    outpPath = replaceExtention(inputPath,'.map')
    options = gdal.TranslateOptions(format='PCRaster',noData=-9999) # creationOptions=["COMPRESS=LZW"]
    gdal.Translate(outpPath,inputPath,options=options) #outputType=gdal.GDT_Float32
    return outpPath

def readRasterAsArry(rasterPath):
   return gdal_array.LoadFile(rasterPath)

def extractRasterProjection(inPath):
    '''
    Extract the projection of the dataset with GDAL.GetProjection()
    @inPath: path to the input file. Must be in one of the GDAL format. ex. 'GTiff'
    @Return: projection file
    '''
    # Open the input TIFF file
    dataset = gdal.Open(inPath)
    # Get the input CRS
    crs = dataset.GetProjection()
    print(crs)
    return crs

def isValidShapefile(shpFilePath)->bool:
    data_source = ogr.Open(shpFilePath)
    if data_source is None:
        print("Could not open shapefile")
        return False
    # Get the first layer
    layer = data_source.GetLayer(0)
    if layer is None:
        print("Could not get layer from shapefile")
        return False
    data_source.Destroy()
    print(f"Verified -> {shpFilePath}")
    return True

def extractVectorSpatialReference(file_path):
    '''
    Extract the spatialReference of the dataset with GDAL.GetProjection()
    @inPath: path to the input file. Must be in one of the GDAL format. ex. 'GTiff'
    @Return: projection file
    '''
    # Open the shapefile
    data_source = ogr.Open(file_path)
    if data_source is None:
        print("Could not open shapefile")
        return None
    # Get the first layer
    layer = data_source.GetLayer(0)
    if layer is None:
        print("Could not get layer from shapefile")
        return None
    # Get the spatial reference
    spatial_ref = layer.GetSpatialRef()
    if spatial_ref is None:
        print("Could not get spatial reference from shapefile")
        return None
    #### ___ Uncomment next line to Print the spatial reference
    # print(spatial_ref.ExportToPrettyWkt())
    data_source.Destroy()
    return spatial_ref

def extractVectorEPSG(shapefile_path):
    '''
    Extract the EPSG value from shapefile spatial reference. 
    '''
    spatial_ref = extractVectorSpatialReference(shapefile_path)
    return spatial_ref.GetAttrValue('AUTHORITY',1)

def reprojectShapefile(input_shapefile:os.path, output_shapefile:os.path, target_epsg:int= 3979)->bool:
    '''
    Reproject a shapefile in a new shapefile in the path <output_shapefile>. The function create a new shapefile with the the EPGS value in the <target_epsg>, keeping the rest of the <input_shapefile> atributes.
    @input_shapefile: os.path: Path to the input shapefile to reproject. 
    @output_shapefile: os.path: Path to the new shapefile. 
    @target_epsg: int (default = 3979). Valid value from the EPGS loist of values. (ex. 4326) 
    @return: bool: False if any step fails, True otherwise. 
    '''
    # Get the input layer
    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataSource = driver.Open(input_shapefile, 0) # 0 means read-only
    if dataSource is None:
            print("Could not open shapefile")
            return False
    layer = dataSource.GetLayer()
    if layer is None:
            print("Could not get layer from shapefile")
            return False
    
    # Target Spatial Reference
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(target_epsg)

    # Create the output shapefile
    out_dataSource = driver.CreateDataSource(output_shapefile)
    out_layer = out_dataSource.CreateLayer(output_shapefile,target_srs,geom_type=ogr.wkbPolygon)

    # Add input Layer Fields to the output Layer if it is the one we want
    in_layer_defn = layer.GetLayerDefn()
    for i in range(0, in_layer_defn.GetFieldCount()):
        field_defn = in_layer_defn.GetFieldDefn(i)
        out_layer.CreateField(field_defn)

    # Get the output Layer's Feature Definition
    out_layer_defn = out_layer.GetLayerDefn()

    # Reproject each feature
    for i in range(0, layer.GetFeatureCount()):
        # Get the input feature
        in_feature = layer.GetFeature(i)
        # Create output feature
        out_feature = ogr.Feature(out_layer_defn)
        # Set geometry after transforming
        geom = in_feature.GetGeometryRef()
        geom.TransformTo(target_srs)
        out_feature.SetGeometry(geom)
        # Add field values from input Layer
        for i in range(0, out_layer_defn.GetFieldCount()):
            nameRef = out_layer_defn.GetFieldDefn(i).GetNameRef()
            # print( f"nameRef {nameRef} and nameRef type{type(nameRef)}")
            field = in_feature.GetField(i)
            # print( f"field {field} and field type{type(field)}")
            out_feature.SetField(nameRef, field)
        # Add new feature to output Layer
        out_layer.CreateFeature(out_feature)
    
    # Close DataSources
    dataSource.Destroy()
    out_dataSource.Destroy()
    return True

def reproject_PCRaster(tif_file,output_crs:str='EPSG:3979') -> str:
    """
    Reprojects a PCraster file to the specified coordinate reference system (CRS).
    Args:
        map_file(str): Path to the input *.map file.
        output_crs (str): Output coordinate reference system (CRS) in the format 'EPSG:<code>'.

    Returns:
        str: Path to the reprojected *.map file.
    NOTE: NOData value do not works. 
    """
    # get input name and extention
    parent,inputNeme,ext = get_parenPath_name_ext(tif_file)
    # Open the input TIFF file
    dataset = gdal.Open(tif_file)
    
    # Create a spatial reference object for the output CRS
    output_srs = osr.SpatialReference()
    output_srs.ImportFromEPSG(int(output_crs.split(':')[1]))
    output_file_path = os.path.join(parent,inputNeme + '_' + ext)
    
    # Create the output dataset
    '''
    NO definas input dataset crs. 
       srcSRS=input_srs, 
    '''
    gdal.Warp(output_file_path, dataset, dstSRS=output_srs, outputType=gdal.GDT_Float32, dstNodata=-9999, creationOptions=['PCRASTER_VALUESCALE=VS_SCALAR'])
    # gdal.Warp(output_file_path, dataset, dstSRS=output_srs, resampleAlg=gdal.GRA_Bilinear, dstNodata=-9999,outputType=gdal.GDT_Float32)
    # Close the datasets
    del dataset
    return output_file_path

def reproject_tif(tif_file, output_crs:str='EPSG:3979') -> str:
    """
    Reprojects a TIFF file to the specified coordinate reference system (CRS).
    Args:
        tif_file (str): Path to the input TIFF file.
        output_crs (str): Output coordinate reference system (CRS) in the format 'EPSG:<code>'. Default <'EPSG:3979'>

    Returns:
        str: Path to the reprojected TIFF file.
    """
    # get input name and extention
    parent,inputNeme,ext = get_parenPath_name_ext(tif_file)
    # Open the input TIFF file
    replace_no_data_value(tif_file)
    dataset = gdal.Open(tif_file,gdal.GA_Update)
    # Create a spatial reference object for the output CRS
    output_srs = osr.SpatialReference()
    output_srs.ImportFromEPSG(int(output_crs.split(':')[1]))
    output_file_path = os.path.join(parent,inputNeme + '_' + ext)
    # Create the output dataset
    '''
    Do not define input dataset crs. 
       srcSRS=input_srs, 
    It works better. Do not know way..!!
    '''
    gdal.Warp(output_file_path, dataset, dstSRS=output_srs, resampleAlg=gdal.GRA_Bilinear, dstNodata=-9999)#outputType=gdal.GDT_Float32
    # Close the datasets
    del dataset
    return output_file_path

def assigneProjection(raster_file, output_crs:str='EPSG:3979') -> str:
    '''
    Assigne prejection <outpt_crs> to files in *.tif and *.map format IF they have no projection defined.
    parameters:
     @raster_file: os.path: Path to the raster to be reprojected.
     @output_crs: <EPSG:####> projection.
     @return: The path to the reprojected file.
    '''
    _,communName,ext = get_parenPath_name_ext(raster_file)
    input_crs = extractRasterProjection(raster_file)
    if not input_crs:
        print(f'Reprojecting..... {communName}{ext}')
        if "map" in ext:
            return reproject_PCRaster(raster_file,output_crs)
        if 'tif' in ext:
            return reproject_tif(raster_file,output_crs) 

def clipRasterByMask(DEMPath:os.path, vectorMask, outPath)-> gdal.Dataset:
    '''
    Simplified version of crop_tif() WORKS well! However, do not perform extra operations like correct NoData or verify crs.
    If you are sure of your inputs and outputs, use it.
    '''
    mask_bbox = get_Shpfile_bbox(vectorMask)
    gdal.Warp(outPath, DEMPath,outputBounds=mask_bbox,cutlineDSName=vectorMask, cropToCutline=True)
    print(f"Successfully clipped at : {outPath}")
    return outPath

def get_Shpfile_bbox(file_path) -> Tuple[float, float, float, float]:
    driver = ogr.GetDriverByName('ESRI Shapefile')
    data_source = driver.Open(file_path, 0)
    layer = data_source.GetLayer()
    extent = layer.GetExtent()
    min_x, max_x, min_y, max_y = extent
    return min_x, min_y, max_x, max_y

def get_Shpfile_bbox_str(file_path) -> str:
    driver = ogr.GetDriverByName('ESRI Shapefile')
    data_source = driver.Open(file_path, 0)
    layer = data_source.GetLayer()
    extent = layer.GetExtent()
    min_x, max_x, min_y, max_y = extent
    bboxStr = str(round(min_x, 2))+','+str(round(min_y,2))+','+str(round(max_x,2))+','+str(round(max_y,2))
    return bboxStr

def computeProximity(inRaster, value:int= 1, outPath:os.path = None) -> os.path:
    '''
    Compute the horizontal distance to features in the input raster.
    @inRaster: A raster with features to mesure proximity from. A 0-1 valued raster,where the 1s are cells of the river network. 
    @outPath: Path to save the output raster. If None,the output is create in the same folder as the input with prefix: <_proximity.tif>.
    @values: list of values to be considered as terget in the inRaster. Default [1].  
    '''
    if outPath is None:  
        path,communName,_ = get_parenPath_name_ext(inRaster)
        # Create output name
        outPath =os.path.join(path,str(communName+'_proximity.tif'))
    ds = gdal.Open(inRaster,GA_ReadOnly)
    band = ds.GetRasterBand(1)
    gt = ds.GetGeoTransform()
    sr = ds.GetProjection()
    cols = ds.RasterXSize
    rows = ds.RasterYSize

    # create empty proximity raster
    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(outPath, cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(gt)
    out_ds.SetProjection(sr)
    out_band = out_ds.GetRasterBand(1)

    # compute proximity
    gdal.ComputeProximity(band, out_band, [f'VALUES= {value}', 'DISTUNITS=GEO'])
    # delete input and output rasters
    del ds, out_ds
    return outPath

def getNeighborsGDAL(raster_file, shape_file):
    """
    This function takes a raster and a shapefile as input and returns an array containing
    the values of the 8 neighbors of each pixel in the raster. The inputs must be in the
    same reference system. The output array will have the same size of the input raster
    and will contain at each cell a list of shape [1:8], with the values of the 8 neighbors
    of the questioned pixel, starting from the left up corner. The function considers only
    the pixels that are not np.nan or noData value in any of the two input layers. If the
    questioned pixel is noData in any of the inputs or np.nan, the list of neighbors values
    will be empty.
                               Neighbourhood order
                                #############
                                ## 1  2  3 ##
                                ## 4  X  5 ##
                                ## 6  7  8 ##
                                #############

    @raster_file: path to raster file.
    @shape_file: path to shapefile.
    @return: array containing values of 8 neighbors for each pixel in raster
    """
    # Open raster file and get band, width, height, and transform information
    dataset = gdal.Open(raster_file)
    band = dataset.GetRasterBand(1)
    width = dataset.RasterXSize
    height = dataset.RasterYSize
    transform = dataset.GetGeoTransform()

    # Open shapefile and get layer information
    shape = gdal.OpenEx(shape_file)
    layer = shape.GetLayer()

    # Create output array with same size as input raster
    output_array = np.empty((height, width), dtype=object)

    # Loop through each pixel in raster and get its neighbors' values
    for y in range(height):
        for x in range(width):
            # Get value at current pixel location
            value = band.ReadAsArray(x, y, 1, 1)[0][0]

            # Check if current pixel is not np.nan or noData value in either input layer
            if not np.isnan(value) and value != band.GetNoDataValue():
                # Get coordinates for current pixel location
                x_coord = transform[0] + (x * transform[1]) + (y * transform[2])
                y_coord = transform[3] + (x * transform[4]) + (y * transform[5])

                ## Create polygon for current pixel location
                # This could be slow, but is less error-prone than rasterize polygons. 
                ring = ogr.Geometry(ogr.wkbPolygon)
                ring.AddPoint(x_coord, y_coord)
                ring.AddPoint(x_coord + transform[1], y_coord)
                ring.AddPoint(x_coord + transform[1], y_coord + transform[5])
                ring.AddPoint(x_coord, y_coord + transform[5])
                ring.AddPoint(x_coord, y_coord)
                poly = ogr.Geometry(ogr.wkbPolygon)
                poly.AddGeometry(ring)

                # Loop through each feature in shapefile and check if it intersects with current pixel location polygon
                neighbors = []
                for feature in layer:
                    if feature.GetGeometryRef().Intersects(poly):
                        neighbor_value = feature.GetField("value")
                        if not np.isnan(neighbor_value) and neighbor_value != band.GetNoDataValue():
                             # Get indices of neighbouring cells
                            indices = [(x_coord-transform[0], y_coord-transform[3]),        #1
                                    (x_coord-transform[0], y_coord),                        #2
                                    (x_coord-transform[0], y_coord+transform[3]),           #3
                                    (x_coord, y_coord-transform[3]),                        #4
                                    (x_coord, y_coord+transform[3]),                        #5
                                    (x_coord+transform[0], y_coord-transform[3]),           #6
                                    (x_coord+transform[0], y_coord),                        #7
                                    (x_coord+transform[0], y_coord+transform[3])]           #8

                            # Get values of neighbouring cells that are not NaN or NoData in either input layer
                            vals = [band[x, y] for x, y in indices]
                            # Add values to neighbours array
                            neighbors[x_coord, y_coord] = vals
                # Add list of neighbor values to output array at current pixel location
                output_array[y,x] = neighbors

            else:
                # Add empty list to output array at current pixel location since current pixel is noData or np.nan
                output_array[y,x] = []

    return output_array

def fullSamplingTwoRasterForComparison(raster1_path, raster2_path) -> np.array:
    '''
    This code takes two input rasters and returns an array with four columns: [x_coordinate, y_coordinate, Z_value  raster one, Z_value raster two]. 
    The first input raster is used as a reference. 
    The two rasters are assumed to be in the same CRS but not necessarily with the same resolution. 
    The algorithm samples the centre of all pixels using the upper-left corner of the first raster as a reference.
    When you read a raster with GDAL, the raster transformation is represented by a <geotransform>. The geotransform is a six-element tuple that describes the relationship between pixel coordinates and georeferenced coordinates ⁴. The elements of the geotransform are as follows:
    
    RASTER Transformation content 
    ex. raster_transformation : (1242784.0, 8.0, 0.0, -497480.0, 0.0, -8.0)
    0. x-coordinate of the upper-left corner of the raster
    1. width of a pixel in the x-direction
    2. rotation, which is zero for north-up images
    3. y-coordinate of the upper-left corner of the raster
    4. rotation, which is zero for north-up images
    5. height of a pixel in the y-direction (usually negative)

    The geotransform to convert between pixel coordinates and georeferenced coordinates using the following equations:

    x_geo = geotransform[0] + x_pixel * geotransform[1] + y_line * geotransform[2]
    y_geo = geotransform[3] + x_pixel * geotransform[4] + y_line * geotransform[5]

    `x_pixel` and `y_line` : pixel coordinates of a point in the raster, 
    `x_geo` and `y_geo` : corresponding georeferenced coordinates.

    In addition, to extract the value in the centre of the pixels, we add 1/2 of width and height respectively.
    x_coord = i * raster1_transform[1] + raster1_transform[0] + raster1_transform[1]/2 
    y_coord = j * raster1_transform[5] + raster1_transform[3] + raster1_transform[5]/2

    '''
    # Open the first raster and get its metadata
    raster1 = gdal.Open(raster1_path)
    raster1_transform = raster1.GetGeoTransform()
    print(f"raster1_transform : {raster1_transform}")
    raster1_band = raster1.GetRasterBand(1)
    raster1_noDataValue = raster1_band.GetNoDataValue()

    # Open the second raster and get its metadata
    raster2 = gdal.Open(raster2_path)
    raster2_transform = raster2.GetGeoTransform()
    raster2_band = raster2.GetRasterBand(1)
    raster2_noDataValue = raster2_band.GetNoDataValue()

    # Get the size of the rasters
    x_size = raster1.RasterXSize
    y_size = raster1.RasterYSize

    # Create an array to store the sampled points
    sampled_points = np.zeros((x_size * y_size, 4))

    # Loop through each pixel in the first raster
    
    for i in range(x_size):
        for j in range(y_size):
            # Get the coordinates of the pixel in the first raster
            x_coord = i * raster1_transform[1] + raster1_transform[0] + raster1_transform[1]/2 
            y_coord = j * raster1_transform[5] + raster1_transform[3] + raster1_transform[5]/2 

            # Get the value of the pixel in the first and second rasters
            value_raster1 = raster1_band.ReadAsArray(i, j, 1, 1)[0][0]
            value_raster2 = raster2_band.ReadAsArray(i, j, 1, 1)[0][0]

            # Add the sampled point to the array
            if (value_raster1!= raster1_noDataValue and value_raster1 != np.NaN 
                and value_raster2 != raster2_noDataValue and value_raster2 != np.NaN):
                sampled_points[i * y_size + j] = [x_coord, y_coord, value_raster1, value_raster2]

    print(f'One sample: {sampled_points[2:]}')
    return sampled_points
    
def randomSamplingTwoRaster(raster1_path, raster2_path, num_samples) -> np.array:
    '''
    This code takes two input rasters and returns an array with four columns: [x_coordinate, y_coordinate, Z_value rather one, Z_value rather two]. 
    The first input raster is used as a reference. 
    The two rasters are assumed to be in the same CRS but not necessarily with the same resolution. 
    The algorithm samples the centre of all pixels using the upper-left corner of the first raster as a reference.
    When you read a raster with GDAL, the raster transformation is represented by a <geotransform>. The geotransform is a six-element tuple that describes the relationship between pixel coordinates and georeferenced coordinates ⁴. The elements of the geotransform are as follows:
    
    RASTER Transformation content 
    ex. raster_transformation : (1242784.0, 8.0, 0.0, -497480.0, 0.0, -8.0)
    0. x-coordinate of the upper-left corner of the raster
    1. width of a pixel in the x-direction
    2. rotation, which is zero for north-up images
    3. y-coordinate of the upper-left corner of the raster
    4. rotation, which is zero for north-up images
    5. height of a pixel in the y-direction (usually negative)

    The geotransform to convert between pixel coordinates and georeferenced coordinates using the following equations:

    x_geo = geotransform[0] + x_pixel * geotransform[1] + y_line * geotransform[2]
    y_geo = geotransform[3] + x_pixel * geotransform[4] + y_line * geotransform[5]

    `x_pixel` and `y_line` : pixel coordinates of a point in the raster, 
    `x_geo` and `y_geo` : corresponding georeferenced coordinates.

    In addition, to extract the value in the centre of the pixels, we add 1/2 of width and height respectively.
    x_coord = i * raster1_transform[1] + raster1_transform[0] + raster1_transform[1]/2 
    y_coord = j * raster1_transform[5] + raster1_transform[3] + raster1_transform[5]/2

    '''    
    
    # Get the shape of the rasters
    # Open the first raster and get its metadata
    raster1 = gdal.Open(raster1_path)
    raster1_transform = raster1.GetGeoTransform()
    # print(f"raster1_transform : {raster1_transform}")
    raster1_band = raster1.GetRasterBand(1)
    raster1_noDataValue = raster1_band.GetNoDataValue()

    # Open the second raster and get its metadata
    raster2 = gdal.Open(raster2_path)
    raster2_transform = raster2.GetGeoTransform()
    raster2_band = raster2.GetRasterBand(1)
    raster2_noDataValue = raster2_band.GetNoDataValue()

    # Get the size of the rasters
    x_size = raster1.RasterXSize
    y_size = raster1.RasterYSize
    # print(f"size x, y : {x_size} , {y_size}")

    # Create an empty array to store the samples
    samples = np.zeros((num_samples, 4))
    # Loop through the number of samples
    sampleCont = 0
    while sampleCont<num_samples:
        i = np.random.randint(0, x_size)
        j = np.random.randint(0, y_size)
        # Generate random coordinates within the raster limits
        x = i * raster1_transform[1] + raster1_transform[0]+ raster1_transform[1]/2 
        y = j * raster1_transform[5] + raster1_transform[3]+ raster1_transform[5]/2 
        
        # Extract the values from the two rasters at the selected coordinates
        value1 = raster1_band.ReadAsArray(i, j, 1, 1)[0][0]
        value2 = raster2_band.ReadAsArray(i, j, 1, 1)[0][0]

        # Check if neither value is : NoData OR NaN
        if (value1!= raster1_noDataValue and value1 != np.NaN and value2 != raster2_noDataValue and value2 != np.NaN):
            # Add the values to the samples array
            samples[sampleCont] = [x, y, value1, value2]
            sampleCont+=1    

    return samples

def getFieldValueFromPolygonByCoordinates(vector_path:os.path, field_name:str, x:float, y:float)->list:
    # Open the vector file
    vector = ogr.Open(vector_path)
    layer = vector.GetLayer()
    # Create a point geometry for the given coordinate pair
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(x,y)
    # Set up a spatial filter to select features that intersect with the point
    layer.SetSpatialFilter(point)
    # Get the value of the specified field for each intersecting feature
    values = []
    for feature in layer:
        values.append(feature.GetField(field_name))
    # Return the first value if there is at least one intersecting feature
    if values:
        return values
    else:
        return []

def getRasterValueByCoord(raster1_path, xy) -> np.array:
    '''
    This code takes two input rasters and returns an array with four columns: [x_coordinate, y_coordinate, Z_value rather one, Z_value rather two]. 
    The first input raster is used as a reference. 
    The two rasters are assumed to be in the same CRS but not necessarily with the same resolution. 
    The algorithm samples the centre of all pixels using the upper-left corner of the first raster as a reference.
    When you read a raster with GDAL, the raster transformation is represented by a <geotransform>. The geotransform is a six-element tuple that describes the relationship between pixel coordinates and georeferenced coordinates ⁴. The elements of the geotransform are as follows:
    
    RASTER Transformation content 
    ex. raster_transformation : (1242784.0, 8.0, 0.0, -497480.0, 0.0, -8.0)
    0. x-coordinate of the upper-left corner of the raster
    1. width of a pixel in the x-direction
    2. rotation, which is zero for north-up images
    3. y-coordinate of the upper-left corner of the raster
    4. rotation, which is zero for north-up images
    5. height of a pixel in the y-direction (usually negative)

    The geotransform to convert between pixel coordinates and georeferenced coordinates using the following equations:

    x_geo = geotransform[0] + x_pixel * geotransform[1] + y_line * geotransform[2]
    y_geo = geotransform[3] + x_pixel * geotransform[4] + y_line * geotransform[5]

    `x_pixel` and `y_line` : pixel coordinates of a point in the raster, 
    `x_geo` and `y_geo` : corresponding georeferenced coordinates.

    In addition, to extract the value in the centre of the pixels, we add 1/2 of width and height respectively.
    x_coord = i * raster1_transform[1] + raster1_transform[0] + raster1_transform[1]/2 
    y_coord = j * raster1_transform[5] + raster1_transform[3] + raster1_transform[5]/2

    '''    
    # Get the shape of the rasters
    # Open raster and get its metadata
    raster1 = gdal.Open(raster1_path)
    raster1_band = raster1.GetRasterBand(1)
    # Create an empty array to store the samples
    rows = xy.shape[0]
    samples = np.zeros([rows,1])
    # Loop through the number of samples
    idx = 0
    for x,y in xy:
        # Extract the values from the two rasters at the selected coordinates
        value1 = raster1_band.ReadAsArray(x, y, 1, 1)[0][0]
        # Check if neither value is : NoData OR NaN):
            # Add the values to the samples array
        samples[idx] = [value1]
        idx+=1    
    return samples


############################
#### Datacube_ Extract  ####
############################

def dc_describe(cfg: DictConfig)-> bool:
    '''
    Configurate the call of d.describe() with hydra parameters.
    '''
    instantiate(OmegaConf.create(cfg.dc_Extract_params['dc_describeCollections']))
    return True

def dc_search(cfg: DictConfig)-> str :
    '''
    Configurate the call of d.search()  with hydra parameters.
    return the output path of the search result.
    '''
    out = instantiate(OmegaConf.create(cfg.dc_Extract_params['dc_search']))
    return out

def dc_extraction(cfg: DictConfig, args:dict=None)-> str:
    '''
    Configurate the call of extract_cog() with hydra parameters.
    return the output path of the extracted file.
    '''
    dict_DcExtract = OmegaConf.create(cfg.dc_Extract_params['dc_extrac_cog'])
    if args is not None:
        dict_DcExtract = updateDict(dict_DcExtract,args)
    print(f"New dcExtract Dict:  {dict_DcExtract}")
    ##  procede to extraction
    out = instantiate(dict_DcExtract)
    return out

def multiple_dc_extract_ByPolygonList(cfg: DictConfig):
    '''
    @cfg: DictConfig
    @csvPolygonList
    @Return: True if no error, otherwise dc_extraction tool errors report.
    '''
    polygList = createListFromCSV(cfg.dc_Extract_params['polygonListCSV'])
    for polyg in polygList:
        if os.path.exists(polyg):
            print(f"Currently working on -> {polyg}")
            _,name,_ = get_parenPath_name_ext(polyg)
            bbox = get_Shpfile_bbox_str(polyg)
            args = {"bbox":bbox,"suffix":name}
            dc_extraction(cfg,args=args)
        else:
            print(f"Path not found in the ssytem -> {polyg}")
    return True


# Helpers
def checkTifExtention(fileName):
    if ".tif" not in fileName:
        newFileName = input("enter a valid file name with the '.tif' extention")
        return newFileName
    else:
        return fileName

def downloadTailsToLocalDir(tail_URL_NamesList, localPath):
    '''
    Import the tails in the url <tail_URL_NamesList>, 
        to the local directory defined in <localPath>.
    '''
    confirmedLocalPath = ensureDirectory(localPath)
    for url in tail_URL_NamesList:
        download_url(url, confirmedLocalPath)
    print(f"Tails downloaded to: {confirmedLocalPath}")

##################################################################
########  DATA Analis tools for Geospatial Information   ########
##################################################################
