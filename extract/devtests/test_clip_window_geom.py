# -*- coding: utf-8 -*-
"""
Created on Thu Sep  1 08:21:54 2022

@author: ccrevier
This code explore the extract.clip_window_geometry() and extract.clip_to_grid() that assums 3979 projection 
The clip_window_geometry() function inside the clip_to_grid() fonction creates a geometry 
crs align or target align to extract the sample of data needed. 

?question?: 
    Could we stream the extrac part of the image au lieu de la downloader?
    Est-ce que nous devrions reprojeter avant d'extraire? 
    
Je pense que le problème vient du fait que nous faisons des extractions indépendante pour chaque image dans la
résolution d'origine, DONC nous faisons un align du bbox dans la projection d'origin de chaque image, alors les images
ne sont pas aligné ensemble...
"""

import pathlib
import extract as dce
import rasterio
from rasterio import mask
from rasterio.shutil import copy as rscopy
import os
from shapely.ops import clip_by_rect

dex = dce.DatacubeExtract()
dsd = dce.DatacubeStandard()

def params():
    """Default test study area"""
    tbox = (-2150469.4724999964,
            144975.05299999937,
            -2155469.4724999964,
            149975.05299999937)
    tbox_crs = 'EPSG:3979'
    
    return tbox, tbox_crs

def validate_bbox_reproj():
    """Validate if the final_geom reprojected back to the original crs is still crs and tap align
    Conclusion : NO IT IS NOT (which is problematic)"""
    

    tbox, tbox_crs = params()
    dex.dict_to_vector_file(dex.tbox_to_poly(tbox), tbox_crs, r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'original_tbox_in_3979.geojson')
    
    clip_tbox = str(float(tbox.split(',')[0])-50)+','+str(float(tbox.split(',')[1])-50)+','+str(float(tbox.split(',')[2])+50)+','+ str(float(tbox.split(',')[3])+50)
    
    dex.dict_to_vector_file(dex.tbox_to_poly(clip_tbox), tbox_crs, r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'original_tbox_in_3979_buffered.geojson')
    
    final_geom = dex.clip_window_geometry(tbox=clip_tbox, 
                                        tbox_crs=tbox_crs, 
                                        cog_crs=rasterio.crs.CRS().from_epsg(2960), 
                                        window_resolution=1, 
                                        national=False)
                
    #reproject final geom back to lcc
    dex.dict_to_vector_file(final_geom, rasterio.crs.CRS().from_epsg(2960), r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'final_geom_in_2960_buffered.geojson')
    final_geom_back_lcc = dex.transform_dict_to_dict(final_geom,rasterio.crs.CRS().from_epsg(2960),rasterio.crs.CRS().from_epsg(3979))
    dex.dict_to_vector_file(final_geom_back_lcc, rasterio.crs.CRS().from_epsg(3979), r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'final_geom_fom2960_to3979buffered.geojson')
    
    #Add the clip of the buffered geom to the og geom 
    xmin, ymin, xmax, ymax = dex.tbox_to_poly(tbox).bounds
    poly = dex.dict_to_poly(final_geom_back_lcc)
    cliped = clip_by_rect(poly, xmin, ymin, xmax, ymax)
    dex.dict_to_vector_file(cliped, rasterio.crs.CRS().from_epsg(3979), r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'final_geom_fom2960_to3979buffered_cliped_to_OG.geojson')
    #Permet de retourner exactement au tbox original....
    
validate_bbox_reproj()

#Changer la logique derière l'extraction lorsque nous voulons une reprojection
def clip_to_grid(self,clip_file,
                 out_file,
                 tbox,
                 dst_crs=None,
                 suffix=None,
                 resample='bilinear',
                 tbox_crs='EPSG:3979',
                 blocksize=512,
                 datacube_aligned=True,
                 national=True,
                 resolution=None):
    """ Aligns clip geometry to datacube national grid before clipping
        If resolution provided, something good will happen
        clips using rasterio.mask.mask
        rasterio.shutil.copy from https://github.com/cogeotiff/rio-cogeo/blob/master/rio_cogeo/cogeo.py"""


    # TODO check out_file dir exists and or create it
    original = pathlib.Path(out_file)
    if suffix:
        file_name = out_file.parent /f'{out_file.stem}_{suffix}{out_file.suffix}'
    else:
        file_name = original
    temp_file = f'{file_name}.temp'
    self._main_log.append(f'make_image_from_cog file_name {file_name}')

    #Permet de trouver le crs de destination
    #print(file_name)
    # open cog with all types of GeoTIFF georeference except that within the TIFF file’s keys and tags turned off
    cog=rasterio.open(clip_file,GEOREF_SOURCES='INTERNAL')
    # cog=rasterio.open(clip_file)
    # transform input geometry to cog crs
    if cog.crs != self._crs:
        msg = (f'Warning : Image projection ({cog.crs})is not'
               f' datacube standard crs ({self._crs}),'
               f' check bbox for distortion for extract from {cog.name}')
        print(msg)

    pixel_size = cog.transform[0]
    
    geom_tbox = self.tbox_to_poly(tbox)
    ori_w,ori_s,ori_e,ori_n = geom_tbox.bounds
    
    if resolution != pixel_size:
    #Si nous avons besoin de faire un resampling, ou un reproject, il faut avoir un tbox buffered
        clip_tbox = str(float(tbox.split(',')[0])-50)+','+str(float(tbox.split(',')[1])-50)+','+str(float(tbox.split(',')[2])+50)+','+ str(float(tbox.split(',')[3])+50)
    else:
        clip_tbox = tbox
    # Define clip window resolution based on native resolution and output resolution
    # TODO window_resolution assumes metres as pixel unit, chekc this with header info
    window_resolution = self.clip_window_resolution(pixel_size,resolution)
    # Now handles non 3979 projection
    std_geom = self.clip_window_geometry(clip_tbox,
                                         tbox_crs,
                                         cog_crs=cog.crs,
                                         window_resolution=window_resolution,
                                         datacube_aligned=datacube_aligned,
                                         national=national)
    self._main_log.append(f'Information : [{datetime.now()}] {cog.name} clip_from_mosaic standard_geom : {std_geom}')
    # convert geom dict to a list and pass in to mask
    # want crop = True and all other params default values
    try:
        sample_cog,cog_transform = mask.mask(dataset=cog,shapes=[std_geom],crop=True)

        #calculate height and width index of shape from number of dimensions
        height=sample_cog.shape[sample_cog.ndim-2]
        width=sample_cog.shape[sample_cog.ndim-1]
        # TODO use the existing kwargs modification
        # kwargs,dst_crs,dst_transform,dst_w,dst_h,blocksize
        kwargs = self.modify_kwargs(kwargs=cog.meta.copy(),
                                    dst_crs=cog.crs,
                                    dst_transform=cog_transform,
                                    dst_w=width,
                                    dst_h=height,
                                    blocksize=blocksize)

        dst=rasterio.open(temp_file,'w+',**kwargs)
        dst.write(sample_cog)
        dst = self.add_overviews(dst)

        rscopy(dst,file_name,copy_src_overviews=True,**kwargs)
        # rscopy(dst,out_file,**kwargs)
        dst.close()
        cog.close()
        os.remove(temp_file)
        self.write_logs()

    except Exception as e:
        self._error_log.append(f'Error : [{datetime.now()}] {cog.name} clip_fto_grid Error with mask or writing file {e}')

    return file_name

# def clip_window_geometry(tbox,
#                     tbox_crs,
#                     cog_crs,
#                     target_crs,
#                     window_resolution,
#                     datacube_aligned=True,
#                     national=False):
#     """The gemoetry object for the clip window in cog native projection"""

#     # Convert the bounding box to datacube standard crs
#     geom_d = dex.poly_to_dict((dex.tbox_to_poly(tbox)))
#     geom_p = dex.transform_dict_to_poly(geom_d,tbox_crs,target_crs)

#     # Calculate standard coords from original coords
#     epsg = cog_crs.to_epsg()
#     dex.dict_to_vector_file(geom_p, cog_crs, r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'{epsg}_geom_p.geojson')
    
#     ori_w,ori_s,ori_e,ori_n = geom_p.bounds
    
#     #TODO : modify the alignement to do a crs alignement on the cog crs
#     #CRS aligned on the cog crs:
    
#     if datacube_aligned:
#         std_w, std_s,std_e,std_n = dex.datacube_origin(ori_w,
#                                                         ori_s,
#                                                         ori_e,
#                                                         ori_n,
#                                                         window_resolution,
#                                                         national)
#     else:
#         std_w, std_s,std_e,std_n = dex.tap_origin(ori_w,
#                                                    ori_s,
#                                                    ori_e,
#                                                    ori_n,
#                                                    window_resolution)
#     std_tbox = f'{std_w}, {std_s},{std_e},{std_n}'
#     std_geom = dex.poly_to_dict((dex.tbox_to_poly(std_tbox)))
#     dex.dict_to_vector_file(std_geom, cog_crs, r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'{epsg}_std_geom.geojson')
#     # print(std_geom)
#     # Retransform standard window to image native crs
#     if cog_crs != dsd._crs:
#         # Reproject std_geom to input image crs
#         final_geom = dex.transform_dict_to_dict(std_geom,dsd._crs,cog_crs)
#         print('est-ce que la fonction se rend ici si epsg 2960?')
#         print(epsg, 'EPSG')
#     else:
#         # Keep the Datacube std crs defined geobox
#         final_geom = std_geom
#     dex.dict_to_vector_file(final_geom, cog_crs, r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_clip_window_geometry', filename=f'{epsg}_final_geom.geojson')
#     return final_geom
#     # return std_geom

# def clip_to_grid(clip_file,
#                  out_file,
#                  tbox,
#                  target_crs=None,
#                  suffix=None,
#                  resample='bilinear',
#                  tbox_crs='EPSG:3979',
#                  blocksize=512,
#                  datacube_aligned=True,
#                  national=True,
#                  resolution=None):
#     """ Aligns clip geometry to datacube national grid before clipping
#         If resolution provided, something good will happen
#         clips using rasterio.mask.mask
#         rasterio.shutil.copy from https://github.com/cogeotiff/rio-cogeo/blob/master/rio_cogeo/cogeo.py"""


#     # TODO check out_file dir exists and or create it
#     original = pathlib.Path(out_file)
#     if suffix:
#         file_name = out_file.parent /f'{out_file.stem}_{suffix}{out_file.suffix}'
#     else:
#         file_name = original
#     temp_file = f'{file_name}.temp'

#     #print(file_name)
#     # open cog with all types of GeoTIFF georeference except that within the TIFF file’s keys and tags turned off
#     cog=rasterio.open(clip_file,GEOREF_SOURCES='INTERNAL')
#     # cog=rasterio.open(clip_file)
#     if not target_crs :
#         target_crs = cog.crs
#     # transform input geometry to cog crs
#     if cog.crs != dsd._crs:
#         msg = (f'Warning : Image projection ({cog.crs})is not'
#                f' datacube standard crs ({dsd._crs}),'
#                f' check bbox for distortion for extract from {cog.name}')
#         print(msg)

#     pixel_size = cog.transform[0]
#     # Define clip window resolution based on native resolution and output resolution
#     # TODO window_resolution assumes metres as pixel unit, chekc this with header info
#     window_resolution = dex.clip_window_resolution(pixel_size,resolution)
#     # Now handles non 3979 projection
#     std_geom = dex.clip_window_geometry(tbox,
#                                          tbox_crs,
#                                          cog_crs=cog.crs,
#                                          targt_crs=target_crs,
#                                          window_resolution=window_resolution,
#                                          datacube_aligned=datacube_aligned,
#                                          national=national)
#     # convert geom dict to a list and pass in to mask
#     # want crop = True and all other params default values
   
#     sample_cog,cog_transform = mask.mask(dataset=cog,shapes=[std_geom],crop=True)

#     #calculate height and width index of shape from number of dimensions
#     height=sample_cog.shape[sample_cog.ndim-2]
#     width=sample_cog.shape[sample_cog.ndim-1]
#     # TODO use the existing kwargs modification
#     # kwargs,dst_crs,dst_transform,dst_w,dst_h,blocksize
#     kwargs = dex.modify_kwargs(kwargs=cog.meta.copy(),
#                                 dst_crs=cog.crs,
#                                 dst_transform=cog_transform,
#                                 dst_w=width,
#                                 dst_h=height,
#                                 blocksize=blocksize)

#     dst=rasterio.open(temp_file,'w+',**kwargs)
#     dst.write(sample_cog)
#     dst = dex.add_overviews(dst)

#     rscopy(dst,file_name,copy_src_overviews=True,**kwargs)
#     # rscopy(dst,out_file,**kwargs)
#     dst.close()
#     cog.close()
#     os.remove(temp_file)


#     return file_name




# tbox, tbox_crs = params()
# clip_window_geometry(tbox, tbox_crs, rasterio.crs.CRS().from_epsg(2960), 1)





# def clip_window_geometry_multiproj(outdir, crs=3979):
    
    
#     clip_window_geometry(self,tbox,
#                         tbox_crs,
#                         cog_crs,
#                         window_resolution,
#                         datacube_aligned=True,
#                         national=True)
#     return

#Modification de la fonction pour essayer de prendre en compte les projections différentes
#J'ai remplacer tous les self. à dce.
# def clip_window_geometry_mod(tbox,
#                     tbox_crs,
#                     cog_crs,
#                     target_crs, #new arg qui permet d'aligner la boite extraite avec le target à la place d'avec le lcc
#                     window_resolution,
#                     datacube_aligned=True,
#                     national=True):
#     """The gemoetry object for the clip window in cog native projection"""

#     # Convert the bounding box to datacube standard crs
#     # Je ne pense pas qu'on devrait faire ça. Je pense qu'on devrait directement convert to extraction crs
#     geom_d = dce.poly_to_dict((dce.tbox_to_poly(tbox))) #OK
#     geom_p = dce.transform_dict_to_poly(geom_d,tbox_crs,cog_crs) #Transformer au crs du cog directement

#     #TODO : align geom (that is in the cog crs) to the target crs
#     ori_w,ori_s,ori_e,ori_n = geom_p.bounds #OK
    
    
    
    
    
    
#     if datacube_aligned:
#         #TODO changer le nom de la variable datacube_aligned pour quelque chose comme lcc aligned ou autre
#         std_w, std_s,std_e,std_n = dce.datacube_origin(ori_w,
#                                                         ori_s,
#                                                         ori_e,
#                                                         ori_n,
#                                                         window_resolution,
#                                                         national)
#     else:
#         std_w, std_s,std_e,std_n = dce.tap_origin(ori_w,
#                                                    ori_s,
#                                                    ori_e,
#                                                    ori_n,
#                                                    window_resolution)
#     std_tbox = f'{std_w}, {std_s},{std_e},{std_n}'
#     std_geom = dce.poly_to_dict((dce.tbox_to_poly(std_tbox)))

#     # Retransform standard window to image native crs
#     if cog_crs != dce._crs:
#         # Reproject std_geom to input image crs
#         final_geom = dce.transform_dict_to_dict(std_geom,dce._crs,cog_crs)
#     else:
#         # Keep the Datacube std crs defined geobox
#         final_geom = std_geom
#     return final_geom



# def dict_to_vector_file(self,dict_poly,crs,outpath,filename,driver='GeoJSON'):
#     """ Converts a polygon dict geojson defintion to a vector definition file, see geopandas for vector file driver options
#         poly={'type': 'Polygon', 'coordinates': (((-419184.0, 1231541.0), (-419184.0, 1345927.2), (-486516.2, 1345927.2), (-486516.2, 1231541.0), (-419184.0, 1231541.0)),)}
#         crs='EPSG:3979'"""
#     # check outpath exists
#     outpath=self.check_outpath(outpath)
#     gs=gpd.GeoSeries(shape(dict_poly),crs=crs)
#     fname=os.path.join(outpath,filename)
#     gs.to_file(os.path.join(outpath,filename),driver='GeoJSON')
#     return fname

#  def transform_dict_to_dict(self,bbox_dict,in_crs,out_crs):
#      """ Reproject geojson dict to geojson_dict """
#      g = warp.transform_geom(in_crs,out_crs,bbox_dict)
#      return g