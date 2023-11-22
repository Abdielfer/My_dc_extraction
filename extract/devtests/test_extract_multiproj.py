# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 16:13:42 2022

@author: ccrevier
Help correct an error happening when extracting cogs with different source crs into a mini-cube
"""
import pathlib
import extract as dce

def hrdem_files():
    """Create list of the cog filepath used in the extraction"""
    tiff_paths = []
    tiff_paths.append('https://datacube-stage-data-public.s3.ca-central-1.amazonaws.com/share/hrdem-utm/NBDNR-2015-1m-dtm.tif')
    tiff_paths.append('https://datacube-stage-data-public.s3.ca-central-1.amazonaws.com/share/hrdem-lcc/NBDNR-2015-1m-dtm.tif')
    return tiff_paths

def params():
    """Default test study area / region d'etude et selection of the file instersecting with it"""
    tbox = '2150469.4724999964,144975.05299999937,2155469.4724999964,149975.05299999937'
    tbox_crs = 'EPSG:3979'
    urls = hrdem_files()
    
    return tbox, tbox_crs, urls

def extract_multiproj(outdir, crs='3979') :
    """Testing the multiproj support of clip to grid"""
    out_dir = pathlib.Path(outdir)
    tbox,tbox_crs,urls = params()
    dex = dce.DatacubeExtract()
    list_outfile = []
    resolution = 5
    method = 'bilinear'
    for url in urls :
        clips = []
        if 'utm' in url:
            suffix = f'UTM_to_{crs}_datacube_aligned'
        elif 'lcc' in url :
            suffix = f'LCC_to_{crs}_datacube_aligned'
        path = pathlib.Path(url)
        out_file = out_dir.joinpath(f'{path.stem}-clip{path.suffix}')
        clip = dex.clip_to_grid(url,out_file,tbox,tbox_crs=tbox_crs,suffix=suffix,national=False,datacube_aligned=True)
        clips.append(clip)
    
        for clip in clips:
            fname = (f'{clip.stem}-{resolution}m'
                     f'-{method}{clip.suffix}')
            dst_file = out_dir.joinpath(fname)
            dex.reproject_image_to_pixel_size(clip,
                                              dst_file,
                                              dst_crs='EPSG:'+crs, #la projection UTM de la zone
                                              pixel_size=resolution,
                                              resample=method,
                                              datacube_aligned=True)
