"""
Test cases for dc_extract.extract
"""
import pathlib
import ccmeo_datacube.extract.extract as dce

def test_params():
    """Default test study area / region d'etude"""
    # Fred 5x5
    tbox = '2150469.4724999964,144975.05299999937,2155469.4724999964,149975.05299999937'
    tbox_crs = 'EPSG:3979'
    return tbox,tbox_crs

def test_dict_make_vector_file():
    """Testing the vector file creation from a tbox"""
    tbox,tbox_crs = test_params()
    dex = dce.DatacubeExtract()
    out_path = pathlib.Path.cwd()
    bbox_dict = dex.poly_to_dict(dex.tbox_to_poly(tbox))

    print(dex.dict_to_vector_file(bbox_dict,
                                  tbox_crs,
                                  out_path,
                                  'test_bbox.geojson',
                                  driver='GeoJSON'))
    return

def test_multiproj():
    """Testing the multiproj support of clip to grid"""
    utm = ('https://datacube-stage-data-public.s3.ca-central-1.amazonaws.com/share/hrdem-utm/NBDNR-2015-1m-dtm.tif')
    lcc = ('https://datacube-stage-data-public.s3.ca-central-1.amazonaws.com/share/hrdem-lcc/NBDNR-2015-1m-dtm.tif')
    tbox,tbox_crs = test_params()
    dex = dce.DatacubeExtract()
    out_file = pathlib.Path.cwd()/'test.tif'

    print(dex.clip_to_grid(utm,out_file,tbox,suffix='UTM',national=False))
    print(dex.clip_to_grid(lcc, out_file, tbox,suffix='LCC',national=False))

    return

def test_asset_urls():
    tbox,tbox_crs = test_params()
    dex = dce.DatacubeExtract()
    urls = dex.asset_urls('hrdem',tbox,tbox_crs)
    return urls
