# Need to test the looping, for now looks like the same next key is passed back

import sys
sys.path.insert(0,'S:\\')
import ccmeo_datacube.extract.extract as dce
big = '-150,43,-40,90'
little = '-75,45,-74,46'
df = dce.asset_urls('hrdem-lidar',little,'EPSG:4326')
urls = [u for u in df.url]
urls_resolution_asc = dce.order_by(df,'resolution',False)