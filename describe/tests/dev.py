import os
print(f'Original value: {os.environ["REQUESTS_CA_BUNDLE"]}')
print('cacert.pem fails VPN ca')
print('new decorator will temp. set REQUESTS_CA_BUNDLE to NRCAN-Root-2019-B64.cer')
import sys
sys.path.insert(0,'S:\\dc_extract')
import ccmeo_datacube.describe as d
d.search(bbox='-123.3250,51.9500,-120.1473,53.7507',cols='cdem')