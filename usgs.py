#################################################################

import os

import numpy as np
import pandas as pd

import landsatxplore.api
from landsatxplore.earthexplorer import EarthExplorer

import boto3

##################################################################

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

data = pd.read_csv("all_regions_data.csv")
data = data[["place","lat","lon"]]
data = data.drop_duplicates()

##################################################################

for _, row in data.iterrows():
      
    api = landsatxplore.api.API('sagar123', 'passwordis123')
    scenes = api.search(
    dataset='LANDSAT_8_C1',
    latitude=row['lat'],
    longitude=row['lon'],
    start_date='2016-01-01',
    end_date='2019-11-01',
    max_cloud_cover=10)
    
    print('{} scenes found.'.format(len(scenes)))
    
    length = len(scenes)
    place = row['place']
    
    place_dir = os.path.join("landsat",f'{place}')
    path =  os.path.join(f'{os.getcwd()}',f'{place_dir}')
    print(path) 
    
    if not os.path.exists(path):
        os.makedirs(path)
           
    while(length>1):
        
        ee = EarthExplorer('sagar123', 'passwordis123')
        #ee.download(scene_id=scenes[length-1]['displayId'], output_dir=f'{path}')
        obj = s3_resource.Object(
            bucket_name="sabudh-displacement-data", key=(f"{place}/{scenes[length-1]['displayId']}.tif"))
        obj.upload_file(f"{place_dir}/{scenes[length-1]['displayId']}.tif")
        os.remove(f"{place_dir}/{scenes[length-1]['displayId']}.tif")
        length = length-1
