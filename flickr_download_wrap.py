# -*- coding: utf-8 -*-
"""
Created on Wed Nov 04 16:38:22 2015

@author: Subhankari
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 15:01:21 2015

@author: Subhankari
"""

import flickr
import urllib, urlparse
import os
from os import listdir
from os.path import isfile, join
from flickr import Photo


def retrieve_image(img_id,secret_id,server_id):
    flickrAPIKey = "306d88a89940cd6c1c2b1321cc57969d"  # API key
    flickrSecret = "ce719a6e1aa8017f" 
    p = Photo(img_id)
    f = p.getSizes()
    image_retrieved = 0
    for url in f:
        print "keys"
        label = url.get('label', "none")
        if "Large" in label:
            source_url = url.get('source',"none")
            print source_url
            image_retrieved = 1
            image = urllib.URLopener()
            image.retrieve(source_url, os.path.basename(urlparse.urlparse(source_url).path)) 
            #print 'downloading:', url
        if image_retrieved == 1:
            break
        
                            
mypath = "C:\\Users\\Subhankari\\Desktop\\desktop_as_on_13th_april\\text books\\ML\\project\\bigdata files";

onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
print onlyfiles

fl = open('metafile.txt', 'w')
photo_id = '0'
photo_secret = '0'
lat = '0'
lon = '0'
i = 0
for f in listdir(mypath):
        if isfile(join(mypath,f)):
            photo_flag = 0
            with open(join(mypath,f)) as txtfile:
                file_parts = f.split('.')
                counter = 0
                for part in file_parts:
                    if counter == 0:
                        tag = part
                    counter = counter + 1
                for line in txtfile:
                    i = i + 1
                    if line.startswith('photo:'):
                        i = 1
                        photo_flag = 1
                        photo_val = line[7:]
                        photo_parts = photo_val.split(" ")
                        counter = 0;
                        for parts in photo_parts:
                            if counter == 0:
                                photo_id = parts
                                print photo_id
                            if counter == 1:
                                photo_secret = parts
                                print photo_secret
                            if counter == 2:
                                photo_server = parts
                                print photo_server
                            counter += 1
                        retrieve_image(photo_id,photo_secret,photo_server)
                    if line.startswith('latitude: '):
                        i = 2
                        lat = line[10:]
                        print "lat"
                        print lat
                    if line.startswith('longitude: '):
                        i = 3
                        lon = line[11:]
                        print "lon"
                        print lon
                        #print "\n"
                    if i == 3:
                        fl.write(photo_id + '_' + photo_secret + '.jpg' + ' ' + '|' + ' ' + 'null' + ' ' + '|' + ' ' + lat + ' ' + '|' + ' ' + lon + ' ' + '|' + ' ' + tag +' ' + '|' + ' '+ 'null' + '\n')
fl.close()