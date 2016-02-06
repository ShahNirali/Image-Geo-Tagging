__author__ = 'Nirali'

import json
import os
import time
import requests
from PIL import Image
from StringIO import StringIO
from requests.exceptions import ConnectionError
import hashlib


def download(query, path):
    """Download full size images from Google image search.
  Don't print or republish images without permission.
  I used this to train a learning algorithm.
  """
    BASE_URL = 'https://ajax.googleapis.com/ajax/services/search/images?' \
               'v=1.0&q=' + query + '&start=%d'

    BASE_PATH = os.path.join(path, query)

    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)

    start = 0  # Google's start query string parameter for pagination.
    while start < 40:  # Google will only return a max of 56 results.
        r = requests.get(BASE_URL % start)
        for image_info in json.loads(r.text)['responseData']['results']:
            url = image_info['unescapedUrl']
            try:
                image_r = requests.get(url)
            except ConnectionError, e:
                print 'could not download %s' % url
                continue

            # Remove file-system path characters from name.
            title = image_info['titleNoFormatting'].replace('/', '').replace('\\', '')
            hs=hashlib.md5(url).hexdigest()

            file = open(os.path.join(BASE_PATH, '%s.jpg') % hs, 'w')
            try:
                Image.open(StringIO(image_r.content)).save(file, 'JPEG')
                outfile.write(hs+".jpg | "+location+" | "+longi+" | "+lati+" | "+addr+"\n")
            except IOError, e:
                # Throw away some gifs...blegh.
                print 'could not save %s' % url
                continue
            finally:
                file.close()

        print start
        start += 4  # 4 images per page.

        # Be nice to Google and they'll be nice back :)
        time.sleep(1.5)


infile = open("/Users/Nirali/Desktop/MetadataExtract_11.txt")
outfile =open('/Users/Nirali/Desktop/dataset.txt','w')
for line in infile:
    if line.strip().startswith('\"coordinates'):
        longi = infile.next().replace(',', '').strip()
        lati = infile.next().replace(',', '').strip()
        if " " in longi:
            longi="null"
        if " " in lati:
            lati="null"
        #print(longi+','+lati)

    if line.strip().startswith('\"State'):
        location = line.strip().replace('"State": ', '').strip()
        location = location.replace(',', "").replace('\"', '')
        if " " in location:
            location="null"

    if line.strip().startswith('\"Add'):
        addr=line.strip().replace('"Add": ', '').strip()
        addr = addr.replace(',', "").replace('\"', '')


    if line.strip().startswith('\"Fcilty_nam'):
        name = line.strip().replace('"Fcilty_nam": ', "").strip()
        name = name.replace(',', '').replace('"','').replace(' ','+')
        print(name)
        download(name, 'Images')

infile.close()
outfile.close()


