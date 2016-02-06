#!/usr/bin/python
#
# Download_script for the Dataset
#
#
# 

import sys, os, urllib, Image

baseurl = "http://www.panoramio.com/wapi/template/photo.html?tag='buildings'&minx=-180&miny=-90&maxx=180&maxy=90&size=medium&mapfilter=true"

def jpg_ok(jpg):
    if not os.path.exists(jpg):
        return false
    f = open(jpg)
    s = f.read(2)
    f.close()
    return s == '\xff\xd8'

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print "Syntax: %s images_panoramio.txt" % sys.argv[0]
        sys.exit(0)

    filelist = sys.argv[1]

    if not os.path.exists(filelist):
        print "Could not find %s" % filelist
        sys.exit(1)

    files_panoramio = [l.strip().split()[0] for l in open(filelist).readlines()]

    flog = open(filelist + ".log", "w")

    def log(s):
        flog.write(s + "\n")
        print s

    cwd = os.getcwd()
    basedir = os.path.join(cwd, os.path.basename(filelist).replace(".txt",""))
    if not os.path.exists(basedir):
        os.mkdir(basedir)

    filecounter = 0
    foldercounter = 0
    for file in files_panoramio:
        if filecounter % 500 == 0:
            foldercounter += 1
            currfolder = os.path.join(basedir, str(foldercounter).rjust(5,'0'))
            if not os.path.exists(currfolder):
                os.mkdir(currfolder)

        img_url = baseurl + file
        log(img_url)

        jpgfile = os.path.join(currfolder, file)
        urllib.urlretrieve(img_url, jpgfile)

        retry = 0
        while ( not jpg_ok(jpgfile) ) and retry < 5:
            log("Oops, file broken. Retrying.")
            urllib.urlretrieve(img_url, jpgfile)
            retry += 1

        if retry > 0:
            if jpg_ok(jpgfile):
                log("File retrieved successfully.")
            else:
                log("Didn't work out. Giving up.")
                if os.path.exists(jpgfile):
                    os.remove(jpgfile)
                continue

        img = Image.open(jpgfile)
        max_size = max(img.size)

        if max_size >= 1024:
            try:
                scale = 1024.0 / max_size
                newsize = (int(img.size[0] * scale), int(img.size[1]*scale))
                img.resize(newsize, resample=Image.ANTIALIAS)
                img.save(jpgfile)
            except:
                log("Error scaling image!")

        filecounter += 1

    flog.close()
