#!/usr/bin/env python
import pymysql
import re
import os
import shutil
import csv
from progress.bar import Bar
from dateutil import rrule
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

#connect to db
conn = pymysql.connect(unix_socket="PATH_TO_LOCAL_MYSQL/mysql.sock", port=3306, user='root', passwd='root', database='mdid')

images=[]
imagefiles=[]
start = datetime(2011, 01, 01)
end = datetime.now()
batchfolder = ""
writer = ""

def create_folder(s,e):
	#create a folder to hold the new csv and all the images that will be copied
	path='''PATH_TO_NEW_FOLDER/{0}_{1}'''
	s=s.strftime('%Y-%m-%d')
	e=e.strftime('%Y-%m-%d')
	global batchfolder 
	batchfolder = path.format(s,e)
	if not os.path.exists(batchfolder): os.makedirs(batchfolder)

def create_csv(s,e):
	#create and open csv
	s=s.strftime('%Y-%m-%d')
	e=e.strftime('%Y-%m-%d')
	csvpath='''%s/%s_%s.csv''' % (batchfolder,s,e)
	global f
	f=open(csvpath, 'w')

	#print csv headers
	fieldnames = ('SSID','Filename','Creator[16684]','Culture[16685]','Title[16686]','Image View Description[16687]','Image View Type[16688]','Date[16689]','Earliest Date[16690]','Latest Date[16691]','Style/Period[16692]','Materials/Techniques[16693]','Measurements[16694]','Artstor Classification[16695]','Work Type[16696]','Repository[16697]','Accession Number[16698]','Location[16699]','Creation/Discovery Site[16700]','Latitude[16701]','Longitude[16702]','Elevation[16703]','Country[16704]','Description/Work Notes[16705]','Subject[16706]','Relationships[16707]','Source[16708]','Photographer[16709]','Image Date[16710]','ID Number[16711]','Rights[16712]','Order Number[16714]','Patron[16715]')
	global writer
	writer = csv.DictWriter(f, fieldnames=fieldnames, restval='', dialect='excel', quoting=csv.QUOTE_ALL)
	headers = dict( (n,n) for n in fieldnames )
	writer.writerow(headers)

def write_rows(s,e):
	global conn
	global writer
	cur = conn.cursor(pymysql.cursors.DictCursor)
	q="""SELECT * from `images` WHERE `CollectionID` = '11' AND `Created` >= '{0}' AND `Created` < '{1}' ORDER BY `Created`;"""
	s=s.strftime('%Y-%m-%d 00:00:00')
	e=e.strftime('%Y-%m-%d 00:00:00')
	#print q.format(s,e)
	cur.execute(q.format(s,e))
	for r in cur.fetchall():
		images.append(r['ID'])
	#print images
	cur.close()
	
	#query db for fields associated with each imageID
	for i in images:
		cur = conn.cursor(pymysql.cursors.DictCursor)
		cur.execute("SELECT * from `fielddata` WHERE `ImageID` = %s", i)
		d={}
		for r in cur.fetchall():
			#construct dictionary of fieldName -> fieldValue pairs
			field = (r['FieldID'])
			if field == 132:
				d['Filename']=r['FieldValue']
			elif field == 131:
				d['Creator[16684]']=r['FieldValue']
			elif field == 126:
				d['Title[16686]']=r['FieldValue']
			elif field == 124:
				d['Date[16689]']=r['FieldValue']
			elif field == 120:
				d['Style/Period[16692]']=r['FieldValue']
			elif field == 123:
				d['Materials/Techniques[16693]']=r['FieldValue']
			elif field == 122:
				d['Measurements[16694]']=r['FieldValue']
			elif field == 118:
				d['Accession Number[16698]']=r['FieldValue']
			elif field == 130:
				d['Location[16699]']=r['FieldValue']
			elif field == 129:
				d['Country[16704]']=r['FieldValue']
			elif field == 119:
				d['Description/Work Notes[16705]']=r['FieldValue']
			elif field == 127:
				d['Subject[16706]']=r['FieldValue']
			elif field == 128:
				d['Source[16708]']=r['FieldValue']
			elif field == 121:
				d['Order Number[16714]']=r['FieldValue']
			elif field == 230:
				d['Patron[16715]']=r['FieldValue']
			d['SSID']='NEW'
			
		if 'Filename' in d:
			imagefiles.append(d['Filename'])
			writer.writerow(d)
		else:
			write_error(i)
		cur.close()
	f.close()

def write_error(i):
	with open('PATH_TO_ERROR_FILE/errors.txt', 'a') as error_file:
		error_file.write(str(i)+'\n')

def copy_image_files(s,e):
	print 'Processing %s images from %s to %s' % (len(images),s,e)
	#this path is to a mounted share
	fullpath = '/Volumes/MediaDB/www/oncampus/artidb/full'

	bar = Bar('Processing', max=len(images))
	for i in imagefiles:
		src=fullpath+'/'+i
		dst=batchfolder+'/'+i
		try: 
			shutil.copyfile(src, dst)
		except IOError:
			with open('PATH_TO_ERROR_FILE/errors.txt', 'a') as error_file:
				error_file.write("Couldn't copy "+i+'\n')

		bar.next()
	bar.finish()

while start < datetime.now():
    s=start
    e=start+relativedelta(months=3)
    images=[]
    imagefiles=[]
    
    #create folder for batch
    create_folder(s,e)

    #create csv and open for writing and write headers
    create_csv(s,e)

    #find images created between s and e and write rows to CSV
    write_rows(s,e)

    #copy images to local storage
    copy_image_files(s,e)

    start=e

conn.close()
