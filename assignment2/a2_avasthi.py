from pyspark import SparkContext
from tifffile import TiffFile
import re
import os
import io
import sys
import zipfile
import numpy as np
import hashlib
import math

def getOrthoTif(zfBytes):
	#given a zipfile as bytes (i.e. from reading from a binary file),
	# return a np array of rgbx values for each pixel
	bytesio = io.BytesIO(zfBytes)
	zfiles = zipfile.ZipFile(bytesio, "r")
	#find tif:
	for fn in zfiles.namelist():
		if fn[-4:] == '.tif':#found it, turn into array:
			tif = TiffFile(io.BytesIO(zfiles.open(fn).read()))
	return tif.asarray()

def divideImage(fileName, imageArr):		#Q1c
	dividedArray = []
	k = 0
	for i in range(5):
		for j in range(5):
			arr = np.array(imageArr)
			row = i*500
			col = j*500
			portion_name = fileName+"-"+str(k)
			dividedArray.append((portion_name, arr[row:row+500, col:col+500]))
			k += 1
	return dividedArray

def printrgb(name, arr):
	if(name == '3677454_2025195.zip-0' or name == '3677454_2025195.zip-1' or name == '3677454_2025195.zip-18' or name == '3677454_2025195.zip-19'):
		print(arr[0][0])
	return (name, arr)

def calculateIntensity(name, arr):			#Q2a
	intensity = np.zeros(shape=(500,500))
	for i in range(500):
		for j in range(500):
			rgb = arr[i][j]
			intensity[i][j] = int(((int(rgb[0])+int(rgb[1])+int(rgb[2]))/3) * (int(rgb[3])/100))			
	return (name, intensity)

def reduceFactor(fileName, intensityArr):		#Q2b
	reducedArr = np.zeros(shape=(50,50))
	k = 0
	for i in range(50):
		for j in range(50):
			arr = np.array(intensityArr)
			row = i*10
			col = j*10
			reducedArr[i][j] = mean(arr[row:row+10, col:col+10])
	return (fileName, reducedArr)

def mean(arr):
	sum = 0
	for i in range(10):
		for j in range(10):
			sum += arr[i][j]
	return sum/100

def matrixDiff(fileName, reducedArr):			#Q2c, Q2d and Q2e
	temp = np.array(reducedArr)
	rowDiff = np.diff(reducedArr, axis=1)
	colDiff = np.diff(temp, axis=0)
	feature = []
	rowFlat = rowDiff.flatten()
	colFlat = colDiff.flatten()
	feature = np.concatenate((rowFlat, colFlat))
	for i in range(len(feature)):
		if(feature[i] < -1):
			feature[i] = -1
		elif(feature[i] > 1):
			feature[i] = 1
		else:
			feature[i] = 0
	if(fileName == '3677454_2025195.zip-1' or fileName == '3677454_2025195.zip-18'):
		print(feature)
	return (fileName, feature)

def printfeature(name, arr):				#Q2f
	if(name == '3677454_2025195.zip-1' or name == '3677454_2025195.zip-18'):
		print(arr)

def sign(fileName, feature):				#Q3a			
	bytes = np.zeros(shape=(128))
	for i in range(128):
		arr = np.array(feature)
		row = i*38
		md = hashlib.md5(arr[row:row+38])
		digest = md.hexdigest()
		bit = int(digest, 16)
		bytes[i] = bin(bit)[3]
	return (fileName, bytes)

def LSH(fileName, bytes):
	hashes = np.zeros(shape=(16))
	for i in range(16):
		arr = np.array(bytes)
		row = i*8
		val = arr[row:row+8]
		hashes[i] = hash(val.tostring())
	return (fileName, hashes)

def similarity(hashValue, imageNames):
	import re
	images = re.split('[:]', imageNames)

	for image in images:
		if image.strip() == '3677454_2025195.zip-0':
			return ('3677454_2025195.zip-0', images)
		if image.strip() == '3677454_2025195.zip-1':
			return ('3677454_2025195.zip-1', images)
		if image.strip() == '3677454_2025195.zip-18':
			return ('3677454_2025195.zip-18', images)
		if image.strip() == '3677454_2025195.zip-19':
			return ('3677454_2025195.zip-19', images)
	return('',[])

def printSimiliar(fileName, similiar_files):
	if(fileName == '3677454_2025195.zip-1'):
		i = 0
		simi = []
		for file in similiar_files:
			if file.strip() != '3677454_2025195.zip-1':
				simi.append(file.strip())
				i += 1
			if(i == 20):
				break
		print(fileName, simi)
	elif(fileName == '3677454_2025195.zip-18'):
		i = 0
		simi = []
		for file in similiar_files:
			if file.strip() != '3677454_2025195.zip-18':
				simi.append(file.strip())
				i += 1
			if(i == 20):
				break
		print(fileName, simi)

	return (fileName, )
			

if __name__ == "__main__":

	os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.5"
	os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.5"

	filePath = "/home/prakhar/Study/3_ms/fall_2017/big_data_system/assignments/assignment2/a2_small_sample"
	np.set_printoptions(threshold=2500)
	sc = SparkContext("local", "a2_avasthi")
	sc.setLogLevel("ERROR")
	filePath = filePath
	filesRdd = sc.binaryFiles(filePath)
	zip_file_names = None

	zip_files = filesRdd.map(lambda a: a[0][a[0].rfind("/")+1:])
	zip_file_names = sc.broadcast([x for x in zip_files.toLocalIterator()])		#Q1a
	image_array = filesRdd.map(lambda a: a[1]).map(lambda x: getOrthoTif(x))	#Q1b
	file_image_array = filesRdd.map(lambda a: (a[0][a[0].rfind("/")+1:], getOrthoTif(a[1])))
	blocks = file_image_array.flatMap(lambda a:divideImage(a[0], a[1]))		#Q1d
	print_blocks = blocks.map(lambda a:printrgb(a[0], a[1]))			#Q1e
	#print_blocks.collect()

	intensity = print_blocks.map(lambda a:calculateIntensity(a[0], a[1]))			#Q2a
	reduced = intensity.map(lambda a:reduceFactor(a[0], a[1]))			#Q2b
	major = reduced.map(lambda a:matrixDiff(a[0], a[1]))				#Q2c, Q2d and Q2e
	#print_feature = major.map(lambda a:printfeature(a[0], a[1]))			#Q2f
	#print_feature.collect()

	signatures = major.map(lambda a:sign(a[0], a[1]))				#Q3a
	band = signatures.map(lambda a:LSH(a[0], a[1]))
	flip = band.flatMap(lambda a:[(x, a[0]+" : ") for x in a[1]])
	similiar = flip.reduceByKey(lambda a,b:a+b)
	similiar_combine = similiar.map(lambda a: similarity(a[0], a[1]))
	similarImage = similiar_combine.reduceByKey(lambda a,b:a+b)
	print_similarImage = similarImage.map(lambda a:printSimiliar(a[0], a[1]))
	print_similarImage.collect()



