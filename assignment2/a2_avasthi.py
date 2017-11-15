from pyspark import SparkContext
from tifffile import TiffFile
import re
import os
import io
import zipfile
import numpy as np

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

def calculateIntensity(name, arr):
	intensity = np.zeros(shape=(500,500))
	print(name)
	for i in range(500):
		temp = []
		for j in range(500):
			rgb = arr[i][j]
			intensity[i][j] = int((int(rgb[0])+int(rgb[1])+int(rgb[2])/3) * (rgb[3]/100))
	print(intensity)
	return (name, intensity)
	

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
	print_blocks.collect()

	intensity = blocks.map(lambda a:calculateIntensity(a[0], a[1]))
	print(intensity.collect())




