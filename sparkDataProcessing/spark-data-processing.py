#!/usr/bin/python

import sys, getopt
import pyspark.sql.functions as f

from pyspark.sql.functions import sum, avg, max, min, mean, count
from pyspark.sql import SparkSession

def main(argv):

   inputfile = ''
   outputfile = ''

   try:

      opts, args = getopt.getopt(argv,"hi:o:",["input=","output="])

   except getopt.GetoptError:

      print 'test.py -a <application> -i <inputfile> -o <outputfile>'
      sys.exit(2)

   for opt, arg in opts:
      if opt == '-h':
         print 'spark-data-processing.py -i <inputfile> -o <outputfile>'
         sys.exit()
      elif opt in ("-i", "--input"):
         inputfile = arg
      elif opt in ("-o", "--output"):
         outputfile = arg
      elif opt not empty:

   print 'Input file is "', inputfile
   print 'Output file is "', outputfile



if __name__ == "__main__":
   main(sys.argv[1:])