
import random
import sys
import string
from pyspark import SparkContext


def main(infile, outfile, ratio):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(infile)
        new_data = data.filter(lambda x: random.random() > ratio)
        with open(outfile, "w") as file:
            ss = "".join(new_data.collect())
            file.write(ss)
            
            

if __name__=="__main__":
    if len(sys.argv)<4:
        infilename, outfilename, ratio = "quijote.txt", "quijote_s05.txt", random.random()
    else:
        infilename = sys.argv[1]
        outfilename = sys.argv[2]
        ratio = float(sys.argv[3])
    main(infilename, outfilename, ratio)
