from pyspark import SparkContext
import sys
import string

def word_split(line):
    for c in string.punctuation + "¿!«»":
        line = line.replace(c,' ')
        line = line.lower()
    return len(line.split())

def main(infilename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(infilename)
        words_rdd = data.map(word_split)
        total = words_rdd.sum()
        if infilename=='quijote_s05.txt':		
            salida1 = open('out_quijote_s05.txt', 'w')
            salida1.write(str(total))
            salida1.close()
        else: 						 
            salida2 = open('out_quijote.txt', 'w')
            salida2.write(str(total))
            salida2.close()
        print(total)


if __name__ == "__main__":
  
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} <infilename>")
        exit(1)
    infilename = sys.argv[1]

    main(infilename)
