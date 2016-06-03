
import glob
import mincemeat
import csv

# Alterar a localizacao dos arquivos
text_files = glob.glob('D:\\map_reduce\\arquivo\\Trab2.3\\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    except:
        print "Error!"
    finally:
        f.close()

datasource = dict((file_name, file_contents(file_name)) for file_name in text_files)

def mapfn(key, value):
    import re
    from stopwords import allStopWords
    for line in value.splitlines():
        wordsinsentence = line.split(":::")
        authors = wordsinsentence[1].split("::")
        # print autores
        words = str(wordsinsentence[2])
        words = re.sub(r'([^\s\w-])+', '', words)
        # re.sub(r'[^a-zA-Z0-9: ]', '', words)

        words = words.split(" ")
        for author in authors:
            for word in words:
                word = word.replace("-"," ")
                word = word.lower()
                if (word not in allStopWords):
                    yield author, word

def reducefn(key, value):
    from collections import Counter
    return Counter(value)

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="changeme")

# print results
w = csv.writer(open('D:\\map_reduce\\arquivo\\output.csv', "w"))
for k, v in results.items():
    w.writerow([k, str(v).replace("[","").replace("]","").replace("'","").replace(' ','')])
