import csv
import os

path = os.getcwd()
path = path+'\\transactions'

def parse(filename,output):
    f1 = open(path+'\\'+filename, 'r', encoding ='utf-8')
    f2 = open(path +'\\data\\'+output , 'w', encoding = 'utf-8', newline='')

    rdr = csv.reader(f1)
    wr = csv.writer(f2)
    i= 0 
    try:
        for line in rdr:
            if i != 0:
                line[7] = str(float(line[7])/1000000000000000000)
            wr.writerow([line[0], line[5], line[6], line[7], line[11]])
            i += 1

    except:
        pass

    f1.close()
    f2.close()