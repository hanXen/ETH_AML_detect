import os
import shutil

path = os.getcwd()
path = path+'\\transactions'
idx = 0
for filename in os.listdir(path):
    idx += 1
    new_filename = 'transactions_{}.csv'.format(idx)
    os.chdir(path)
    print(filename)
    os.rename(filename,new_filename)