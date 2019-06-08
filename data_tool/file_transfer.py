import os
import shutil

path = os.getcwd() 
for dirname in os.listdir(path):
    dirname_path = path+"\\"+dirname
    try:
        dirname2 = os.listdir(dirname_path)
        dirname2_path = dirname_path+'\\'+dirname2[0]
        filename = os.listdir(dirname2_path)
        file_path = dirname2_path+'\\'+filename[0]
        print(file_path)
        shutil.copy(file_path, path)
    except:
        pass
        