import csv_parser
import os


path = os.getcwd()
path = path+'\\transactions'
idx = 0
for filename in os.listdir(path):
    idx += 1
    try:
        csv_parser.parse('transactions_{}.csv'.format(idx),'tx_data{}.csv'.format(idx))
    except:
        pass