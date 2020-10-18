import glob
import pandas as pd
import os
import re
import qgrid as qd
import numpy as np
import difflib
from FileOperation import FileOperation 

class FilesMerger:
    def __init__(self,path):
        self.path = path
        self.all_files = self.readDirCsv()
        self.results = self.bulk_operation()
        
    def readDirCsv(self):
        all_files = glob.glob(self.path + "/*.csv")
        return all_files
    
    def bulk_operation(self):
        results = [FileOperation(filename).getExtractedDataBase() for filename in self.all_files]
        return results
    
    def concat(self):
        df = pd.concat(self.results)
        min_number_of_value = 10
        df = df.loc[:, (df.notnull().sum(axis=0) >= min_number_of_value)]
        moveItems = ["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" , "filename"]
        df = df[[y for y in moveItems]+[x for x in df if x not in moveItems ]]
        df.to_csv("out/finalMerge_ver4.csv")
        return df
        
if __name__ == '__main__':
    path = r'raw'
    fm = FilesMerger(path)
    fm.concat()
    
    
            
