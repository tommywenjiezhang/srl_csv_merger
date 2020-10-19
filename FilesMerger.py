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
        hasEmailorPhone = np.logical_or(pd.notnull(df["UserEmail"]),pd.notnull(df["WorkNumber"]))
        df = df.loc[hasEmailorPhone]
        df.to_csv("out/finalMerge_ver4.csv")
        df.drop_duplicates(inplace=True)
        df = df.sort_values(by=["UserEmail", "CompanyName"], ascending=True, na_position="last")
        return self.clean(df)
    
    def clean(self, final_merge):
        final_merge = final_merge.loc[pd.notnull(final_merge["UserEmail"])]
        final_merge.drop_duplicates(subset=['UserEmail', 'CompanyName'], keep='first')
        CompanyNameCheck = pd.isnull(final_merge.CompanyName)
        UserFirstNameCheck = pd.isnull(final_merge.UserFirstName)
        UserLastNameCheck = pd.isnull(final_merge.UserLastName)
        removeNoName = np.logical_not(np.logical_and.reduce((CompanyNameCheck, UserFirstNameCheck, UserLastNameCheck)))
        final_merge = final_merge[removeNoName]
        final_merge = final_merge.drop(final_merge.columns[0], axis=1)
        final_merge.sort_values(by=['UserEmail', 'CompanyName'], ascending=False,inplace=True)
        final_merge = final_merge[final_merge.UserEmail.str.match(r"[^@]+@[^@]+\.[^@]+", na=False)]
        return final_merge
if __name__ == '__main__':
    path = r'raw'
    fm = FilesMerger(path)
    fm.concat()
    
    
            
