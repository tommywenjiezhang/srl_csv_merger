import glob
import pandas as pd
import os
import re
import qgrid as qd
import numpy as np
import difflib
from TableOperation import TableOperation
from FileReport import FileReport
from MatchTable import MatchTable
from FileOpener import FolderOpener
from FileData import FileData
import subprocess
import sys
import webbrowser
import multiprocessing
import time
from Merger import Merger


class FilesMerger(Merger):
    CPU_COUNT = multiprocessing.cpu_count()
    def __init__(self,path):
        super(FilesMerger,self).__init__(path)
        
    def show(self):
        for f in self.all_files:
            fo = FileData(f)
            df = fo.get()
            yield df
            
    
    def bulk_operation(self):
        with multiprocessing.Pool(FilesMerger.CPU_COUNT) as pool:
            results = pool.map(FilesMerger.combin_operation, self.all_files)
        return results
    
    @staticmethod
    def combin_operation(filename):
        fo = FileData(filename)
        df = fo.get()
        return df
    
    def merge(self,file):
        results = self.bulk_operation()
        df = pd.concat(results)
        # min_number_of_value = 10
        # df = df.loc[:, (df.notnull().sum(axis=0) >= min_number_of_value)]
        # hasEmailorPhone = np.logical_or(pd.notnull(df["UserEmail"]),pd.notnull(df["WorkNumber"]))
        # df = df.loc[hasEmailorPhone]
        
        # df.drop_duplicates(inplace=True)
        # df = df.sort_values(by=["UserEmail", "CompanyName"], ascending=True, na_position="last")
        moveItems = ["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" ,"filename" ]
        tb = TableOperation(df)
        df = tb.moveColumnsToFront(moveItems)
        if not os.path.exists('./finalMerge'):
            os.mkdir('./finalMerge')
        fullpath = os.path.join('./finalMerge', file)
        df.to_csv(fullpath)
        # self.writeMatchTableReport()
        return df
        
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
    filename = FolderOpener().OpenDir()
    t1_start = time.perf_counter() 
    fm = FilesMerger(filename)
    fm.merge('final_merge_ver5.csv')
    ## Specify Folder Name You want you output to
    # fm.writeFolderStatsReport(r'./TableStats')
    # next_df = fm.readPerTable()
    # g = next(next_df)
    # print(g['UserEmail'])
    t1_stop = time.perf_counter() 
    print("Elapsed time:", t1_stop, t1_start)
    print("Elapsed time during the whole program in seconds:", t1_stop-t1_start) 
    
    
   
    
    
            
