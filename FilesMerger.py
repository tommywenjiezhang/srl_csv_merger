import glob
import pandas as pd
import os
import re
import qgrid as qd
import numpy as np
import difflib
from FileOperation import FileOperation 
from TableOperation import TableOperation
from FileReport import FileReport
from MatchTable import MatchTable

class FilesMerger:
    def __init__(self,path):
        self.path = path
        self.all_files = self.readDirCsv()
        self.results =  self.bulk_operation()
        
    def readDirCsv(self):
        all_files = glob.glob(self.path + "/*.csv")
        return all_files
    
    def bulk_operation(self):
        results = map(lambda x: FilesMerger.combinOperation(x), self.all_files)
        return results
    
    def writeMatchTableReport(self):
        for f in self.all_files:
            mt = MatchTable(pd.read_csv('schema/schema2.csv'),pd.read_csv(f, encoding = 'latin')).matchColumns()
            FileReport.MatchTableReport(f, mt)
    
    def writeFolderStatsReport(self):
        reports = []
        for f in self.all_files:
            fo = FileOperation(f)
            columsSelected = ["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" ,"filename" ]
            reports.append(fo.getColumnsStats(columsSelected))
        combin_report = pd.concat(reports)
        combin_report.fillna(0, inplace=True)
        def color_negative_red(val):
            color = 'red' if val == 0 else 'black'
            return 'background-color: %s' % color
        html = combin_report.style.applymap(color_negative_red).render()
        with open("debug/tablestats.html", "w") as text_file:
            text_file.write(html)
            
    
    @staticmethod
    def combinOperation(file):
        fo = FileOperation(file)
        df = fo.getExtractedData()
        return df
    
    def concat(self):
        df = pd.concat(self.results)
        # min_number_of_value = 10
        # df = df.loc[:, (df.notnull().sum(axis=0) >= min_number_of_value)]
        # hasEmailorPhone = np.logical_or(pd.notnull(df["UserEmail"]),pd.notnull(df["WorkNumber"]))
        # df = df.loc[hasEmailorPhone]
        
        # df.drop_duplicates(inplace=True)
        # df = df.sort_values(by=["UserEmail", "CompanyName"], ascending=True, na_position="last")
        moveItems = ["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" ,"filename" ]
        tb = TableOperation(df)
        df = tb.moveColumnsToFront(moveItems)
        self.writeMatchTableReport()
        df.to_csv("out/finalMerge_ver4.csv")
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
    
    
import xlrd
import csv

def csv_from_excel(filename):
    
    outdir = './xlsxsave'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    fileCsveXT = re.sub(r"\.xlsx", "", filename) + ".csv"
    fileCsveXT = re.sub(r'[()_\s+]', "_", fileCsveXT)
    
    outputPath = os.path.join(outdir, fileCsveXT[4:])
    data = pd.read_excel(filename, sheet_name=0 )
    data.to_csv(outputPath,index=False,header=None ,mode="w", encoding='utf-8')



def readDirCsv(path):
        all_files = glob.glob(path + "/*.csv")
        return all_files
    
if __name__ == '__main__':
    fm = FilesMerger("./raw")
    fm.writeFolderStatsReport()
    # path = './raw'
    # for f in readDirCsv(path):
    #     fileCsveXT = re.sub(r"(\.csv)\.csv", "\1", f)
    #     os.rename(f,fileCsveXT)
    
    
   
    
    
            
