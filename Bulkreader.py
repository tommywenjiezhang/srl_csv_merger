import glob
import pandas as pd
import os
import re
import qgrid as qd
import numpy as np
import difflib

class BulkReader:
    def __init__(self,path):
        self.path = path
        self.all_files = self.readDirCsv()
        self.results = None 
        
    def readDirCsv(self):
        all_files = glob.glob(self.path + "/*.csv")
        return all_files
    
    def write_bulk_report(self,func,save_file_name):
        report_writer = pd.ExcelWriter(save_file_name, engine='xlsxwriter')
        report_writer.save()
    
    def bulk_operation(self, func, values):
        self.results = [func(filename,values) for filename in self.all_files]
        return self
        
    
        
            
