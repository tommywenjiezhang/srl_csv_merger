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
from FileOpener import FolderOpener
import subprocess
import sys
import webbrowser
import multiprocessing
import time
from Merger import Merger
from FileUtils import readCsvDir
from FileOperation import FileOperation



class ReportMerger(Merger):
    CSV = 'CSV'
    HTML = 'HTML'
    DATAFRAME = 'DATAFRAME'
    
    
    def __init__(self,path):
        super().__init__(path)
        self.all_files = readCsvDir(path)
        self.output = None
    
    def _single_report(self, f):
        columsSelected=["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" ,"HealthCareProfessionalInformation_specialty1", "filename" ]
        return FileReport(f).get()
    
    def getColumnReport(self):
        reports = [FileReport(f).columnsListReport() for f in self.all_files]
        combined =  pd.concat(reports)
        if os.path.exists('./reports'):
            fullpath = os.path.join('./reports', 'columnReport.csv')
            combined.to_csv(fullpath)
    
    
    def getEmailReport(self):
        reports = [FileReport(f).getEmailStatsReport() for f in self.all_files]
        reports = list(filter(lambda x: x is not None,reports))
        combined =  pd.concat(reports)
        self.output = combined
        return self
    
    
    
    def out(self,outputFormat,fpath):
        if self.output == None:
            return None
        else:
            if outputFormat == ReportMerger.CSV:
                csv_output = self.output.stack(0)
                if not os.path.exists('./reports'):
                    os.makedirs('./reports/csvFileReport')
                    fullpath = os.path.join('./reports/csvFileReport',fpath)
                    csv_output.to_csv(fullpath)
            elif outputFormat == ReportMerger.HTML:
                def color_negative_red(val):
                    color = 'red' if val == 0 else 'black'
                    return 'color: %s' % color
                if not os.path.exists('./reports/htmlFileReport'):
                    os.makedirs('./reports/htmlFileReport')
                    htmlpath = os.path.join('./reports/htmlFileReport',fpath)
                    html = self.output.style.format({'count_nulls': "{:.1f}", 'count_unique': '{:.1f}'}).set_properties(**{'font-size': '11pt','background-color': '#edeeef','border-color': 'black','border-style' :'solid' ,'border-width': '0px','border-collapse':'collapse'}).applymap(color_negative_red).render()
                    with open(htmlpath, "w") as text_file:
                        text_file.write(html)
                    webbrowser.open_new('file://' + os.path.realpath(htmlpath))
            else:
                return self.output
                        
        
    def merge(self, fpath):
        reports = []
        for f in self.all_files:
            report = self._single_report(f)
            reports.append(report)
        reports = list(filter(lambda x: x is not None,reports))
        combin_report = pd.concat(reports)
        combin_report.fillna(0, inplace=True)
        excel_version = combin_report.stack(0)
        if not os.path.exists('./reports'):
            os.makedirs('./reports/csvFileReport')
        fullpath = os.path.join('./reports/csvFileReport',fpath)
        excel_version.to_csv(fullpath)
        def color_negative_red(val):
            color = 'red' if val == 0 else 'black'
            return 'color: %s' % color
        html = combin_report.style.format({'count_nulls': "{:.1f}", 'count_unique': '{:.1f}'}).set_properties(**{'font-size': '11pt','background-color': '#edeeef','border-color': 'black','border-style' :'solid' ,'border-width': '0px','border-collapse':'collapse'}).applymap(color_negative_red).render()
        htmlpath = os.path.join('./reports/csvFileReport','tablestats.html')
        with open(htmlpath, "w") as text_file:
            text_file.write(html)
        webbrowser.open_new('file://' + os.path.realpath(htmlpath))
    
    
    def show(self):
        for f in self.all_files:
            fo = FileReport(f)
            columsSelected=["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" ,"HealthCareProfessionalInformation_specialty1", "filename" ]
            df = fo.get()
            yield df

if __name__ == "__main__":
    # filename = FolderOpener().OpenDir()
    r = ReportMerger('../newRaw')
    r.merge('tabstats.csv')
    
    
