import os
import numpy as np
import re
import pandas as pd
import glob
from FileReport import FileReport


def readExcelFiles(dir):
    all_files = glob.glob(dir + "/*.xlsx")
    return all_files


def readCsvDir(path):
    all_files = glob.glob(path + "/*.csv")
    if len(all_files) <= 0:
        print("Folder does not contain csv file")
        pass
    return all_files

def mismatch_email(series):
    return series[np.logical_not(series.str.match(r"[^@]+@[^@]+\.[^@]+"))]


def print_columns(*columns, **name):
    output = pd.DataFrame()
    for i in range(0,len(columns)):
        colName = name['name']
        output[f'{colName[i]}'] = pd.Series(columns[i]).sort_values(ascending=True)
    output.T.to_csv(rf"debug/{self.filename[4:9]}.csv")
   

def convertExceltoCsv(path,outdir):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    filesNames = readExcelFiles(path)
    output_path = os.path.join('outdir')
    completed = 0
    total = len(filesNames)
    for f in filesNames:
        percentageCompleted = (completed / total) * 100
        print(f"{percentageCompleted}% completed Converting {f} now ")
        rawFileNames = os.path.split(f)[1]
        withoutExtension = rawFileNames[0:rawFileNames.find(".xlsx")]
        output_fileName = re.sub(r"[\(\)\s+]", "_", withoutExtension) +  "_toExcel.csv"
        output_path = os.path.join(outdir, output_fileName)
        data = pd.read_excel(f, sheet_name=0)
        data.to_csv(output_path,index=False,header=None ,mode="w", encoding='utf-8')
        completed += 1
    print(f"Total of {completed } files has been converted to Excel")
    
    
def unique_email_w_matching_name(df):
    # email not null check
    is_null_email = pd.notnull(df.UserEmail)
    # null middlename
    IS_null_middleName = pd.isnull(df.UserMiddleName)
    # null firstname
    IS_null_FirstName = pd.isnull(df.UserFirstName)
    # null lastname
    IS_null_UserLastName = pd.isnull(df.UserLastName)
    # null company
    is_null_companyName = pd.isnull(df.CompanyName)

    email_check=  FileReport.email_format_check(df)
    name_check = np.logical_and(IS_null_FirstName,IS_null_UserLastName)
    companyName_and_Name_check = np.logical_and(name_check,is_null_companyName)
    
    # unique email least contain first and last or companyName
    at_least_one = np.logical_and(email_check, ~companyName_and_Name_check)
    
    return df[at_least_one]

def contain_all_three(df):
    # contain first name last name and company anem
    not_null_middleName = pd.notnull(df.UserMiddleName)
    not_null_FirstName = pd.notnull(df.UserFirstName)
    not_null_UserLastName = pd.notnull(df.UserLastName)
    not_null_companyName = pd.notnull(df.CompanyName)
    
    valid_Email = FileReport.email_format_check(df)
    
    name_check = np.logical_and(not_null_UserLastName,not_null_FirstName)
    company_name_check = np.logical_and(not_null_companyName, name_check)
    condition = np.logical_and(valid_Email, company_name_check)
    
    return df[condition]





