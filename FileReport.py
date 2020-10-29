import pandas as pd
from FileOperation import FileOperation
import re
import os
import pathlib
from MatchTable import MatchTable
from TableOperation import  TableOperation 
import numpy as np



from pandarallel import pandarallel
pandarallel.initialize()

class FileReport(FileOperation):
    def __init__(self,filename,schema=pd.read_csv('schema/schema2.csv')):
        super(FileReport,self).__init__(filename,schema)
        self.desiredColumns =  ["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" ,"HealthCareProfessionalInformation_specialty1"]
        
        
        
    def getColumnsStats(self):
        mt = self.matchTable
        inputColumns = np.array(self.desiredColumns)
        matchedColumn = mt.getMatchedColumn(MatchTable.MATCHEDCOLUMN)
        matchedColumn = matchedColumn[pd.notnull(matchedColumn)]
        columnsSelected = np.intersect1d(matchedColumn, inputColumns)
        if len(columnsSelected) <= 0:
            columnsSelected =  np.unique(columnsSelected)
        else:
            columnsSelected = list(filter(lambda x : not x.isnumeric(),np.unique(columnsSelected)))
        rename_mapper = self.matchTable._columRenameMapper()
        tb = TableOperation(self._df)
        extracted = tb.renameColumns(rename_mapper).extractColumns(columnsSelected)
        
        def count_nulls(series):
            return len(series) - series.count()
        def count_unique(series):
            return series.nunique()
        def mismatch_email(series):
            return series[np.logical_not(series.str.match(r"[^@]+@[^@]+\.[^@]+"))].count()
        
        
        extracted['filename'] = self.filename
        null_values = np.full(extracted.shape[1],count_nulls)
        unique_values = np.full(extracted.shape[1],count_unique)
        mapper =  {x:list(y) for x,y in  zip(extracted, zip(null_values,unique_values))}
        
        if len(columnsSelected) <= 0 or extracted.empty:
            return None
    
        if 'UserEmail' in mapper:
            mapper['UserEmail'].append(mismatch_email)
            
        pivotTable = extracted.groupby('filename').agg([count_nulls,count_unique])
        return pivotTable
    
    
    def getExtractdf(self):
        mt = self.matchTable
        inputColumns = np.array(self.desiredColumns)
        matchedColumn = mt.getMatchedColumn(MatchTable.MATCHEDCOLUMN)
        matchedColumn = matchedColumn[pd.notnull(matchedColumn)]
        columnsSelected = np.intersect1d(matchedColumn, inputColumns)
        if len(columnsSelected) <= 0:
            columnsSelected =  np.unique(columnsSelected)
        else:
            columnsSelected = list(filter(lambda x : not x.isnumeric(),np.unique(columnsSelected)))
        
        print(f"Column Selected {columnsSelected}")
        rename_mapper = self.matchTable._columRenameMapper()
        tb = TableOperation(self._df)
        extracted = tb.renameColumns(rename_mapper).extractColumns(columnsSelected)

        return extracted
    
    @staticmethod 
    def count_nulls(series):
        return len(series) - series.count()
    @staticmethod
    def count_unique(series):
        return series.nunique()
    @staticmethod
    def mismatch_email(series):
        return series[np.logical_not(series.str.match(r"[^@]+@[^@]+\.[^@]+"))].count()
    @staticmethod
    def get_top(series):
        return series.head(1)
    def show(self):
        print(self.getColumnsStats())
        
    def get(self):
        return self.getColumnsStats()
    
    def columnsListReport(self):
        columns = self._df.columns
        new_df = pd.DataFrame(columns,columns=['ColumnName'])
        new_df['filename'] = self.filename
        return new_df
    
    @staticmethod
    def MatchTableReport(filename, df):
        csvIndex = filename.find(".csv")
        filename = re.sub(r".csv", "",filename[4:])
        filename = re.sub(r"[_]", " ", filename)
        filename =  filename[4:csvIndex]
        if os.path.exists("debug/MatchTableReport.xlsx"):   
            with pd.ExcelWriter("debug/MatchTableReport.xlsx", engine="openpyxl", mode="a") as writer:
                df.to_excel(writer, sheet_name=filename)
        else:
            os.mkdir("./debug")
            with pd.ExcelWriter("debug/MatchTableReport.xlsx", engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name=filename)
    
    @staticmethod
    def getEmailStatsReport(df):
        groupy_by_email = df.groupby('filename').parallel_apply(FileReport.emailGroupBy)
        return groupy_by_email
        
             
    @staticmethod
    def emailGroupBy(df):
        # total email count
        total_email_count = df.UserEmail.count()
        # empty email count
        null_email_count = df.UserEmail.isnull().sum()
        # unique email in the merge
        unique_email_count = df.UserEmail.nunique()
        # mismatch email count    
        mismatchEmail_Count = df[~df.UserEmail.str.match(r"[^@]+@[^@]+\.[^@]+", na=False)].UserEmail.count()
        # labels
        stats_Labels = ['total_email_count',
                    'unique_email_count',
                    'empty_email_count',
                    'mismatched_email_count',
                    'email matched with at least one field (CompanyName, FirstName, or LastName)',
                    "unique email with companyName and name (CompanyName First, Last)",
                    "email address with missing names",
                   ]
        unique_email_least_one = FileReport.unique_email_least_one(df)
        contain_all_three = FileReport.contain_all_three(df)
        missing_name = FileReport.missingName(df)
        fields = [
             total_email_count,
             unique_email_count,
             null_email_count,
             mismatchEmail_Count,
             unique_email_least_one,
             contain_all_three,
             missing_name
        ]
        df_email_stats = pd.Series(fields, index=stats_Labels)
        return df_email_stats
        
        
    @staticmethod
    def missingName(df):
        # check if valid email address with missing name
        null_middleName = pd.isnull(df.UserMiddleName)
        null_FirstName = pd.isnull(df.UserFirstName)
        null_UserLastName = pd.isnull(df.UserLastName)
        valid_Email = FileReport.email_format_check(df)
        name_check = np.logical_and(null_UserLastName,null_FirstName)
        condition = np.logical_and(valid_Email, name_check)
        
        return df[condition].UserEmail.nunique()
    @staticmethod
    def unique_email_least_one(df):
        # email not null check
        
        # null middlename
        IS_null_middleName = pd.isnull(df.UserMiddleName)
        # null firstname
        IS_null_FirstName = pd.isnull(df.UserFirstName)
        # null lastname
        IS_null_UserLastName = pd.isnull(df.UserLastName)
        # null company
        is_null_companyName = pd.isnull(df.CompanyName)
        
        email_check = FileReport.email_format_check(df)
        
        name_check = np.logical_and(IS_null_FirstName,IS_null_UserLastName)
        
        companyName_and_Name_check = np.logical_and(name_check,is_null_companyName)
        
        # unique email least contain first and last or companyName
        at_least_one = np.logical_and(email_check, ~companyName_and_Name_check)
        total_count  = df[at_least_one].UserEmail.nunique()
        return total_count
    
    @staticmethod
    def email_format_check(df):
        NOT_NULL_EMAIL = pd.notnull(df.UserEmail)
        matchEmail_check = np.where(df.UserEmail.str.match(r"[^@]+@[^@]+\.[^@]+", na=False),True, False)
        return np.logical_and(NOT_NULL_EMAIL,matchEmail_check)
    
    
    @staticmethod
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
        total_count = df[condition].UserEmail.nunique()
        return total_count
    
    
if __name__ == "__main__":
    final_merge = pd.read_csv("./finalMerge/final_merge_ver5.csv")
    report = FileReport.getEmailStatsReport(final_merge)
    s = report.stack(0)
    s.to_csv('./reports/merge_email_stats_report.csv')
    
    
            
                
       
        
    
        
        