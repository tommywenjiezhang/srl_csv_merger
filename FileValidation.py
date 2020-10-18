import glob
import pandas as pd
import os
import re
import qgrid as qd
import numpy as np
import difflib





## Match Table Variable
# ismatchedForeignColumns
# notMatchedForeignColumn

class FileValidation:
    def __init__(self,filename, schema):
        self.filename = filename
        self._schema_df = schema
        self._df = pd.read_csv(filename, encoding = 'latin')
        self.matchTable = self._matchedTable()
        self.drop_columns = self._drop_columns()
        self.drop_columns_report_writer = pd.ExcelWriter('debug/drop_colums_report.xlsx', engine='xlsxwriter')
        
    def _columRenameMapper(self):
        isMatched = self.matchTable['ismatchedForeignColumns']
        IsExisted = pd.notnull(self.matchTable['fuzzyMatch'])
        citeria = np.logical_and(isMatched,IsExisted)
        tableColumnsName = self.matchTable['foreignColumns'].where(citeria).dropna()
        schemaColumnsName= self.matchTable['fuzzyMatch'].where(self.matchTable['ismatchedForeignColumns']).dropna()
        keyName = np.array(tableColumnsName)
        valueName = np.array(schemaColumnsName)
        return dict(zip(keyName, valueName))
    
    def _drop_columns(self):
        drop_columns = np.array(self.matchTable['notMatchedForeignColumn'].dropna())
        return drop_columns
    
    def write_drop_columns_report(self):
        new_df = pd.DataFrame()
        new_df['filename'] = self.filename
        
        new_df.to_excel(self.drop_columns_report_writer,self.filename[4:10])
        self.drop_columns_report_writer.save()
        
    def _matchedTable(self):
        # Create a new  table dataframe to store the matches
        new_df = pd.DataFrame() 
        # Create a foreign column in table schema
        foreignColumns = pd.Series(self._df.columns)
        
        # First save column name from schema  and foreign columns
        new_df['schemaColumnsName'] = pd.Series(self._schema_df['ColumnName'])
        new_df['foreignColumns'] =  foreignColumns
        
        # Fuzzy Match Alorgthrim 
        def is_in_matched(foreignColumsName,schemaColumns):
            w = str(foreignColumsName).lower()
            matchedArry = difflib.get_close_matches(w, schemaColumns.ColumnName.astype(str), n=50, cutoff=.6)
            if len(matchedArry) > 0:
                return matchedArry[0]
            else:
                return None
        
        # creating fuzzyMatch column exact Match
        new_df['fuzzyMatch'] = new_df['foreignColumns'].apply(lambda x: is_in_matched(x,self._schema_df))
        new_df['fuzzyMatch'].drop_duplicates()
        isfuzzyMatched = pd.notnull(new_df['fuzzyMatch'])
        exactMatched = foreignColumns.isin(self._schema_df['ColumnName'])
        
        # check if the foreign columns name 
        new_df['ismatchedForeignColumns'] = np.logical_or(isfuzzyMatched, exactMatched)
        

        # filter out the not match columns later needs to drop
        new_df['notMatchedForeignColumn'] = pd.Series(new_df['foreignColumns']).where(np.logical_not(new_df['ismatchedForeignColumns']))
        # filter out match column
        new_df['ismatchedForeignColumns'].fillna(False)
        
        strOutPut = pd.Series(new_df['foreignColumns']).where(new_df['ismatchedForeignColumns'])
        new_df['MatchedForeignColumn'] = pd.Series(strOutPut)
        
        new_df['filename'] = self.filename
        return new_df


def readDirCsv(path):
    all_files = glob.glob(path + "/*.csv")
    return all_files

if __name__ == '__main__':
    path = r'raw'
    schema_df = pd.read_csv('schema/schema2.csv')
    concatDifferentframes = [FileValidation(filename,schema_df)  for filename in readDirCsv(path)]
    
    for x in concatDifferentframes:
        x.write_drop_columns_report()
        
    
    
    
    