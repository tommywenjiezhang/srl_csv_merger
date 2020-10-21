import glob
import pandas as pd
import os
import re
import qgrid as qd
import numpy as np
import difflib
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from MatchTable import MatchTable
from TableOperation import TableOperation


## Match Table Variable
# ismatchedForeignColumns
# notMatchedForeignColumn

class FileOperation:
    def __init__(self,filename, schema=pd.read_csv('schema/schema2.csv')):
        self.filename = filename
        self._schema_df = schema
        self._df = pd.read_csv(filename, encoding = 'latin')
        self.matchTable = MatchTable(self._schema_df,self._df)
    
    def getExtractedData(self):
        tb = TableOperation(self._df)
        matchedColumn =  self.matchTable.getMatchedColumn(MatchTable.MATCHEDCOLUMN)
        missing = self.matchTable.getMissingSchemaColumns()
        mapper = self.matchTable._columRenameMapper()
        combined = np.concatenate([matchedColumn,missing])
        columnsSchema = self._schema_df['ColumnName'].dropna()
        extracted =  tb.renameColumns(mapper).appendEmptyColumn(missing).extractColumns(columnsSchema)
        extracted["filename"] = self.filename
        self._df = extracted
        return extracted
    
    def getColumnsStats(self,desiredColumns):
        mt = self.matchTable
        inputColumns = np.array(desiredColumns)
        matchedColumn = mt.getMatchedColumn(MatchTable.MATCHEDCOLUMN)
        columnsSelected = np.intersect1d(matchedColumn, inputColumns)
        rename_mapper = self.matchTable._columRenameMapper()
        tb = TableOperation(self._df)
        extracted = tb.renameColumns(rename_mapper).extractColumns(columnsSelected)
        
        
        def count_nulls(series):
            return len(series) - series.count()
        def count_unique(series):
            return series.nunique()
        def mismatch_email(series):
            return series[np.logical_not(series.str.match(r"[^@]+@[^@]+\.[^@]+"))].count()
        
        
        df_selected = self._df[[c for c in extracted ]]
        df_selected['filename'] = self.filename
        null_values = np.full((len(extracted)),count_nulls)
        unique_values = np.full((len(extracted)),count_unique)
        mapper =  {x:list(y) for x,y in  zip(extracted, zip(null_values,unique_values))}
    
        if 'UserEmail' in mapper:
            mapper['UserEmail'].append(mismatch_email)
            
        pivotTable = df_selected.groupby('filename').agg({**mapper})
        return pivotTable
    
    def print_columns(self,*columns, **name):
        output = pd.DataFrame()
        for i in range(0,len(columns)):
            colName = name['name']
            output[f'{colName[i]}'] = pd.Series(columns[i]).sort_values(ascending=True)
        output.to_csv(rf"debug/{self.filename[4:9]}.csv")
   


    


    
    
        
    
    
    
    