import glob
import pandas as pd
import os
import re
import qgrid as qd
import numpy as np

class TableOperation():
    def __init__(self, df):
        self._df = df
    def renameColumns(self,renameSchema):
        self._df.rename(columns=renameSchema, inplace=True)
        return self
    
    def appendEmptyColumn(self, *columns):
        for column in columns:
            self._df[column] = np.nan
        return self
    
    def moveColumnsToFront(self, columns):
        self._df = self._df[[y for y in columns]+[x for x in self._df if x not in columns ]]
        return self._df
    
    def extractColumns(self, columns):
        if len(columns) <=0:
            pass
        columnsToArray = np.array(columns)
        columnsToArray = np.sort(columnsToArray)
        difference = np.setdiff1d(columnsToArray, self._df.columns)
        for d in difference:
            if d not in self._df:
                self._df[[d for d in difference]] = np.nan
            else:
                np.append(columnsToArray,d)
        
        extracted_df = self._df[[x for x in columnsToArray]]
        extracted_df = extracted_df.loc[:,~extracted_df.columns.duplicated()]
        self._df = extracted_df
        return extracted_df
    
    def print_columns(self,*columns, **name):
        output = pd.DataFrame()
        for i in range(0,len(columns)):
            colName = name['name']
            output[f'{colName[i]}'] = pd.Series(columns[i]).sort_values(ascending=True)
        return output