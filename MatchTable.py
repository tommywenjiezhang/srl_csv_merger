from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import numpy as np


class MatchTable():
    ISMATCHEDCOLUMN = "matchedColumn"
    ORIGINALCOLUMN = "foreignColumn"
    MATCHEDCOLUMN = "MatchedColumnName"
    def __init__(self,schema_df,raw_df):
        self.schema_df = schema_df
        self.raw_df = raw_df
        self.matchtable = self.matchColumns()
    
    def matchColumns(self):
        columnsNames = self.raw_df.columns
        matchedArry = []
        for column in columnsNames:
            matchedArry.append(process.extractOne(column, self.schema_df.ColumnName.astype(str)))
        filter_high_score =  list(filter(lambda x : x[1] >= 90,matchedArry))
        df = pd.DataFrame.from_records(matchedArry, columns =['MatchedColumnName', 'MatchScore', 'index'])
        df['foreignColumn'] = pd.Series(self.raw_df.columns)
        df['matchedColumn'] = df['MatchScore'].apply(lambda x : x >= 90)
        df['matchedColumn'] = df['matchedColumn'].fillna(False)
        df.to_csv(r"debug/matchTable_ver4.csv")
        return df
    
    def getMatchedColumn(self, columnName):
        matchedCiteria =  np.logical_and(self.matchtable['matchedColumn'], pd.notnull(self.matchtable['matchedColumn']))
        matchedColumn = pd.Series(self.matchtable[columnName].where(matchedCiteria)).dropna()
        return matchedColumn.to_numpy()
     
    def getMissingSchemaColumns(self):
        matchedCiteria =  np.logical_and(self.matchtable['matchedColumn'], pd.notnull(self.matchtable['matchedColumn']))
        MatchedColumn = np.array(pd.Series(self.matchtable['MatchedColumnName'].where(matchedCiteria)).dropna())
        schemaArray = np.array(self.schema_df['ColumnName'].dropna())
        difference = np.setdiff1d(schemaArray[1:], MatchedColumn )
        return difference
    
    def getColumnsNotSelectedOrigin(self):
         matchedCiteria =  np.logical_not(self.matchtable['matchedColumn'])
         combinedCiteria = np.logical_and(matchedCiteria, pd.notnull(self.matchtable['matchedColumn']))
         matchedColumn = pd.Series(self.matchtable['foreignColumn'].where(combinedCiteria)).dropna()
         return matchedColumn
         
    def _columRenameMapper(self):
        isMatched = self.matchtable['matchedColumn']
        IsExisted = pd.notnull(self.matchtable['MatchedColumnName'])
        citeria = np.logical_and(isMatched,IsExisted)
        tableColumnsName = self.matchtable['foreignColumn'].where(citeria).dropna()
        self.matchtable.loc[self.matchtable['MatchedColumnName'].duplicated(), 'MatchedColumnName'] = np.NaN
        schemaColumnsName= self.matchtable['MatchedColumnName'].where(citeria).dropna()
        keyName = np.array(tableColumnsName)
        valueName = np.array(schemaColumnsName)
        return dict(zip(keyName, valueName))