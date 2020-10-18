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

class FileOperation:
    def __init__(self,filename, schema=pd.read_csv('schema/schema2.csv')):
        self.filename = filename
        self._schema_df = schema
        self._df = pd.read_csv(filename, encoding = 'latin')
        self.matchTable = self._matchedTable()
        self.drop_columns = self._drop_columns()
        
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
    
    def getExtractedDataBase(self):
        # rename the mat df column
        
        total_column = self.align()
        
        d = self._columRenameMapper()
        # dict_Df = pd.DataFrame.from_dict(d, orient='index').reset_index()
        # dict_Df.to_csv(r'debug/dictcsv.csv')
        extract_array = np.intersect1d(np.sort(self.matchTable['schemaColumnsName'].to_numpy()), total_column)
        extract_df = self._df[[x for x in extract_array]]
        print(f"Shape is {extract_df.shape[1]} ==== Columns {extract_df.shape[0]} ==== Rows ++++++")
        
        # index for the extraction
        extract_df = extract_df.loc[:,~extract_df.columns.duplicated()]
        row_count = pd.Index(np.arange(extract_df.shape[0]))
        filename = pd.Index(np.full(shape=extract_df.shape[0],fill_value=self.filename,dtype=np.str))
        
        # set index as filename and rowcount
        extract_df =  extract_df.set_index(filename, row_count)
        extract_df['filename'] = self.filename
        message = f"{self.filename} has columns of{np.array2string(extract_df.columns, separator=' ')}"
        print(self.filename)
        print(message)
        with open("debug/test.txt", "a+") as myfile:
            myfile.write(message)
        return extract_df
    
    def write_extracted_output(self,writer):
        output = self.getExtractedDataBase()
        sheetName = re.sub(r'\.csv','',self.filename[4:])
        sheetName = re.sub(r'[_]',' ', sheetName)
        output.to_excel(writer,sheetName)
    
    def align(self):
        matchedCiteria = np.logical_and(self.matchTable['ismatchedForeignColumns'],pd.notnull(self.matchTable['fuzzyMatch']))
        # matched value
        matched = self.matchTable['fuzzyMatch'].where(matchedCiteria).to_numpy()
        matched = matched[pd.notnull(matched)]
        # find the difference between schema and matched
        # schemaArray create from schmema
        schemaArray = np.array(self._schema_df['ColumnName'].dropna())
        difference = np.setdiff1d(schemaArray[1:], matched)
        # find the missing match from schema column
        total_columns = np.concatenate([difference,matched])
        # sort column unique
        sorted_total_columns = np.sort(np.unique(total_columns))
        self.print_columns(total_columns,difference, matched, name=['total_columns', 'difference', 'matched'])
        # append not matching empty columns
        self._df = pd.concat([self._df,pd.DataFrame(columns=[x for x in difference])],ignore_index=True)
        
        self._df= self._df.rename(columns=self._columRenameMapper())
        
        return sorted_total_columns
        
    def write_drop_columns_report(self,writer):
        new_df = pd.Series(self.drop_columns, name="Column_tobe_droped")
        new_df.to_excel(writer,self.filename[4:10])
        
    def print_columns(self,*columns, **name):
        output = pd.DataFrame()
        for i in range(0,len(columns)):
            colName = name['name']
            output[f'{colName[i]}'] = pd.Series(columns[i]).sort_values(ascending=True)
        output.to_csv(rf"debug/{self.filename[4:9]}.csv")
        
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



    


    
    
        
    
    
    
    