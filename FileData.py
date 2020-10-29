import pandas as pd
import re
import os
import pathlib
from MatchTable import MatchTable
from TableOperation import  TableOperation 
import numpy as np
from FileOperation import FileOperation 


class FileData(FileOperation):
    def __init__(self,filename, schema=pd.read_csv('schema/schema2.csv')):
        super(FileData,self).__init__(filename,schema)
    
    def getExtractedData(self):
        tb = TableOperation(self._df)
        matchedColumn =  self.matchTable.getMatchedColumn(MatchTable.MATCHEDCOLUMN)
        missing = self.matchTable.getMissingSchemaColumns()
        mapper = self.matchTable._columRenameMapper()
        combined = np.concatenate([matchedColumn,missing])
        columnsSchema = self._schema_df['ColumnName'].dropna()

        print(f"Mapper: {mapper}")
        extracted =  tb.renameColumns(mapper).appendEmptyColumn(missing).extractColumns(columnsSchema.values)
        extracted["filename"] = self.filename
        self.writeColumnReport(self.filename, mapper)
        tb.print_columns(extracted.columns,matchedColumn, combined, missing, name=["actual" ,"matchColumn", "combin" , "missing"])
        self._df = extracted
        return extracted
    
    def get(self):
        return self.getExtractedData()
        
    def writeColumnReport(self,text,d):
        if not os.path.exists(r"debug/columnsReport.txt"):
            f = open(r"debug/columnsReport.txt", "w+")
        with open(r"debug/columnsReport.txt", "a") as f:
            f.write(text)
            CSV = ""
            for k,v in d.items():
                line = "{},{}\n".format(k, ",".join(v))
                CSV+=line
            f.write(CSV)
            
if __name__ == "__main__":
    f = FileData('../newRaw/dental_prospective3.csv')
    d = f.getExtractedData()
    print(d['UserEmail'])
    