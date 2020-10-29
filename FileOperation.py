import pandas as pd
from MatchTable import MatchTable
## Match Table Variable
# ismatchedForeignColumns
# notMatchedForeignColumn

class FileOperation():
    def __init__(self,filename, schema_df):
        self.filename = filename
        self._schema_df = schema_df
        self._df = pd.read_csv(filename, encoding = 'latin')
        self.matchTable = MatchTable(self._schema_df,self._df)
        
    def get(self):
        pass
    
    def show(self):
        pass
    
    
   


    


    
    
        
    
    
    
    