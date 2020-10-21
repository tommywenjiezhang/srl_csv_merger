import pandas as pd
import re
import os
class FileReport():
    @staticmethod
    def MatchTableReport(filename, df):
        csvIndex = filename.find(".csv")
        filename = re.sub(r".csv", "",filename)
        filename = re.sub(r"[_]", " ", filename)
        filename =  filename[4:csvIndex]
        if os.path.exists("debug/MatchTableReport.xlsx"):   
            with pd.ExcelWriter("debug/MatchTableReport.xlsx", engine="openpyxl", mode="a") as writer:
                df.to_excel(writer, sheet_name=filename)
        else:
            with pd.ExcelWriter("debug/MatchTableReport.xlsx", engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name=filename)
            
                
       
        
    
        
        