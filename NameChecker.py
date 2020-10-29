import nltk
from nameparser.parser import HumanName
import pandas as pd
import numpy as np
import spacy
import multiprocessing
import time
import csv






try:
    # Pyarrow version > 0.14
    from pyarrow.plasma import PlasmaStoreFull as _PlasmaStoreFull
except ImportError:
    # Pyarrow version <= 0.14
    from pyarrow.lib import PlasmaStoreFull as _PlasmaStoreFull

from pandarallel import pandarallel
pandarallel.initialize()

class NameCheck():
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        
    def get_human_names(self,text):
        if text is None:
            with open(r'debug/final_merge_debug_out.csv', 'a') as f:
                writer = csv.writer(f)
                writer.writerow([None, None, None,None])
            return (None , None, None, None)
        text = str(text)
        doc = self.nlp(text)
        firstName  = None
        LastName = None
        MiddleName = None
        categories = None
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                x = ent.text
                firstName = HumanName(x).first
                lastName = HumanName(x).last
                middleName = HumanName(x).middle
                print(firstName, lastName, middleName)
                with open(r'debug/final_merge_debug_out.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow(list((firstName, lastName, middleName)))
                return (firstName, lastName, middleName, ent.label_)
            else:
                with open(r'debug/final_merge_debug_out.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow([None, None, None])
                return (None,None, None, ent.label_)






if __name__ == "__main__":
    fields=['firstName','lastName','MiddleName']
    
    with open(r'debug/final_merge_debug_out.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(fields)

    Company_df = pd.read_csv('./clean/submission/second/ValidCompanyName_Or_Name_Final_Merge (1).csv')
    companyName_not_null = Company_df['CompanyName'].values
    t1_start = time.perf_counter() 
    
    nc = NameCheck()
    Company_df['fullname']= Company_df['CompanyName'].parallel_apply(nc.get_human_names)
    Company_df[['firstName','lastName','MiddleName','categories']] =  pd.DataFrame(Company_df['fullname'].tolist(),index=Company_df.index)
    # convertStr  = [str(x) for x in companyName_not_null.to_list()]
    df = pd.DataFrame(Company_df['fullname'].tolist(), columns=['firstName', 'lastName' , 'MiddleName', 'categories'])
    # rawpersonText = (";".join(convertStr))
    # personList = get_human_names(rawpersonText)
    # print(personList)
    Company_df.to_csv(r"debug/name_test.csv")
    
    
    t1_stop = time.perf_counter() 
    print("Elapsed time:", t1_stop, t1_start)
    print("Elapsed time during the whole program in seconds:", t1_stop-t1_start) 
    