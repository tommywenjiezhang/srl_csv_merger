import os
import pandas as pd
from itertools import cycle
import re

def fileSplit(f):
    splitFile  = pd.read_csv(f)
    splitting_list = splitFile.index[splitFile.CompanyName == 'CompanyName'].tolist()
    list_cycle = cycle(splitting_list)
    next_element = next(list_cycle)
    filename = os.path.basename(f)
    withoutExtension = filename[0:filename.find(".csv")]
    output_fileName = re.sub(r"[\(\)\s+]", "_", withoutExtension)
    output_fileName  = os.path.join('./newSplit',output_fileName)
    if not os.path.exists('newSplit'):
        os.mkdir('newSplit')
    for elem in splitting_list:
        if splitting_list[0] == splitting_list[-1]:
            print("no splitting for this " + f)
            break;
        previous = elem
        next_element = next(list_cycle)
        print(f"Row: {previous+2} to Row: {next_element+2}")
        newFileName = output_fileName  + str(previous) + '_' + str(next_element) + '.csv'
        current_df = splitFile.iloc[previous:next_element]
        current_df.columns = splitFile.iloc[previous].tolist()
        current_df = current_df.drop([previous])
        current_df .to_csv(newFileName)
        splitting_list = splitting_list[1:]
        if next_element == splitting_list[len(splitting_list)-1]:
            newFileName = output_fileName  + str(next_element) + '_' + str(splitFile.shape[0]) + '.csv'
            splitFile.iloc[next_element:].to_csv(newFileName)
            splitFile.columns = splitFile.iloc[next_element].tolist()
            break;
    
if __name__ == "__main__":
    f = fileSplit('../../newRaw/Merge2_toExcel.csv')
