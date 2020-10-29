from MatchTable import MatchTable
import pandas as pd
import pytest
import json

@pytest.fixture
def mt():
    raw_Array = ['USEREMAIL',	'First Name', 'USEREMAIL',  "City" , 'Last Name'	,'Unsubscribed', 'State', 'Country','Zip', 'Mobile']
    shemaArry = columsSelected=["CompanyName" , "UserEmail" , "UserFirstName", "UserLastName" , "Website", "WorkNumber", "Address" ,  "City" , "State" ,  "Zip" ,"HealthCareProfessionalInformation_specialty1" ]
    schema_df = pd.DataFrame(data=shemaArry, columns=['ColumnName'])
    raw_df = pd.DataFrame(columns=raw_Array)
    mt =  MatchTable(schema_df,raw_df)
    return mt

def test_getColumns_passed(mt):
    expect_result = ["UserEmail" ,"City" , "State" ,  "Zip"] 
    actual_result = list(filter(lambda x: pd.notnull(x), mt.getMatchedColumn(MatchTable.MATCHEDCOLUMN)))
    print(actual_result)
    assert all([a == b for a, b in zip(actual_result, expect_result)])

def test_mapper_passed(mt):
    a = {'USEREMAIL' : "UserEmail" , "City": "City" , 'State': "State", 'Zip': "Zip"}
    b  = mt. _columRenameMapper()
    expected = json.dumps(a, sort_keys=True)
    actual = json.dumps(b, sort_keys=True)
    print(actual)
    assert expected == actual
    
    
    
    
    
    