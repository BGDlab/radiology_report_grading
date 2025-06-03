from reportMarkingFunctions import *
from reliabilityLib import *
from google.cloud import bigquery # SQL table interface on Arcus

def removeTestUser(name):
    client = bigquery.Client()
    
    q = "delete from lab.training_selfeval where grader_name = '"+name+"';"
    updateJob = client.query(q)
    updateJob.result()
    print(name, "has been removed from lab.training_selfeval")

    q = "delete from lab.grader_table_with_metadata where grader_name = '"+name+"';"
    updateJob = client.query(q)
    updateJob.result()
    print(name, "has been removed from lab.grader_table_with_metadata")

    
def autofillSelfEval(name):
    client = bigquery.Client()
 
    # Automatically give all the reliability reports grades of 1
    q = "update lab.training_selfeval set grade = 1 where grader_name = '"+name+"';"
    updateJob = client.query(q)
    updateJob.result()
    print("User's self-evaluation reports have been given grades of 1.")
    

def autofillAllReports(name):
    client = bigquery.Client()
    
    # Check that name has the reliability reports in the table
    q = "select * from lab.grader_table_with_metadata where grader_name = '"+name+"' and grade_category = 'Reliability';"
    df = client.query(q).to_dataframe()
    
    if len(df) > 0:
        print("User has reliability reports in main table")
    else:
        welcomeUser(name)
        
    # Automatically give all the reliability reports grades of 1
    q = "update lab.grader_table_with_metadata set grade = 1 where grader_name = '"+name+"' and grade_category = 'Reliability';"
    updateJob = client.query(q)
    updateJob.result()
    print("User's reliability reports have been given grades of 1.")
    

def autofillNReports(name, numReports):
    client = bigquery.Client()
    
    # Check that name has the reliability reports in the table
    q = "select * from lab.grader_table_with_metadata where grader_name = '"+name+"' and grade_category = 'Reliability';"
    df = client.query(q).to_dataframe()
    
    if len(df) > 0:
        print("User has reliability reports in main table")
    else:
        welcomeUser(name)
        q = "select * from lab.grader_table_with_metadata where grader_name = '"+name+"' and grade_category = 'Reliability';"
        df = client.query(q).to_dataframe()
        
    # Automatically give N reports a grade of 1
    procIds = df['proc_ord_id'].values[:numReports]
    for procId in procIds:
        q = "update lab.grader_table_with_metadata set grade = 1 where grader_name = '"+name+"' and grade_category = 'Reliability'"
        q += " and proc_ord_id = '"+str(procId)+"';"
        updateJob = client.query(q)
        updateJob.result()
    print(numReports, " have been given grades of 1.")

    
def createTestUser(name):
    # Create the test user
    isOnMainTable = welcomeUser(name)
    
