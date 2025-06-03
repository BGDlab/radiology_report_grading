import pandas as pd
from annotationHelperLib import *
from sklearn.metrics import cohen_kappa_score
from IPython.display import clear_output
from google.cloud import bigquery 
import os
import json

with open(f"{os.path.dirname(__file__)}/sql_tables.json", 'r', encoding='utf-8') as f:
    sql_tables = json.load(f)
    

def get_reliability_proc_ord_ids():
    """
    Get the proc_ord_id values for the reliability reports

    Args:

    Return:
        proc_ord_ids (list): List of the 151 proc_ord_ids for the reliability reports
    """
   
    tmp = pd.read_csv("reliability_report_info.csv")
    proc_ord_ids = tmp["proc_ord_id"]
    return proc_ord_ids


def get_reliability_ratings_df():
    """
    Load reliability ratings dataframe

    Return:
        df_ratings (dataframe): Dataframe with 151 reliability report ratings
    """
    
    global sql_tables
    # Initialize the client service
    client = bigquery.Client()

    reliability_ratings_query = "select * from " + sql_tables["grader_table"] + " where grade_category = 'Reliability';"
    df_reliability = client.query(reliability_ratings_query).to_dataframe()
    df_reliability[["grade", "proc_ord_id"]] = df_reliability[
        ["grade", "proc_ord_id"]
    ].astype("int64")
    print(df_reliability.shape)

    # Pivot
    df_ratings = pd.pivot_table(
        df_reliability, values="grade", index="proc_ord_id", columns="grader_name"
    )

    df_ratings.reset_index(inplace=True)
    print(df_ratings.shape)

    query = """
    with cte as (
      select
        distinct(proc_ord_id) as proc_ord_id,
        grader_name
      from
        """ + sql_tables["grader_table"] + """
      where
        grade_category = 'Reliability'
    )
    select
      main.proc_ord_id
    from
      """ + sql_tables["grader_table"] + """ main
      inner join cte on main.proc_ord_id = cte.proc_ord_id
    """

    df_reliability = client.query(query).to_dataframe()
    reliability_ids = sorted(list(df_reliability["proc_ord_id"].values))[:-1]

    df_ratings = df_ratings[df_ratings["proc_ord_id"].isin(reliability_ids)]

    return df_ratings


def identify_disagreement_reports(df_grades1, df_grades2):
    """
    Identifies reports with different grades for 2 graders

    Args:
        df_grades1 (dataframe): A dataframe object with columns proc_ord_id and grade for user 1
        df_grades2 (dataframe): A dataframe object with columns proc_ord_id and grade for user 2
    
    Return:
        disagreement_proc_ord_ids (list): proc_ord_id strings specifying reports with 2 different grades
    """
    
    # Double check that the incoming dataframes are sorted the same way
    df_grades1 = df_grades1.sort_values("proc_ord_id", ignore_index=True)
    df_grades2 = df_grades2.sort_values("proc_ord_id", ignore_index=True)

    # Triple check the order
    assert list(df_grades1["proc_ord_id"].values) == list(
        df_grades2["proc_ord_id"].values
    )

    shared = 0
    disagreement_proc_ord_ids = []

    for idx, row in df_grades1.iterrows():
        proc_ord_id = row["proc_ord_id"]
        grades = [df_grades1.iloc[idx]["grade"], df_grades2.iloc[idx]["grade"]]
        if 999.0 in grades:
            continue
        else:
            shared += 1
            if grades[0] != grades[1]:
                disagreement_proc_ord_ids.append(proc_ord_id)

    return disagreement_proc_ord_ids


def calc_kappa(user1_grades, user2_grades):
    """
    For a pair of users with their grades in separate dataframes, compare them and calculate kappa

    Args:
        user1_grades (dataframe): proc_ord_id and corresponding grades for grader 1 reliability reports
        user2_grades (dataframe): proc_ord_id and corresponding grades for grader 2 reliability reports
    
    Return:
        kappa (float): Cohen's kappa between grader 1 and 2
    """
    
    # Double check that the incoming dataframes are sorted the same way
    user1_grades = user1_grades.sort_values("proc_ord_id")
    user2_grades = user2_grades.sort_values("proc_ord_id")

    # Triple check the order
    assert list(user1_grades["proc_ord_id"].values) == list(
        user2_grades["proc_ord_id"].values
    )

    # Calculate Cohen's kappa
    kappa = cohen_kappa_score(
        list(user1_grades["grade"].values), list(user2_grades["grade"].values)
    )
    # print("Cohen's kappa:", kappa)
    return kappa


def calc_kappa_2_v_all(user1_grades, user2_grades):
    """
    For a pair of users with their grades in separate dataframes, 
    compare the reports they graded as 2 versus reports the other
    graded as 0 and 1 and calculate kappa

    Args:
        user1_grades (dataframe): proc_ord_id and corresponding grades for grader 1 reliability reports
        user2_grades (dataframe): proc_ord_id and corresponding grades for grader 2 reliability reports
    
    Return:
        kappa (float): Cohen's kappa between grader 1 and 2
    """
    
    grades1 = user1_grades["grade"].values.astype(int)
    grades2 = user2_grades["grade"].values.astype(int)

    condenser = lambda x: 2 if x == 2 else 0

    grades1 = [condenser(i) for i in grades1]
    grades2 = [condenser(i) for i in grades2]

    kappa = cohen_kappa_score(grades1, grades2)

    print(" 2 vs. 0+1 kappa:", kappa)
    return kappa


def calc_kappa_0_v_all(user1_grades, user2_grades):
    """
    For a pair of users with their grades in separate dataframes, 
    compare the reports they graded as 0 versus reports the other
    graded as 1 and 2 and calculate kappa

    Args:
        user1_grades (dataframe): proc_ord_id and corresponding grades for grader 1 reliability reports
        user2_grades (dataframe): proc_ord_id and corresponding grades for grader 2 reliability reports
    
    Return:
        kappa (float): Cohen's kappa between grader 1 and 2
    """

    grades1 = user1_grades["grade"].values.astype(int)
    grades2 = user2_grades["grade"].values.astype(int)

    condenser = lambda x: 0 if x == 0 else 2

    grades1 = [condenser(i) for i in grades1]
    grades2 = [condenser(i) for i in grades2]

    kappa = cohen_kappa_score(grades1, grades2)

    print(" 0 vs. 1+2 kappa:", kappa)
    return kappa


def get_reports_for_user(user, proc_ord_ids, project = "reliability"):
    """
   Queue reliability reports for a user to grade

    Args:
        user (string): Name of the user/grader
        proc_ord_ids (list): proc_ord_id(s) for reports that user will grade based on the project
        project (string): Project the user will grade reports for
    
    Return:
        user_reliablity_reports (list): proc_ord_id of reliability reports to grade
    """
     
    global sql_tables
    client = bigquery.Client()

    get_user_reports = '''select cast(proc_ord_id as int64) as proc_ord_id, 
    grade, grade_category, grade_date
    from ''' + sql_tables["grader_table"] + '''
    where grader_name = "''' + user
    
    if project == "reliability":
        get_user_reports += '''"
        and grade_category = "Reliability";'''
    else:
        get_user_reports += '''"
        and grade_category = "Unique";'''
        
    user_reliablity_reports = client.query(get_user_reports).to_dataframe()
    user_reliablity_reports = user_reliablity_reports[
        user_reliablity_reports["proc_ord_id"].astype(int).isin(proc_ord_ids)
    ]

    return user_reliablity_reports


def print_report_from_proc_ord_id(proc_ord_id):
    """
    Print a specified radiology report

    Args:
        proc_ord_id (int): proc_ord_id of a report to print
    """
    
    global sql_tables
    client = bigquery.Client()

    print("Proc ord id:", proc_ord_id)
    print()

    # Get the report for that proc_ord_id from the primary report table
    get_report_row = (
        'SELECT * FROM reports_master where proc_ord_id like "'
        + str(proc_ord_id)
        + '"'
    )
    df_report = client.query(get_report_row).to_dataframe()

    # If the id was in the original table:
    if len(df_report) == 1:
        # Combine the narrative and impression text
        report_text = df_report["narrative_text"].values[0]
        if df_report["impression_text"].values[0] != "nan":
            report_text += "\n\nIMPRESSION: " + df_report["impression_text"].values[0]

    elif len(df_report) == 0:
        get_report_row = (
            'SELECT * FROM ' + sql_tables["source_table"] + ' where proc_ord_id like "'
            + str(proc_ord_id)
            + '"'
        )
        report_text = (
            client.query(get_report_row).to_dataframe()["narrative_text"].values[0]
        )

        get_report_row = (
            'SELECT * FROM ' + sql_tables["impression_table"] + ' where proc_ord_id like "'
            + str(proc_ord_id)
            + '"'
        )
        df_report = client.query(get_report_row).to_dataframe()

        if len(df_report) == 1:
            report_text += "\n\nIMPRESSION: " + df_report["impression_text"].values[0]

    print(report_text)


def print_disagreement_reports(disagreement_ids, grades1, grades2):
    """
    Print reports where 2 graders disagree

    Args:
        disagreement_ids (list): List of proc_ord_id where graders disagreed
        grades1 (dataframe): Grades for grader 1 
        grades2 (dataframe): Graders for grader 2

    """
    
    for proc_ord_id in disagreement_ids:
        report_grade1 = grades1[grades1["proc_ord_id"].astype(str) == str(proc_ord_id)][
            "grade"
        ].values[0]
        report_grade2 = grades2[grades2["proc_ord_id"].astype(str) == str(proc_ord_id)][
            "grade"
        ].values[0]

        print_report_from_proc_ord_id(proc_ord_id)
        print(
            "\nThis report was given the grades of",
            report_grade1,
            "by user 1 and",
            report_grade2,
            "by user 2",
        )

        confirm_continue = str(input("Press enter to continue"))
        clear_output()


def calculate_metric_for_graders(graders, metric, project = "reliability"):
    """
    Calculate Cohen's Kappa between graders

    Args:
        graders (list): Names of graders to compare grades
        metric (string): Metric to evaluate. Options: disagreement, kappa, 
                         kappa2vAll, kappa0VAll
        project (string): Project to evaluate grades for
    
    Return:
        metric_table (dataframe): Dataframe with specified metric between listed graders
    """
    
    global sql_tables
    if project == "reliability":
        proc_ord_ids = get_reliability_proc_ord_ids()
    else:
        client = bigquery.Client()
        q_query = f'''
        SELECT DISTINCT proc_ord_id
        FROM {sql_tables["project_table"]}
        WHERE project = "{project}"'''
        proc_ord_ids = client.query(q_query).to_dataframe().proc_ord_id.astype(int)
    metric_table = pd.DataFrame(np.nan, columns=graders[1:], index=graders[:-1])

    for idx1 in range(len(graders) - 1):
        # Get the grades for grader 1
        grades1 = get_reports_for_user(graders[idx1], proc_ord_ids, project = project)
        grades1 = grades1.sort_values("proc_ord_id", ignore_index=True)
        grades1 = grades1.loc[np.logical_and(grades1.grade >= 0, grades1.grade <= 2),:]

        for idx2 in range(idx1 + 1, len(graders)):
            # Get the grades for grader 2
            grades2 = get_reports_for_user(graders[idx2], proc_ord_ids, project = project)
            grades2 = grades2.sort_values("proc_ord_id", ignore_index=True)
            grades2 = grades2.loc[np.logical_and(grades2.grade >= 0, grades2.grade <= 2),:]
            
            # Select only intersecting proc_ord_ids
            intersect_ids = set(grades1.proc_ord_id).intersection(set(grades2.proc_ord_id))
            print(f"{len(intersect_ids)} common grades between {graders[idx1]} and {graders[idx2]}")
            grades1_subset = grades1.loc[grades1.proc_ord_id.isin(intersect_ids),:]
            grades2_subset = grades2.loc[grades2.proc_ord_id.isin(intersect_ids),:]
            
            if not ~grades1_subset.proc_ord_id.equals(grades2_subset.proc_ord_id):
                print("Warning: proc_ord_ids from graders are not identical")
            
            if metric == "disagreement":
                tmp = identify_disagreement_reports(grades1_subset, grades2_subset)
                metric_table.loc[graders[idx1], graders[idx2]] = len(tmp)

            elif metric == "kappa":
                k = calc_kappa(grades1_subset, grades2_subset)
                metric_table.loc[graders[idx1], graders[idx2]] = k
                

            elif metric == "kappa2vAll":
                k = calc_kappa_2_v_all(grades1_subset, grades2_subset)
                metric_table.loc[graders[idx1], graders[idx2]] = k
            elif metric == "kappa0vAll":
                k = calc_kappa_0_v_all(grades1_subset, grades2_subset)
                metric_table.loc[graders[idx1], graders[idx2]] = k
            

    return metric_table
