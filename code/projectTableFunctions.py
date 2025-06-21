import pandas as pd
import numpy as np
import random
import os
from IPython.display import clear_output
from google.cloud import bigquery # SQL table interface on Arcus
from dxFilterLibraryPreGrading import *
from reportMarkingFunctions import *
import json
import matplotlib.pyplot as plt
import pathlib
base_dir = os.path.dirname(pathlib.Path(__file__).parent.resolve())

with open(f"{os.path.dirname(__file__)}/sql_tables.json", 'r', encoding='utf-8') as f:
    sql_tables = json.load(f)


def load_project(project_name, name):
    if project_name != "AUTO":
        project_id = project_name
    else:
        try:
            with open(os.path.expanduser("~/arcus/shared/annotation-helper-tools/behind_the_scenes/auto_control.json"), 'r') as file:
                project_assign = json.load(file)
                project_id = project_assign[name]
        except KeyError:
            project_id = project_assign["Default"]
    print(f"Grading reports under project {project_id}")
    return(project_id)

def phrasesToHighlightFn(phrases_file = "code/phrases_to_highlight.json"):
    # Load the dictionary of phrases to highlight in certain colors 
    with open(phrases_file, 'r', encoding='utf-8') as f:
        toHighlight = json.load(f)
    return(toHighlight)

def load_cohort_config(project_id, field):
    fn = f"{base_dir}/queries/config.json"
    with open(fn, "r") as f:
        project_lookup = json.load(f)

    # Get the info for the specified project
    project_info = project_lookup[project_id]
    if field == "query":
        query_fn = project_info["query"]
        query_fn = f"{base_dir}/{query_fn}"
        q_dx_filter = ""
        if "dx_filter" in project_info:
            # Get the name of the dx filter file
            fn_dx_filter = project_info["dx_filter"]
            # Expand the tilda for each user
            fn_dx_filter_full = os.path.expanduser(fn_dx_filter)
            # Convert the contents of the dx filter file to a sql query
            q_dx_filter = convert_exclude_dx_csv_to_sql(fn_dx_filter_full)
    
        ## --- I think this was put into a function?
        # Open the specified query file
        with open(query_fn, "r") as f:
            q_project = f.read()
    
        # If there is a dx filter, incorporate it into the loaded query
        if q_dx_filter != "":
            q_tmp = q_dx_filter + q_project.split("where")[0]
            q_tmp += "left join exclude_table on proc_ord.pat_id = exclude_table.pat_id where exclude_table.pat_id is null and"
            q_tmp += q_project.split("where")[1]
            q_project = q_tmp
    
        return q_project

    elif field == "grade_criteria":
        return project_info['grade_criteria']


def get_project_report_stats(cohort):
    global sql_tables
    # Set up the client
    client = bigquery.Client()
    
    # Get the number of reports with the project label
    q_project_reports = 'select * from ' + sql_tables["project_table"] + ' where project = "'+cohort+'"'
    df_project_reports = client.query(q_project_reports).to_dataframe()

    # Load the config
    criteria = load_cohort_config(cohort, "grade_criteria")
    
    # Get the number of reports with the project label in the grader table
    q_graded_reports = '''
    select distinct reports.* 
    from ''' + sql_tables["grader_table"] + ''' reports 
    join ''' + sql_tables["project_table"] + ''' projects 
    on (reports.proc_ord_id = projects.proc_ord_id and reports.pat_id = projects.pat_id) 
    where projects.project = "''' + cohort + '''" 
    and grade_criteria = "''' + criteria + '''";'''
    df_graded_reports = client.query(q_graded_reports).to_dataframe()
    
    # Print info
    print("Project:", cohort)
    print("Total reports:", len(df_project_reports), "(note each report must be graded by 2 graders)")
    print("Graded 0:", len(df_graded_reports[df_graded_reports['grade'] == 0]))
    print("Graded 1:", len(df_graded_reports[df_graded_reports['grade'] == 1]))
    print("Graded 2:", len(df_graded_reports[df_graded_reports['grade'] == 2]))
    print("Queued:", len(df_graded_reports[df_graded_reports['grade'] == 999]))
    print("Skipped:", len(df_graded_reports[df_graded_reports['grade'] < 0]))

