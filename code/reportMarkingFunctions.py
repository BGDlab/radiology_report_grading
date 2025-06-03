import pandas as pd
import numpy as np
import os
import random
import json
from annotationHelperLib import *
from dxFilterLibraryPreGrading import *
from IPython.display import clear_output
from google.cloud import bigquery  # SQL table interface
from datetime import date
from projectTableFunctions import *

num_validation_graders = 2

with open(f"{os.path.dirname(__file__)}/sql_tables.json", 'r', encoding='utf-8') as f:
    sql_tables = json.load(f)


def backup_grader_table():
    """
    Back up grader table.
    """
    
    global sql_tables
    client = bigquery.Client()
    
    # Step 1: save the grader_table_with_metadata to a .csv
    tmp_dir = os.path.expanduser("~.backups/")
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    tmp_fn = f'{sql_tables["grader_table"].replace(".", "_")}_{np.datetime64("today")}.csv'
    tmp_csv = os.path.join(tmp_dir, tmp_fn)
    get_table_query = "select * from " + sql_tables["grader_table"]
    grader_table = client.query(get_table_query).to_dataframe()
    grader_table.to_csv(tmp_csv, index=False)

    # Step 2: drop table bak_grader_table_with_metadata
    grader_table_name_bak = "bak_" + sql_tables["grader_table"]
    q_drop_table = "drop table "+grader_table_name_bak
    try:
        job = client.query(q_drop_table)
        job.result()
    except: 
        pass

    # Step 3: create table bak_grader_table_with_metadata
    q_create_backup_table = "create table "+grader_table_name_bak+" as select * from " + sql_tables["grader_table"]
    job = client.query(q_create_backup_table)
    job.result()
    print(sql_tables["grader_table"], " backup successful")


def regrade_skipped_reports(client, project_name="", grader="", flag=-1):
    """
    Regrade skipped reports

    Args:
        client (string): A bigquery client object
        project_name (string): Name of a project to evaluate
        grader (string): Name of user/grader (leave blank to review all flagged reports)
        flag (int): The level of "skip" to examine (-1 is group, -2 is clinician)

    """
    
    # Handle different projects: 
    # If the project is not specified, assume we're looking only at SLIP grading
    global sql_tables
    # Get the flagged reports
    # Start the query
    q = "select distinct * from " + sql_tables["grader_table"]
    # If the user wants to incorporate the project
    if project_name != "":
        q += f'''
        reports join {sql_tables["project_table"]} project
            on (reports.proc_ord_id = projects.proc_ord_id
            and reports.pat_id = projects.pat_id)
        where projects.project = "'+project_name+'" '''

    q += " where grade = " + str(flag)
    # If the grader i
    if grader != "":
        q += " and name = '" + grader + "'"

    q += ";"
    flagged_reports = client.query(q).to_dataframe() #LOH

    if flagged_reports.shape[0] == 0:
        print("There are currently no reports with the grade of", flag)
        return

    # Shuffle the flagged reports
    flagged_reports = flagged_reports.sample(frac=1)

    # Get the narrative and impression for all reports
    proc_ord_ids = flagged_reports.proc_ord_id.astype(str).unique()
    proc_ord_id_str = '","'.join(proc_ord_ids)
    
    # Get the narrative and impression for all reports
    q_get_report_rows = f'''
        SELECT 
          COALESCE(narr.proc_ord_id, impr.proc_ord_id) as proc_ord_id,
          narr.narrative_text,
          impr.impression_text
        FROM narrative narr
        FULL OUTER JOIN impression impr
          ON (narr.proc_ord_id = impr.proc_ord_id)
        WHERE narr.proc_ord_id IN ("{proc_ord_id_str}")
        OR impr.proc_ord_id IN ("{proc_ord_id_str}");'''
    report_df = client.query(q_get_report_rows).to_dataframe()
    if df.proc_ord_id[~df.proc_ord_id.isin(report_df.proc_ord_id)].shape[0] == 0:
        report_df.loc[~report_df.impression_text.isna(),"impression_text"] = "\n\nIMPRESSION: " + report_df.impression_text
        report_df.loc[report_df.impression_text.isna(),"impression_text"] = ""
        report_df["report_text"] = report_df.narrative_text + report_df.impression_text.astype(str)
    else:
        # Don't exit the function if proc_ord_ids are missing. Flag them for technical review
        missing_proc_ord_ids = df.proc_ord_id[~df.proc_ord_id.isin(report_df.proc_ord_id)]
        missing_proc_str = '","'.join(missing_proc_ord_ids)
        print(f"Missing proc_ord_ids: {missing_proc_str}")
        return
    
    # for each flagged report
    count = 0
    for idx, row in flagged_reports.iterrows():
        clear_output()
        count += 1
        print(str(count) + "/" + str(len(flagged_reports)))
        print()
        # Add a print to show why the report was previously flagged
        # Check if the report is in the lab.skipped_reports table
        check_skipped_query = (
            "select * from "+ sql_tables["skipped_reports_table"] +" where proc_ord_id = '"
            + str(row["proc_ord_id"])
        )
        check_skipped_query += "' and name = '" + row["name"] + "';"
        skipped_df = client.query(check_skipped_query).to_dataframe()

        print("Grader:", row["name"])
        print("Grading criteria:", row["grade_criteria"])

        is_skip_logged = False
        if len(skipped_df) == 1:
            is_skip_logged = True

        if is_skip_logged:
            print("Reason report was flagged:", skipped_df["skip_reason"].values[0])
        else:
            print("Skipped reason not available.")

        # Print the report
        proc_ord_id = row["proc_ord_id"]

        # print("Projects:", row["project"]) # -- if useful, add back in for graders
        print("Year of scan:", row["proc_ord_year"])
        print("Age at scan:", np.round(row["age_in_days"] / 365.25, 2), "years")
        proc_ord_id = row["proc_ord_id"]
        with open("code/phrases_to_highlight.json", "r") as f:
            to_highlight = json.load(f)
            
        print_report(report_df.report_text[report_df.proc_ord_id == proc_ord_id].values[0], to_highlight)  # -- LOH

        print()
        # ask for grade
        grade = get_grade(enable_md_flag=True)

        if not (grade == -1 or grade == -2 or grade == 503):
            regrade_reason = get_reason("regrade")

            # Update the grader table with the new grade
            q_update = "UPDATE "+ sql_tables["grader_table"] +" set grade = " + str(
                grade
            )
            q_update += ' WHERE proc_ord_id = "' + str(proc_ord_id) + '"'
            q_update += ' and name = "' + row["name"] + '"'

            j_update = client.query(q_update)
            j_update.result()

            if is_skip_logged:
                # Update the skipped reports table
                q_update_skipped = "update "+ sql_tables["skipped_reports_table"] +" set grade = " + str(
                    grade
                )
                q_update_skipped += ', regrade_reason = "' + regrade_reason + '" '
                q_update_skipped += (
                    'where proc_ord_id = "' + str(proc_ord_id) + '" and '
                )
                q_update_skipped += 'name = "' + row["name"] + '";'

                j_update_skipped = client.query(q_update)
                j_update_skipped.result()
            else:
                # Add the report to the skipped reports table.
                # ('proc_ord_id', 'grade', 'name', 'skip_date', 'skip_reason', 'regrade_date', 'regrade_reason')
                q_skip_report = "insert into "+ sql_tables["skipped_reports_table"] +" values ("
                today = date.today().strftime("%Y-%m-%d")
                q_skip_report += (
                    "'"
                    + str(proc_ord_id)
                    + "',"
                    + str(grade)
                    + ", '"
                    + row["name"]
                    + "', '', '', '"
                    + today
                    + "', '"
                    + regrade_reason 
                    + "', '"
                    + grade_criteria
                    + "');"
                )

            print("New grade saved. Run the cell again to grade another report.")


def print_report_from_proc(proc_ord_id, client, to_highlight={}, source_table="narrative"):
    """
    Print a report for a user to evaluate and grade

    Args:
        proc_ord_id (int): proc_ord_id associated with a radiology report
       client (string): A bigquery client object
       to_highlight (dictionary): terms to highlight and the color(s) to highlight them as
       source_table (string): Name of a SQL table where radiology reports are stored
    """
    
    try:
        # Get the report for that proc_ord_id from the primary report table
        q_get_report_row = (
            "SELECT * FROM "
            + source_table
            + ' where proc_ord_id like "'
            + str(proc_ord_id)
            + '"'
        )
        df_report = client.query(q_get_report_row).to_dataframe()
    except:
        print(
            "AN ERROR HAS OCCURRED: REPORT", proc_ord_id, "CANNOT BE FOUND IN", source_table
        )

    # If the id was in the new table:
    origin_table = source_table

    q_get_report_row = (
        "SELECT * FROM "
        + '.narrative where proc_ord_id = "'
        + str(proc_ord_id)
        + '"'
    )
    if len(df_report) == 1:
        report_text = (
            client.query(q_get_report_row).to_dataframe()["narrative_text"].values[0]
        )
    else:
        report_text = ""

    q_get_report_row = (
        "SELECT * FROM "
        + '.impression where proc_ord_id = "'
        + str(proc_ord_id)
        + '"'
    )
    df_report = client.query(q_get_report_row).to_dataframe()

    if len(df_report) == 1:
        report_text += "\n\nIMPRESSION: " + df_report["impression_text"].values[0]
    elif len(df_report) == 0:
        print("proc_ord_id not in", source_table, ":", proc_ord_id)

    report_text = " ".join(report_text.split())
    report_text = report_text.replace("CLINICAL INDICATION", "\n\nCLINICAL INDICATION")
    report_text = report_text.replace("TECHNIQUE", "\n\nTECHNIQUE")
    report_text = report_text.replace("HISTORY", "\n\nHISTORY")
    report_text = report_text.replace("IMPRESSION", "\n\nIMPRESSION")
    report_text = report_text.replace("FINDINGS", "\n\nFINDINGS")
    report_text = report_text.replace("COMPARISON", "\n\nCOMPARISON")

    # If the user passed a dictionary of lists to highlight
    if len(to_highlight.keys()) > 0:
        for key in to_highlight.keys():
            report_text = mark_text_color(report_text, to_highlight[key], key)

    # Print the report and ask for a grade
    print(report_text)
    print()
    # Print the proc_ord_id
    print("Report id:", str(proc_ord_id))
    print()


def get_grade_counts_since(d):
    """
    Print a count of the number of reports graded by each grader since date d

    Args:
        d (string): representation of the date in YYYY-MM-DD format
    """
    
    client = bigquery.Client()
    global sql_tables

    # Query the table
    q = (
        'select * from '+ sql_tables["grader_table"] +' where grade_date != "0000-00-00" and cast(grade_date as date) >= cast("'
        + d
        + '" as date);'
    )
    df = client.query(q).to_dataframe()

    # Get the count of rows for each grader
    graders = list(set(df["name"].values))

    # Print the table header
    print("# Reports \t Grader Name")

    # Print the rows for each grader
    for grader in graders:
        print(len(df[df["name"] == grader]), "\t\t", grader)

    # Print a statement about who has not graded any reports
    print()
    print(
        "Any graders not in the displayed table have not graded any reports since before "
        + d
    )


def read_sample_reports(to_highlight={}):
    """
    Iteratively show the user example reports in a random order (training step 1)
    
    Args:
        to_highlight (dictionary): terms to highlight and the color(s) to highlight them as
    """
    
    # Initialize the client service
    client = bigquery.Client()

    # Get the example reports
    get_slip_examples = "SELECT * FROM training_examples;"

    df_slip = client.query(get_slip_examples).to_dataframe()

    slip_reports = [
        row["narrative_text"]
        + "\n\nIMPRESSION: "
        + str(row["impression_text"])
        + "\n\nReport given grade of "
        + str(row["grade"])
        for i, row in df_slip.iterrows()
    ]

    # Shuffle the list of all reports
    random.shuffle(slip_reports)

    # Iteratively print each report
    for report in slip_reports:
        # If the user passed a dictionary of lists to highlight
        if len(to_highlight.keys()) > 0:
            report_text = report
            report_text = report_text.replace("CLINICAL INDICATION", "\n\nCLINICAL INDICATION")
            report_text = report_text.replace("TECHNIQUE", "\n\nTECHNIQUE")
            report_text = report_text.replace("HISTORY", "\n\nHISTORY")
            report_text = report_text.replace("IMPRESSION", "\n\nIMPRESSION")
            report_text = report_text.replace("FINDINGS", "\n\nFINDINGS")
            report_text = report_text.replace("COMPARISON", "\n\nCOMPARISON")
            for key in to_highlight.keys():
                report_text = mark_text_color(report_text, to_highlight[key], key)

            # Print the report and ask for a grade
            print(report_text)
        else:
            print(report)

        print()

        confirm = str(
            input(
                "After you read the report and understand its grade, press ENTER to continue to the next report."
            )
        )
        clear_output()

    print(
        "You have finished reading the example reports. Rerun this cell to read them again or proceed to the next section."
    )


def add_self_eval_reports(name):
    """
    Add reports for the user to grade for the self-eval

    Args:
        name (string): Full name of the grader (to also be referenced in publications)
    """
    
    client = bigquery.Client()

    q_get_selfeval = "select distinct report_id from training_selfeval;"
    df_self_eval = client.query(q_get_selfeval).to_dataframe()
    report_ids = df_self_eval["report_id"].values

    q_insert_report = "INSERT into training_selfeval (report_id, grade, name, reason) VALUES"

    for report in report_ids:
        q_insert_report += " ('" + str(report) + "', 999, '" + name + "', ' '),"

    q_insert_report = q_insert_report[:-1] + ";"
    print(
        "Adding "
        + str(len(report_ids))
        + " self-evaluation reports for "
        + name
        + " to grade."
    )
    j_add_report = client.query(q_insert_report)
    j_add_report.result()


def mark_selfeval_report_sql(name, to_highlight={}):
    """
    Pull the report associated with a proc_ord_id for which the specified grader has a grade of 999, 
    and then grade the report.

    Args:
        name (string): Full name of the grader (to also be referenced in publications)
        to_highlight (dictionary): terms to highlight and the color(s) to highlight them as
    """
    
    # Initialize the client service
    client = bigquery.Client()

    # Get a row from the grader table for the specified rater that has not been graded yet
    q_get_single_row = (
        'SELECT * FROM training_selfeval WHERE name like "'
        + name
        + '" and grade = 999 LIMIT 1'
    )

    df = client.query(q_get_single_row).to_dataframe()

    if len(df) == 0:
        print(
            "There are currently no reports to grade for",
            name,
            " in the table. You have completed the self-evaluation.",
        )
        return

    # Get the report for that proc_ord_id from the primary report table
    q_get_report_row = (
        'SELECT * FROM reports_master where combo_id = "'
        + str(df["report_id"].values[0])
        + '"'
    )
    df_report = client.query(q_get_report_row).to_dataframe()
    print(df_report.shape)
    print(list(df_report))

    # Combine the narrative and impression text
    report_text = df_report["narrative_text"].values[0]
    if df_report["impression_text"].values[0] != "nan":
        report_text += " IMPRESSION:" + df_report["impression_text"].values[0]

    # If the user passed a dictionary of lists to highlight
    if len(to_highlight.keys()) > 0:
        for key in to_highlight.keys():
            report_text = mark_text_color(report_text, to_highlight[key], key)

    # Print the report and ask for a grade
    print_report(report_text)
    print()
    grade = str(
        input(
            "Assign a rating to this report (0 do not use/1 maybe use/2 definitely use): "
        )
    )
    while grade != "0" and grade != "1" and grade != "2":
        grade = str(
            input(
                "Invalid input. Assign a rating to this report (0 do not use/1 maybe use/2 definitely use): "
            )
        )
    print()

    # Update the grader table with the new grade
    q_update = "UPDATE training_selfeval set grade = " + str(grade)
    q_update += ' WHERE report_id like "' + str(df["report_id"].values[0]) + '"'
    q_update += ' and name like "' + name + '"'

    j_update = client.query(q_update)
    j_update.result()

    # Ask for a reason the report was given the grade it was
    reason = str(input("Why does this report get that grade? "))
    print()

    # Update the grader table with the new grade
    q_update = 'UPDATE training_selfeval set reason="' + reason + '"'
    q_update += ' WHERE report_id like "' + str(df["report_id"].values[0]) + '"'
    q_update += ' and name like "' + name + '"'

    j_update = client.query(q_update)
    j_update.result()

    # Print out the grade and reason others gave the report
    print()
    q_others = (
        'SELECT grade, reason from training_selfeval WHERE report_id like "'
        + str(df["report_id"].values[0])
    )
    q_others += '" and name not like "' + name + '"'

    df_truth = client.query(q_others).to_dataframe()
    print(
        "For reference, other graders have given this report the following grades for the specified reasons:"
    )
    print()
    for idx, row in df_truth.iterrows():
        if int(row["grade"]) != 999:
            print("Grade:", row["grade"], "For reason:", row["reason"])

    print()
    confirm_continue = str(input("Press enter to continue"))

    print("Grade saved. Run the cell again to grade another report.")


def mark_reports(name, project, n_grades = 10, to_highlight={}):
    """
    Pull the report associated with a proc_ord_id for which the specified grader has a grade of 999,
    and then grade the report.

    Args:
        name (string): Full name of the grader (to also be referenced in publications)
        project (string): Project to grade for
        n_grades (int): Number of reports to present
        to_highlight (dictionary): terms to highlight and the color(s) to highlight them as
        

    """
    
    # Initialize the client service
    client = bigquery.Client()
    global sql_tables

    # Get a row from the grader table for the specified rater that has not been graded yet
    # If there's any reliability reports, start there
    q_get_rows = f'''
    SELECT *
    FROM {sql_tables["grader_table"]} grader
    INNER JOIN 2023_04_05.narrative narr
        on narr.proc_ord_id = grader.proc_ord_id
    WHERE name = "{name}"
        and grade = 999
        and grade_category = "Reliability"
    LIMIT {n_grades}'''
    df = client.query(q_get_rows).to_dataframe()
    source_table = "2023_04_05.narrative"

    if len(df) == 0:
        # If no reports need Reliability, then get Unique reports
        q_get_rows = f'''
            SELECT * FROM {sql_tables["grader_table"]} reports
            WHERE name = "{name}"
                and grade = 999
                and grade_category = "Unique"
            ORDER BY reports.proc_ord_id
            LIMIT {n_grades};'''
        # print(q_get_rows)
        df = client.query(q_get_rows).to_dataframe()
        source_table = "narrative"
        
    if len(df) == 0:
        print(
            "There are currently no reports to grade for",
            name,
            " in the table. Please add more to continue.",
        )
        return
        
    proc_ord_ids = df.proc_ord_id.astype(str).unique()
    proc_ord_id_str = '","'.join(proc_ord_ids)

    # Get the projects for all reports
    q_get_report_rows = f'''
        SELECT *
        FROM {sql_tables["project_table"]}
        where proc_ord_id IN ("{proc_ord_id_str}");'''
    # print(q_get_report_rows)
    project_df = client.query(q_get_report_rows).to_dataframe()
    
    # Get the narrative and impression for all reports
    q_get_report_rows = f'''
        SELECT 
          COALESCE(narr.proc_ord_id, impr.proc_ord_id) as proc_ord_id,
          narr.narrative_text,
          impr.impression_text
        FROM narrative narr
        FULL OUTER JOIN impression impr
          ON (narr.proc_ord_id = impr.proc_ord_id)
        WHERE narr.proc_ord_id IN ("{proc_ord_id_str}")
        OR impr.proc_ord_id IN ("{proc_ord_id_str}");'''
    # print(q_get_report_rows)
    report_df = client.query(q_get_report_rows).to_dataframe()
    if df.proc_ord_id[~df.proc_ord_id.isin(report_df.proc_ord_id)].shape[0] == 0:
        report_df.loc[~report_df.impression_text.isna(),"impression_text"] = "\n\nIMPRESSION: " + report_df.impression_text
        report_df.loc[report_df.impression_text.isna(),"impression_text"] = ""
        report_df["report_text"] = report_df.narrative_text + report_df.impression_text.astype(str)
    else:
        # Don't exit the function if proc_ord_ids are missing. Flag them for technical review
        missing_proc_ord_ids = df.proc_ord_id[~df.proc_ord_id.isin(report_df.proc_ord_id)]
        missing_proc_str = '","'.join(missing_proc_ord_ids)
        print(f"Missing proc_ord_ids: {missing_proc_str}")
        q_update = f'''
        UPDATE {sql_tables["grader_table"]}
            set grade = 404,
            grade_date = "{date.today().strftime("%Y-%m-%d")}"
            WHERE proc_ord_id IN ("{missing_proc_str}")
                and name like "{name}"'''
        
        j_update = client.query(q_update)
        j_update.result()

    # print(report_df)
        
    for proc_ord_id in proc_ord_ids:
        print("Report ID: ", proc_ord_id)
        print("Year of scan:", df.loc[df.proc_ord_id == proc_ord_id, "proc_ord_year"].values[0])
        print("Age at scan:", np.round(df.loc[df.proc_ord_id == proc_ord_id, "age_in_days"].values[0] / 365.25, 2), "years")
        print("Grading Criteria:", df.loc[df.proc_ord_id == proc_ord_id, 'grade_criteria'].values[0])
        # Fixing the project name confusion
        # Query the project table
        proc_projects = project_df.project[project_df.proc_ord_id == proc_ord_id].values
        if df['grade_category'].values[0] == "Reliability":
            print("This is a Reliability report, not a unique report")
        elif project in proc_projects:
            print("Project:", project)
            print()
        else:
            print("WARNING: report "+str(proc_ord_id)+" does not appear to belong to the cohort for "+project)
            print("It does belong to the following cohorts: "+", ".join(list(proc_projects)))
            print()
        # print(report_df.report_text[report_df.proc_ord_id == proc_ord_id].values[0])
        print_report(report_df.report_text[report_df.proc_ord_id == proc_ord_id].values[0], to_highlight)  # -- LOH
        grade = get_grade(enable_md_flag=False)
    
        # write the case to handle the skipped reports 
        if grade == -1 or grade == 503:
            # Ask the user for a reason
            if grade == -1:
                skip_reason = get_reason("skip")
            elif grade == 503:
                skip_reason = "OUTSIDE SCAN"
            # Write a query to add the report to the skipped reports table.
            # ('proc_ord_id', 'grade', 'name', 'skip_date', 'skip_reason', 'regrade_date', 'regrade_reason', 'grade_criteria')
            q_skip_report = "insert into "+ sql_tables["skipped_reports_table"] +" values ("
            today = date.today().strftime("%Y-%m-%d")
            q_skip_report += (
                "'"
                + str(proc_ord_id)
                + "', "
                + str(grade)
                + ", '"
                + name
                + "', '"
                + str(today)
                + "', '"
                + skip_reason
                + "', '', '', '"
                + df['grade_criteria'].values[0]
                + "');"
            )
    
            # Execute the query
            # print(q_skip_report)
            j_skip_report = client.query(q_skip_report)
            j_skip_report.result()
    
        # Update the grader table with the new grade
        q_update = f'''
        UPDATE {sql_tables["grader_table"]}
            set grade = {str(grade)},
            grade_date = "{date.today().strftime("%Y-%m-%d")}"
            WHERE proc_ord_id = "{str(proc_ord_id)}"
                and name like "{name}"'''
        # print(q_update)
        j_update = client.query(q_update)
        j_update.result()
        print("Grade saved.")
        clear_output()
    print("Run the cell again to grade another report.")


def get_more_reports_to_grade(name, project="SLIP Adolescents", num_to_add=100):
    """
    Get more proc_ord_id for which no reports have been rated for the specified user to grade

    Args:
        name (string): Full name of the grader (to also be referenced in publications)
        project (string): Project to grade for
        num_to_add (int): Number of reports to add to queue
        to_highlight (dictionary): terms to highlight and the color(s) to highlight them as
    """
    if project == "SLIP":
        print(
            "SLIP is too broad of a cohort definition. Please modify your project to include the appropriate age group descriptor and then rerun this function."
        )
        return -1

    # Load project parameter file
    if os.path.exists(f"project_params/{project}.json"):
        param_file = f"project_params/{project}.json"
    else:
        print(f"Parameter file not found for project {project}. Loading default project parameters")
        param_file = f"project_params/Default.json"
        
    with open(param_file, "r") as f:
        project_params = json.load(f)

    # Extract sorting criteria from the parameters file
    order_params = project_params["sort"]
    if project_params["validation"] == "yes" and project_params["prioritize_validation"] == "yes":
        order_params = ['report_type = "validation" desc'] + order_params
    order_params_str = ",\n            ".join(order_params)

    
    # Global var declaration
    global num_validation_graders
    # If the project does not want validation reports, exclude them
    if project_params["validation"] == "no":
        num_validation = 0
    else:
        num_validation = num_validation_graders
    
    global sql_tables
    print("It is expected for this function to take several minutes to run. Your patience is appreciated.")

    # Initialize the client service
    client = bigquery.Client()

    # Get the grading criteria
    criteria = load_cohort_config(project, "grade_criteria")

    # Get the number of reports for a cohort
    get_project_report_stats(project)

    # Get both validation and new reports in a single table
    q_get_reports = f'''
    with joint_reports as (
        with CTE as (
          select
            count(proc_ord_id) as counter,
            proc_ord_id,
            avg(grade) as avg_grade
          from {sql_tables["grader_table"]}
          group by proc_ord_id
        )
        select
        distinct 
          CTE.counter,
          CTE.avg_grade,
          grader.proc_ord_id,
          grader.name,
          "validation" as report_type
        from {sql_tables["grader_table"]} grader
        join {sql_tables["project_table"]} projects on (
          grader.proc_ord_id = projects.proc_ord_id
          and grader.pat_id = projects.pat_id
        )
        join CTE on (
          grader.proc_ord_id = CTE.proc_ord_id
        )
        where
          projects.project = "{project}"
          and grader.name != "{name}"  
          and CTE.counter < {str(num_validation)}
          and grader.name not like "Coarse Text Search%"
          and grade_category = "Unique"
          and grade_criteria = "{criteria}"
          and avg_grade > 0 
          and avg_grade <= 2
        UNION ALL
        select
          0 as counter,
          null as avg_grade,
          projects.proc_ord_id,
          "" as name,
          "new" as report_type
        from {sql_tables["project_table"]} projects
        join procedures proc on 
          (proc.proc_ord_id = projects.proc_ord_id)
        left join visits enc on 
          (proc.encounter_id = enc.encounter_id)
        where
        project = "{project}"
          and projects.proc_ord_id not in (
          select
            grader.proc_ord_id
            from {sql_tables["grader_table"]} grader
            where grader.grade_criteria = "{criteria}"
          )
          and enc_type_name != "Reconciled Outside Data"
    )
    SELECT DISTINCT
    joint_reports.*,
    pat.birth_weight_kg,
    pat.gestational_age_num,
    proc.proc_ord_datetime,
    proc.proc_ord_age'''

    
    q_get_reports += f'''
    FROM joint_reports
    LEFT JOIN procedures proc ON (proc.proc_ord_id = joint_reports.proc_ord_id)
    LEFT JOIN patient_info pat ON (pat.pat_id = proc.pat_id)'''

        
    # Add birth weight, gestational_age, and proc_ord_datetime
    
    q_get_reports += f'''
    ORDER BY {order_params_str}
    LIMIT {str(num_to_add)};'''
        
    
    df_reports = client.query(q_get_reports).to_dataframe()
    to_add = list(set(df_reports['proc_ord_id'].values))
    if len(to_add) > 0:
        add_reports_for_grader(to_add, name, project)
   
    # Count reports
    print("Number of validation reports added:", sum(df_reports.report_type == "validation"))
    print("Number of new reports added:", sum(df_reports.report_type == "new"))

    # Check: how many reports were added for the user?
    if (len(to_add)) == 0:
        print(
            "There are no reports for this project that have yet to be either graded or validated."
        )
    else:
        get_user_unrated_count = (
            'SELECT * FROM '+ sql_tables["grader_table"] +' WHERE name like "'
            + name
            + '" and grade = 999'
        )

        df = client.query(get_user_unrated_count).to_dataframe()

        # Inform the user
        print(len(df), "reports are in the queue for grader", name)


def add_reports_for_grader(proc_ord_ids, name, project):
    """
    Add reports to grade to a users' queu

    Args:
        proc_ord_ids (list): List of proc_ord_ids associated with a report to grade
        name (string): Full name of the grader (to also be referenced in publications)
        project (string): Project to grade for
    """
    
    client = bigquery.Client()
    global sql_tables
    
    # Get the column names from the table
    q_get_cols = "select * from "+ sql_tables["grader_table"] +" limit 1;"
    df_get_cols = client.query(q_get_cols).to_dataframe()
    cols_str = " ("+", ".join(list(df_get_cols))+") "
    
    # Convert the list of proc_ord_ids to a SQL string
    procs_str = ' ("'+'", "'.join(proc_ord_ids)+'") '

    # Get the grading criteria field for the project
    criteria = load_cohort_config(project, "grade_criteria")
    
    # Set up the query
    q_insert = '''insert into '''+ sql_tables["grader_table"] +cols_str+'''
        select
          distinct 
          proc_ord.proc_ord_id, "'''+name+'''" as name,
          999 as grade,
          "Unique" as grade_category,
          proc_ord.pat_id,
          proc_ord.proc_ord_age as age_in_days,
          proc_ord.proc_ord_year,
          proc_ord.proc_ord_desc as proc_name,
          "procedure_order" as report_origin_table, '''
    if "project" in cols_str:
        q_insert += '"'+project+'" as project, '
    q_insert += '"0000-00-00" as grade_date, '
    q_insert += '"'+criteria+'''" as grade_criteria
        from
          procedures proc_ord
          join patient_info pat on proc_ord.pat_id = pat.pat_id
        where
          proc_ord.proc_ord_id in '''+procs_str+'''
        order by 
          proc_ord.proc_ord_year desc;'''
    # print(q_insert)
    j_insert = client.query(q_insert)
    j_insert.result()


def get_second_look_reports_to_grade(name, num_to_add=100):
    """
    Get more proc_ord_id for which no reports have been rated for the specified user to grade

    Args:
        name (string):  Full name of the grader (to also be referenced in publications)
        num_to_add (int): Number of reports to add to queue

    """
    
    # Global var declaration
    global num_validation_graders
    global sql_tables
    print(
        "It is expected for this function to take several minutes to run. Your patience is appreciated."
    )
    print("Looking at", sql_tables["grader_table"])

    # Initialize the client service
    client = bigquery.Client()

    # Get the proc_ord_ids from the grader table
    q_grade_table = f'''with CTE as (
          select
            count(proc_ord_id) as counter,
            proc_ord_id
          from {sql_tables["grader_table"]}
          group by proc_ord_id
        )
        select
        distinct 
          CTE.counter,
          grader.proc_ord_id,
          grader.name,
          grader.proc_ord_year
        from {sql_tables["grader_table"]} grader
          join {sql_tables["project_table"]} projects on (
            grader.proc_ord_id = projects.proc_ord_id
            and grader.pat_id = projects.pat_id
          )
          join CTE on (
            grader.proc_ord_id = CTE.proc_ord_id
          )
        where
          grader.name != "{name}"  
          and CTE.counter < {str(num_validation_graders)}
          and grader.name not like "Coarse Text Search%"
          and grade_category = "Unique"
        order by grader.proc_ord_year desc
        limit {str(num_to_add)};'''
    df_grade_table = client.query(q_grade_table).to_dataframe()
    
    to_validate_ids = df_grade_table["proc_ord_id"].values
    print(df_grade_table.shape)

    # Add validation reports - proc_ord_ids already in the table
    if len(to_validate_ids) > 0:
        add_reports_for_grader(to_validate_ids, name) 

    # New reports
    q_get_user_unrated_count = (
        'select * from '+ sql_tables["grader_table"] +' where grade = 999 and name like "%'
        + name
        + '%";'
    )
    df = client.query(q_get_user_unrated_count).to_dataframe()

    # Inform the user
    print(len(df), "reports are in the queue for grader", name)


def welcome_user(name):
    """
    Print welcome message to user, letting them know the status of their grading 

    Args:
        name (string):  Full name of the grader (to also be referenced in publications)
    
    Return:
        (string): type of reports waiting in the user's queue
    """
    
    print("Welcome,", name)
    global sql_tables

    client = bigquery.Client()

    # Possibly pull this bit into its own function - make it user proof
    q_check_self_eval = (
        'select * from training_selfeval where name like"' + name + '"'
    )
    df_self_eval = client.query(q_check_self_eval).to_dataframe()

    if len(df_self_eval) == 0:
        print(
            "It appears you have yet to do the self-evaluation. Please grade those reports before continuing."
        )
        add_self_eval_reports(name)
        return "self-eval"

    elif 999 in df_self_eval["grade"].values:
        print(
            "It appears you have started the self-evaluation but have not finished it. Please grade those reports before continuing."
        )
        return "self-eval"

    q_reliability = (
        'select * from '+ sql_tables["grader_table"] +' where grade_category = "Reliability" and name like"'
        + name
        + '"'
    )
    df_reliability = client.query(q_reliability).to_dataframe()
    # print(check_reliability_ratings(df_reliability))

    if not check_reliability_ratings(df_reliability):
        print("It appears you have yet to grade the reliability reports.")
        add_reliability_reports(name)
        return "reliability"
    elif 999 in df_reliability["grade"].values:
        reliability_count = len(df_reliability[df_reliability["grade"] == 999])
        print("You have", reliability_count, "reliability reports to grade.")
        return "reliability"
    else:
        q_get_queued_count = (
            'select * from '+ sql_tables["grader_table"] +' where name like "'
        )
        q_get_queued_count += name + '" and grade = 999'

        df_grader_unrated = client.query(q_get_queued_count).to_dataframe()

        if len(df_grader_unrated) == 0:
            print("You are caught up on your report ratings")
            # TODO add function here to get more reports for the user
        else:
            print(
                "You currently have",
                len(df_grader_unrated),
                "ungraded reports to work on.",
            )
        return "unique"


def add_reliability_reports(name):
    """
    Add reliability reports to a user's queue

    Args:
        name (string):  Full name of the grader (to also be referenced in publications)
    """
    
    client = bigquery.Client()
    global sql_tables

    # Get the grader table
    q_get_grader_table = (
        "SELECT * from "+ sql_tables["grader_table"] +" where name = '"
        + name
        + "' and grade_category = 'Reliability';"
    )
    df_grader = client.query(q_get_grader_table).to_dataframe()

    df_reliability = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    add_reports = False

    q_insert_report = "INSERT into "+ sql_tables["grader_table"] +" (proc_ord_id, name, grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, report_origin_table, grade_date, grade_criteria) VALUES"

    # print(df_grader['proc_ord_id'].values)

    for idx, row in df_reliability.iterrows():
        # print(row['proc_ord_id'])
        if str(row["proc_ord_id"]) not in df_grader["proc_ord_id"].values:
            # Add the report
            q_insert_report += f'''
            ("{str(int(row["proc_ord_id"]))}", "{name}", 999, "Reliability",
            "{row["pat_id"]}", {str(row["age_in_days"])}, {str(row["proc_ord_year"])},
            "{row["proc_name"]}", "{row["report_origin_table"]}", 
            "0000-00-00", "SLIP"),'''
            add_reports = True

    if add_reports:
        print("Adding reliability reports to grade")
        q_insert_report = q_insert_report[:-1] + ";"
        # print(q_insert_report)
        j_add_report = client.query(q_insert_report)
        j_add_report.result()


def check_reliability_ratings(df_grader):
    """
    Print the status of a user's reliability report grading progress

    Args:
        df_grader (dataframe): A user's report history/queue in a table

    """
    if len(df_grader) == 0:
        return False

    name = df_grader["name"].values[0]
    df_reliability = pd.read_csv("~/arcus/shared/reliability_report_info.csv")
    reliability_ids = df_reliability["proc_ord_id"].values
    df_grader_reliability = df_grader[df_grader["grade_category"] == "Reliability"]
    grader_ids = df_grader_reliability["proc_ord_id"].values
    num_reliability = len([i for i in reliability_ids if str(i) in grader_ids])
    num_graded_reliability = len(
        [
            i
            for i in reliability_ids
            if str(i) in grader_ids
            and max(
                df_grader_reliability[df_grader_reliability["proc_ord_id"] == str(i)][
                    "grade"
                ].values
            )
            != 999
        ]
    )
    # print(num_reliability)
    # print(len(reliability_ids))

    # assert num_reliability == len(reliability_ids)
    print(
        name,
        "has graded",
        num_graded_reliability,
        "of",
        num_reliability,
        "reliability reports",
    )

    if num_graded_reliability == num_reliability:
        return True
    elif num_graded_reliability < num_reliability:
        return False
    else:
        print(
            "Error (code surplus): Grader has graded more reliability reports than exist"
        )


def release_reports(name, reports_list):
    """
    For a specified list of reports, change their grades to 999 to put them back
    in a user's queue. ASSUMES THE USER HAS VERIFIED THE REPORTS TO RELEASE

    Args:
        name (string):  Full name of the grader (to also be referenced in publications)
        reports_list (list): proc_ord_id for reports to reset the grades for
    """
    
    # Initialize the client
    client = bigquery.Client()
    # Use the previously specified global vars
    global sql_tables

    # For each report
    for proc_ord_id in reports_list:
        # Update the grader table with the new grade
        q_update = "UPDATE "+ sql_tables["grader_table"] +" set grade = 999,"
        q_update += ' grade_date="0000-00-00"'
        q_update += ' WHERE proc_ord_id = "' + str(proc_ord_id) + '"'
        q_update += ' and name = "' + name + '"'

        j_update = client.query(q_update)
        j_update.result()

    print(len(reports_list), "were released back into the queue for", name)


def backup_reliability_grades(name):
    """
    Backup a user's recently entered grades

    Args:
        name (string):  Full name of the grader (to also be referenced in publications)
    """
    
    client = bigquery.Client()
    # Use currently set global vars
    global sql_tables

    q = "select * from "+ sql_tables["grader_table"] +" where name = '" + name
    q += "' and grade_category = 'Reliability'"
    df_primary = client.query(q).to_dataframe()

    for proc_ord_id in df_primary["proc_ord_id"].values:
        # If the proc id is not in the df for the user
        q = "select * from lab.reliability_grades_original where name = '" + name
        q += (
            "' and grade_category = 'Reliability' and proc_ord_id = "
            + str(proc_ord_id)
            + ";"
        )
        df_backup = client.query(q).to_dataframe()

        # if the query returned an empty dataframe
        if len(df_backup) == 0:
            # Then add the row to the table
            q_add = "insert into lab.reliability_grades_original (proc_ord_id, name, "
            q_add += (
                "grade, grade_category, pat_id, age_in_days, proc_ord_year, proc_name, "
            )
            q_add += (
                "report_origin_table, project) values ('"
                + str(proc_ord_id)
                + "', '"
                + df_primary["name"].values[0]
            )
            q_add += (
                "', "
                + str(df_primary["grade"].values[0])
                + ", 'Reliability', '"
                + str(df_primary["pat_id"].values[0])
            )
            q_add += (
                "', "
                + str(df_primary["age_in_days"].values[0])
                + ", "
                + str(df_primary["proc_ord_year"].values[0])
            )
            q_add += (
                ", '"
                + str(df_primary["proc_name"].values[0])
                + "', '"
                + str(df_primary["report_origin_table"].values[0])
            )
            q_add += (
                "', '"
                + str(df_primary["project"].values[0])
                + "', '"
                + str(df_primary["grade_date"].values[0])
                + "' ) ;"
            )

        elif len(df_backup) == 1:
            if df_backup["grade"].values == 999:
                q_update = (
                    "UPDATE lab.reliability_grades_original set grade = "
                    + df_primary["grade"].values[0]
                )
                q_update += (
                    ' WHERE proc_ord_id = "'
                    + str(df_primary["proc_ord_id"].values[0])
                    + '"'
                )
                q_update += (
                    ' and name = "'
                    + str(df_primary["name"].values[0])
                    + '"'
                )

                j_update = client.query(q_update)
                j_update.result()


def print_report(report_text, to_highlight={}):
    """
    Print a report with highlighted text

    Args:
        report_text (string): Text of a radiology report in a string
        to_highlight (dictionary): terms to highlight and the color(s) to highlight them as
    """

    report_text = " ".join(report_text.split())
    report_text = report_text.replace("CLINICAL INDICATION", "\n\nCLINICAL INDICATION")
    report_text = report_text.replace("TECHNIQUE", "\n\nTECHNIQUE")
    report_text = report_text.replace("HISTORY", "\n\nHISTORY")
    report_text = report_text.replace("IMPRESSION", "\n\nIMPRESSION")
    report_text = report_text.replace("FINDINGS", "\n\nFINDINGS")
    report_text = report_text.replace("COMPARISON", "\n\nCOMPARISON")

    # If the user passed a dictionary of lists to highlight
    if len(to_highlight.keys()) > 0:
        for key in to_highlight.keys():
            report_text = mark_text_color(report_text, to_highlight[key], key)

    # Print the report and ask for a grade
    print(report_text)
    print()
    print()


def get_reason(usage):
    """
    List a reason why a report was skipped or list a reason for it's regrade

    Args:
        usage (string): Type of reason to list. Options: skip or regrade
    
    Return:
        reason (string): Reason
    """
    
    if usage == "skip":
        message = "This report was skipped. Please include the part(s) of the report that were confusing:"
    elif usage == "regrade":
        message = "This report was previously skipped. Please include an explanation why it received its updated grade:"

    reason = str(input(message))
    while type(reason) != str and len(reason) <= 5:  # arbitrary minimum string length
        reason = str(input(message))

    return reason


def get_grade(enable_md_flag=False):
    """
    Confirm a user's entered grade 

    Args:
        enable_md_flag (boolean): True/false to allow -2 grades for clinican escalation
    """
    
    if enable_md_flag:
        potential_grades = ["0", "1", "2", "-1", "-2", "503"]
        grade = str(
            input(
                "Assign a rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip/-2 escalate to clinician/503 outside scan): "
            )
        )

    else:
        potential_grades = ["0", "1", "2", "-1", "503"]
        grade = str(
            input(
                "Assign a rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip/503 outside scan): "
            )
        )

    while grade not in potential_grades:
        if not enable_md_flag:
            if grade == "-2":
                print(
                    "Reports cannot be marked for clinician review without undergoing peer review first. Please flag using a grade of -1 instead."
                )
                message = "Please enter a grade value from the acceptable grade list (0/1/2/-1): "
                grade = str(input(message))
            else:
                grade = str(
                    input(
                        "Invalid input. Assign a rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip): "
                    )
                )
        else:
            grade = str(
                input(
                    "Invalid input. Assign a rating to this report (0 do not use/1 maybe use/2 definitely use/-1 skip/-2 escalate to MD): "
                )
            )

    print()

    # Ask the user to confirm the grade
    confirm_grade = "999"
    while confirm_grade != grade:
        while confirm_grade not in potential_grades:
            if not enable_md_flag and confirm_grade == "-2":
                print(
                    "Reports cannot be marked for clinician review without undergoing peer review first. Please flag using a grade of -1 instead."
                )
                message = "Please enter a grade value from the acceptable grade list (0/1/2/-1): "
            else:
                message = "Please confirm your grade by reentering it OR enter a revised value to change the grade: "
            confirm_grade = str(input(message))
        if confirm_grade != grade:
            if not enable_md_flag and confirm_grade == "-2":
                print(
                    "Reports cannot be marked for clinician review without undergoing peer review first. Please flag using a grade of -1 instead."
                )
                message = "Please enter a grade value from the acceptable grade list (0/1/2/-1): "
                confirm_grade = str(input(message))
            else:
                grade = confirm_grade
                confirm_grade = "999"

    if confirm_grade == "-1":
        print("This report is being marked as SKIPPED (-1) for you.")
        return -1
    elif confirm_grade == "-2":
        print(
            "WARNING: this report is being marked as SKIPPED for you AND is being escalated to a clinician for further review."
        )
        return -2
    elif confirm_grade == "503":
        print("This report is being SKIPPED and labeled as an outside scan.")
        return 503
    else:
        print("Saving your grade of", grade, "for this report.")
        return grade


def check_unique_grades(df, name):
    """
    Print statement on a user's grading progress

    Args:
        df (dataframe): A user's report grading queue
        name (string):  Full name of the grader (to also be referenced in publications)
    """
    
    # Unique
    df_unique_reports = df[df["grade_category"] == "Unique"]
    df_graded_unique_reports = df[
        (df["grade_category"] == "Unique") & (df["grade"] != 999)
    ]
    print(
        name,
        "has graded",
        df_graded_unique_reports.shape[0],
        "unique reports of",
        df_unique_reports.shape[0],
        "assigned where",
    )
    for grade in range(3):
        num_graded = df_graded_unique_reports[
            df_graded_unique_reports["grade"] == grade
        ].shape[0]
        print(num_graded, "have been given a grade of", grade)
    

def get_grader_status_report(name):
    """
    Check a user's grading status

    Args:
        name (string):  Full name of the grader (to also be referenced in publications)
    """
    
    client = bigquery.Client()
    # Declare global var, but automatically start with SLIP regardless
    global sql_tables

    query = "select * from "+sql_tables["grader_table"].replace("nonslip_", "")+" where "
    query += "name = '" + name + "' and grade_criteria = 'SLIP' ;"
    df = client.query(query).to_dataframe()
    # Case: user not in table
    if len(df) == 0:
        print("User is not grading SLIP reports yet.")
        return

    # Reliability ratings
    check_reliability_ratings(df)
    # SLIP grades
    print("")
    print("SLIP ---------")
    check_unique_grades(df, name)

    # See if the user is also grading nonslip reports
    query = "select * from "+ sql_tables["grader_table"] +" where "
    query += "name = '" + name
    df = client.query(query).to_dataframe()

    # Case: user not in table
    if len(df) > 0:
        print("")
        check_unique_grades(df, name)




# Main
if __name__ == "__main__":
    print("Radiology Report Annotation Helper Library v 0.2")
    print("Originally written and maintained by Jenna Young, PhD (@jmschabdach on Github)")
    print("Currently maintained by Ben Jung, PhD (@bencephalon on Github)")
