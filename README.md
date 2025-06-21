# Radiology Report Grading 
Example code to develop a pipeline for grading radiology reports

## Requirements
* Python 3.X
* `pandas`
* `numpy`
* `jupyter notebook`

This repository contains an example of how to structure a pipeline to grade radiology reports using specified guidelines. Functions are split into 3 main scripts:
1. `projectTableFunctions.py` functions to load and manipulate SQL tables for grading.
2. `reliabilityLib.py` functions to allow grader's to interact with reliability reports
3. `reportMarkingFunctions.py` functions to add reports to a queue, present reports and save assigned grades, and print helpful status messages

In addition, there are two json files that allow for customization of the grading process:
1. `phrases_to_highlight.json` contains phrases that should be highlighted in the reports to help graders focus on important information.
2. `sql_tables.json` contains the names of the SQL tables used in the grading process, allowing for easy modification of the table names. These tables include:
    - `procedure_table` : Contains all of the procedure IDs for the radiology reports
        - necessary columns = [X, X, X, X, X]
    - `patient_table` : Contains patient IDs and any desired demographic informations
        - necessary columns = [X, X, X, X, X]
    - `grader_table`: Contains the assigned and completed grades from all graders.
        - necessary columns = [X, X, X, X, X]
    - `project_table` : Contains the procedure IDs and the assigned project for each ID in long format. Procedure IDs may be duplicated.
        - necessary columns = [X, X, X]
    - `source_table` : Contains the radiology narrative
        - necessary columns = [X, X, "narrative_text"]
    - `impression_table` : Contains the radiology impression (if distinct from the narrative). Can be blank
        - necessary columns = ["proc_ord_id", X, "impression_text"]
    - `skipped_reports_table` : Contains the skipped/flagged reports for later review
        - necessary columns = [, X, "impression_text"]

Graders will interact with the grading pipeline through a set of three jupyter notebooks.
1. `1_training.ipynb` : The training notebook that will guide the grader through how to evaluate reports
2. `2_report_grading.ipynb` : This is the primary notebook and how users will grade reports and add additional reports to their queue.
3. `3_grader_evaluation.ipynb` : Contains functions to evaluate the inter-rater reliability of graders and examine flagged reports.

## Guidelines
Grading guidelines used by graders to assign a grade to a radiology report. 

## License
![alt text](logo.png) 

This work is licensed under a [Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License](https://creativecommons.org/licenses/by-nc-nd/4.0/)

### CHANGES REMOVE AFTER REVIEW
* a% = narrative
* report_annotation_master =
* arcus.procedure_order_impression = impression
* arcus.patient = patient_info
* arcus.procedure_order = procedures
* arcus.encounter = visits
* arcus_2023_05_02.reports_annotations_master = reports_master
* lab.training_selfeval = training_selfeval
* arcus_2023_04_05.narrative = 2023_04_05.narrative