# Radiology Report Grading 
Example code to develop a pipeline for grading radiology reports

## Requirements
* Python 3.X
* `pandas`
* `numpy`
* `jupyter notebook`

This repository contains an example of how to structure a pipeline to grade radiology reports using specified guidelines. Functions are split into 3 main scripts:
1. `annotationHelperLib.py` functions to aid in the marking of reports 
2. `reliabilityLib.py` functions to allow grader's to interact with reliability reports
3. `reportMarkingFunctions.py` functions to add reports to a queue, present reports and save assigned grades, and print helpful status messages

## Guidelines
Grading guidelines used by graders to assign a grade to a radiology report. 

## License
![alt text](logo.png) 

This work is licensed under a [Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License](https://creativecommons.org/licenses/by-nc-nd/4.0/)

### CHANGES REMOVE AFTER REVIEW
* arcus.procedure_order_narrative = narrative
* report_annotation_master =
* arcus.procedure_order_impression = impression
* arcus.patient = patient_info
* arcus.procedure_order = procedures
* arcus.encounter = visits
* arcus_2023_05_02.reports_annotations_master = reports_master
* lab.training_selfeval = training_selfeval
* arcus_2023_04_05.narrative = 2023_04_05.narrative