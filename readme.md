# Setup.sh for creating venv and installing dependency
./setup.sh

# This repo is to read patient resource FHIR data in json format, process via pyspark(batch process) and store into mysql database
Two table created in mysql: one for patient personal data other is for patient address data
Similar to two table we can create more tables as per resource type data being received and populate values with pipeline.
Driver Class: 
#execute_paient_data_pipeline.py

![Paient Table data](https://github.com/rainidhi09/FHIR-paient-data-extract/blob/master/images/Screenshot%202022-04-25%20at%2023.57.07.png "Optional title")

![Paient Address Table data](https://github.com/rainidhi09/FHIR-paient-data-extract/blob/master/images/Screenshot%202022-04-25%20at%2023.57.49.png "Optional title")

#Flow chart
![Possible data ingestion pattern](https://github.com/rainidhi09/FHIR-paient-data-extract/blob/master/images/Screenshot%202022-04-26%20at%2000.17.43.png "Possible data ingestion pattern")