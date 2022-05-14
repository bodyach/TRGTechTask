## Implementation of technical task

### Prerequsites
- Linux
- Installed JDK 1.8
- Installed scala 2.12.12
- Installed spark 3.1.2 with hadoop 3.2 support
- Dataset is loaded and unziped from https://data.police.uk/data/ 

### Steps to run task implementation
1. Before project run, you need to go to the root folder of that project and run next command to build the jar:
```console
mvn clean package
```
After that, you will see that there is created a directory with name target, that conains **TRGTechTask-1.0-SNAPSHOT-jar-with-dependencies.jar.** That jar file should be used to run all spark jobs and start web service.

2. FIrst, we need to create a final table and save it to the local filesystem. For this purpose ***org.example.jobs.CrimeTableCreation*** class is created to define spark job, To run it, you can use create_street_crime_table.sh scipt from scripts folder. If you want to execute it from project root, you can use next command:
```console
sh scripts/create_street_crime_table.sh <absolute_path_to_TRGTechTask-1.0-SNAPSHOT-jar-with-dependencies.jar> <input_data_path> <output_data_path>
```
Please note, that output folder should not exist to successfuly write data at the end. In the next section, output of the executed spark job will be called streetCrimes table/data.

3. After streetCrime data creation, we can read it either using spark, or using web service API that was implemented in scope of the task. Web service usage we will discuss in the next section. To read data using spark, ***org.example.jobs.ReadParquetInputData*** class is created and can be used to read amount of the data you want:
```console
sh scripts/read_street_crime_table.sh <absolute_path_to_TRGTechTask-1.0-SNAPSHOT-jar-with-dependencies.jar> <path_to_streetCrimes_data_path> <number_of_rows_to_show>
```
There is a possibility to specify custom sql for streetCrime table, if you want to transforn data. For example, if you want to filter out all null values in crimeId you need to run command like that:
```console
sh scripts/read_street_crime_table.sh <absolute_path_to_TRGTechTask-1.0-SNAPSHOT-jar-with-dependencies.jar> <path_to_crimeData_path> <number_of_rows_to_show> "SELECT * FROM streetCrimes WHERE crimeId IS NOT NULL"
```
4. There are two kpis spark job implemeted to show some insight from the data. Spark job for them defined in ***org.example.jobs.CrimeKPIs*** class. The job calculates predefine KPIs, store them as json to specified output path and show results to the user. CrimeKPIs class describe two transformation: one for the statistic for crimeType, another one for disctrict. In case you need to see KPIs for crimeType column, you can use `write_and_show_crime_types_kpis.sh` for that:
```console
sh scripts/write_and_show_crime_types_kpis.sh <absolute_path_to_TRGTechTask-1.0-SNAPSHOT-jar-with-dependencies.jar> <path_to_streetCrimes_data_path> <output_path_for_jsons>
```
In case you need to get KPIs for the disctircs, you can use `write_and_show_district_kpis.sh` script:
```console
sh scripts/write_and_show_district_kpis.sh <absolute_path_to_TRGTechTask-1.0-SNAPSHOT-jar-with-dependencies.jar> <path_to_streetCrimes_data_path> <output_path_for_jsons>
```

## Web service API
In scope of the task created web service, that expose next APIs to retrieve same data that we use with spark:

|  Endpoint | What returns  |
| ------------ | ------------ |
|  /?query="SQL query you want to run" | Return array of json object that was retrieved with specified SQL query  |
| /crimesList | Returns data from streetCrimes table with  a default limit 1000 |
| /crimesList?lowerIndex=1000000&upperIndex=1100000 | Return data from a specified range,  where lowerIndex represents a row from which start reading and upperIndex represents the last row to show. |
| /kpis/crimes | Shows KPIs information for crimeTypes as an array of JSON objects |
| /kpis/locations | Shows KPIs information for dosctrict as an array of JSON objects |

An implementation for that service you can check in org.example.service.CrimeStatsService. To run the the service, you can use `run_crime_stats_service.sh` scripts:
```console
sh scripts/run_crime_stats_service.sh <absolute_path_to_TRGTechTask-1.0-SNAPSHOT-jar-with-dependencies.jar> <path_to_streetCrimes_data_path>
```
When service is succesfully started, at the last line you should see message in log like:
```console
INFO finagle: Finagle version 22.4.0 (rev=8e95266684268c1fbac47d071753f78a3d8ba1fe) built at 20220419-220327
```
Service should be available at the port 10000.
