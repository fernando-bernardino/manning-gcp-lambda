# Process data

## Milestone 2 - Modify BigQuery Table Schema for Optimized Querying and Data Analysis

Created the table `partitioned_table`, partition on field `timestamp`, by DAY:

![img_1.png](img_1.png)

Create a Cloud function to import the CSV data from `gs://fernando-bernardino-long-term/telemetry_data`, with
[com.example.ImportCvsToTable.java](../src/main/java/com/example/ImportCvsToTable.java), a background function.

![img_6.png](img_6.png)

Although it is listening in a topic, I didn't send any message to the topic, instead I triggered that background 
function from my machine with:

    $gcloud functions call --project pure-album-313018 --region europe-west2 extract-event --data '{}'
    executionId: ajgdrvyl08ri
    $

Where are the logs from its execution:

![img_5.png](img_5.png)

And here is the imported data in the partitioned table (20 rows total):

![img_2.png](img_2.png)

Created a new pipeline job from the one used to import to data from the topic 
`projects/pure-album-313018/topics/bernardino-realtime-ingestion` to the table, 
but this time saving the data into `partitioned_table`:

![img.png](img.png)

Executed the simulator again and waited for data to arrived at the partitioned table.

This is the newly added data, in the 2021-05-20 (10 rows total):

![img_3.png](img_3.png)

Here is the table data all together (30 rows total): 

![img_4.png](img_4.png)

Changed the previous extractor cloud function to export to newly partitioned to the long term storage,
`gs://fernando-bernardino-long-term/telemetry_data_partitioned`. (see
[com.example.ExtractPartitionedTableToCsv.java](../src/main/java/com/example/ExtractPartitionedTableToCsv.java))
and pushed the Scheduler job to run right away and trigger the that cloud function.
It generated the file [telemetry_data_partitioned](./telemetry_data_partitioned)

Here is the object resulting from the extraction:

![img_7.png](img_7.png)

And here is the logs from the execution of the cloud funtion:

![img_8.png](img_8.png)