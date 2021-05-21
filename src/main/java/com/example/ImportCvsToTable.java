package com.example;

import com.google.cloud.bigquery.*;
import com.google.cloud.functions.*;

public class ImportCvsToTable implements BackgroundFunction<Void> {

  @Override
  public void accept(Void o, Context context) throws Exception {
    runLoadCsvFromGcs();
  }

  public static void runLoadCsvFromGcs() throws Exception {
    String projectId = "pure-album-313018";
    String datasetName = "tsunami_reading";
    String tableName = "partitioned_table";
    String sourceUri = "gs://fernando-bernardino-long-term/telemetry_data";
    Schema schema =
      Schema.of(
        Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("tsunami_event_validity", StandardSQLTypeName.NUMERIC).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("tsunami_cause_code", StandardSQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("earthquake_magnitude", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("latitude", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("longitude", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("maximum_water_height", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build());
    loadCsvFromGcs(projectId, datasetName, tableName, sourceUri, schema);
  }

  public static void loadCsvFromGcs(
    String projectId, String datasetName, String tableName, String sourceUri, Schema schema) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      // Skip header row in the file.
      CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();

      TableId tableId = TableId.of(projectId, datasetName, tableName);
      LoadJobConfiguration loadConfig =
        LoadJobConfiguration.newBuilder(tableId, sourceUri, csvOptions).setSchema(schema).build();

      // Load data from a GCS CSV file into the table
      Job job = bigquery.create(JobInfo.of(loadConfig));
      // Blocks until this load table job completes its execution, either failing or succeeding.
      job = job.waitFor();
      if (job.isDone()) {
        System.out.println("CSV from GCS successfully added during load append job");
      } else {
        System.out.println(
          "BigQuery was unable to load into the table due to an error:"
            + job.getStatus().getError());
      }
    } catch (BigQueryException | InterruptedException e) {
      System.out.println("Column not added during load append \n" + e.toString());
    }
  }
}