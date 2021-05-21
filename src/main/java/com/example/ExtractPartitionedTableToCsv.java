package com.example;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.functions.*;
import org.threeten.bp.Duration;

public class ExtractPartitionedTableToCsv implements HttpFunction {

  @Override
  public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
    try {
      runExtractTableToCsv();
      httpResponse.setStatusCode(200);
    } catch (Exception e) {
      httpResponse.setStatusCode(500);
    }
  }


  public void runExtractTableToCsv() {
    String projectId = "pure-album-313018";
    String datasetName = "tsunami_reading";
    String tableName = "partitioned_table";
    String bucketName = "fernando-bernardino-long-term";
    String destinationUri = "gs://" + bucketName + "/telemetry_data_partitioned";
    // For more information on export formats available see:
    // https://cloud.google.com/bigquery/docs/exporting-data#export_formats_and_compression_types
    // For more information on Job see:
    // https://googleapis.dev/java/google-cloud-clients/latest/index.html?com/google/cloud/bigquery/package-summary.html

    String dataFormat = "CSV";
    extractTableToCsv(projectId, datasetName, tableName, destinationUri, dataFormat);
  }

  // Exports datasetName:tableName to destinationUri as raw CSV
  public void extractTableToCsv(
    String projectId,
    String datasetName,
    String tableName,
    String destinationUri,
    String dataFormat) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      TableId tableId = TableId.of(projectId, datasetName, tableName);
      Table table = bigquery.getTable(tableId);

      Job job = table.extract(dataFormat, destinationUri);

      // Blocks until this job completes its execution, either failing or succeeding.
      Job completedJob =
        job.waitFor(
          RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
          RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob == null) {
        System.out.println("Job not executed since it no longer exists.");
        return;
      } else if (completedJob.getStatus().getError() != null) {
        System.out.println(
          "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
        return;
      }
      System.out.println(
        "Table export successful. Check in GCS bucket for the " + dataFormat + " file.");
    } catch (BigQueryException | InterruptedException e) {
      System.out.println("Table extraction job was interrupted. \n" + e.toString());
    }
  }
}