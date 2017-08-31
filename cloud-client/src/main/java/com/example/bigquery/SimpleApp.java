/*
  Copyright 2016, Google, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.example.bigquery;

// [START all]
// [START imports]
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.util.*;
// [END imports]

public class SimpleApp {

    private static final String projectId = "corded-vim-168917";
    private static final String datasetName = "motor_show";

    public static void main(String... args) throws Exception {


        // ----------------- [START create_client]
        // BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("key.json")))
                .build().getService();
        // -----------------  [END create_client]


        // ----------------- [START run_query]
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "SELECT * FROM `corded-vim-168917.GBQ_DS.COLORS` "
                                + "WHERE upper(_English) like '%YELLOW%';")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        QueryResponse response = bigquery.getQueryResults(jobId);
        // -----------------  [END run_query]


        // -----------------  [START print_results]
        QueryResult result = response.getResult();

        // Print all pages of the results.
        if (result != null) {

            // Print Headers
            List<Field> headers = result.getSchema().getFields();
            for (Field name : headers) {
                System.out.print(name.getName() + ",");
            }

            // Print Data
            System.out.println();
            while (result != null) {
                for (List<FieldValue> row : result.iterateAll()) {
                    for (FieldValue val : row) {
                        System.out.print(val.getStringValue() + ",");
                    }
                    System.out.println();
                }
                result = result.getNextPage();
            }
        }
        System.out.println("END");
        // -----------------  [END print_results]


        // -----------------  [START create a dataset]
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
        // Creates the dataset
        try {
            Dataset dataset = bigquery.create(datasetInfo);
            System.out.printf("Dataset %s created.%n", dataset.getDatasetId().getDataset());
        } catch (BigQueryException ex) {
            System.out.printf("%s%n", ex.getMessage());
        }
        // -----------------  [END create a dataset]


        // -----------------  [START create a data table]
        String tableName = UUID.randomUUID().toString().replace("-","_");
        //String tableName = "cars";
        //Boolean deleted = bigquery.delete(datasetName, tableName);
        TableId tableId = TableId.of(projectId, datasetName, tableName);

        // Table schema definition
        Schema schema = Schema.of(
                Field.of("year", Field.Type.integer()),
                Field.of("make", Field.Type.string()),
                Field.of("model", Field.Type.string()),
                Field.of("dealer", Field.Type.record(
                        Field.of("name", Field.Type.string()),
                        Field.of("address", Field.Type.string()),
                        Field.of("country", Field.Type.string())))
        );

        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table newTable = bigquery.create(tableInfo);
        // -----------------  [END create a data table]


        // -----------------  [START upload_data]

        // Values of the row to insert
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("year", 2002);
        rowContent.put("make", "ACURA");
        rowContent.put("model", "NSX");

        // Records are passed as a map
        Map<String, Object> recordsContent = new HashMap<>();
        recordsContent.put("name", "Sunning Motors Ltd");
        recordsContent.put("address", "5-7 Yuen Shun Circuit, Sha Tin");
        recordsContent.put("country", "Hong Kong");
        rowContent.put("dealer", recordsContent);

        InsertAllResponse ins_response = bigquery.insertAll(InsertAllRequest.newBuilder(newTable.getTableId())
                .addRow("rowId", rowContent)
                // More rows can be added in the same RPC by invoking .addRow() on the builder
                .build());
        if (ins_response.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            for (Map.Entry<Long, List<BigQueryError>> entry : ins_response.getInsertErrors().entrySet()) {
                // inspect row error
            }
        }
        // -----------------  [END upload_data]
    }

}
// [END all]
