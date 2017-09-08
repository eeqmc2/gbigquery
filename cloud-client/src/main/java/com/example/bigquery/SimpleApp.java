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
import com.example.bigquery.clientApi.BigQueryApiWrapper;
import com.example.bigquery.util.Utils;
import com.example.bigquery.util.XmlConverter;
import com.google.cloud.bigquery.*;

//import com.google.common.base.Charsets;
//import com.google.common.io.Resources;
import java.util.UUID;

// [END imports]

public class SimpleApp {

    // Person Googgle Account
    //private static final String projectId = "corded-vim-168917";
    //private static final String datasetName = "motor_show";

    // MapleQuad
    private static final String projectId = "eep-ref-data";
    private static final String datasetName = "public_random_data";
    private static final String testDatasetName = "public_testing_data";

    public static void main(String... args) throws Exception {


        BigQueryApiWrapper gbq = new BigQueryApiWrapper();

        // Connect to Project via key
        gbq.connect(projectId);

        // Perform a SQL Query & Print Results
        String queryString = "SELECT * FROM `" + datasetName + ".country` WHERE country like '%Australia%';";
        QueryResponse response = gbq.query(queryString);

        // Print Results
        gbq.printResult(response);

        // Create a New Dataset
        gbq.createDataset("testing");

        // Create empty table
        String tableName = UUID.randomUUID().toString().replace("-","_");
        Table tbl = gbq.createTable(projectId, "testing", tableName);

        gbq.insert(tbl);

        System.out.println("Testing");



        Utils helper = new Utils();
        String content = helper.getFileFromResource("data/country-list.xml");
        System.out.println(content);


        XmlConverter.Xml2Json(content);
        System.out.println("-------------------");


        String tblName = "AlexTest";
        TableId tableId = TableId.of(projectId, testDatasetName, tblName);
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.csv())
                        .build();
        /*
        TableDataWriteChannel writer = bigquery.writer(writeChannelConfiguration);
        // Write data to writer
        try (OutputStream stream = Channels.newOutputStream(writer)) {
            Path resPath = helper.getPathFromResource("data/country-list.xml");
            Files.copy(resPath, stream);
        }

        // Get load job
        Job job = writer.getJob();
        job = job.waitFor();
        JobStatistics.LoadStatistics stats = job.getStatistics();
        stats.getOutputRows();
        */


    }

}
// [END all]
