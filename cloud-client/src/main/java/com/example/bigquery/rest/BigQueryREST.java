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

package com.example.bigquery.rest;


import java.io.*;
import java.net.*;

public class BigQueryREST {


    /*https://www.googleapis.com/bigquery/v2/projects/myproject/datasets/mydataset/tables?alt=json
    {"tableReference":
        {"tableId": "dfdlkfjx", "projectId": "myproject", "datasetId": "mydataset"},
        "schema":
        {"fields": [{"name": "a", "type": "STRING"}]}}*/

    //https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll

/*
 * Stolen from http://xml.nig.ac.jp/tutorial/rest/index.html
 * and http://www.dr-chuck.com/csev-blog/2007/09/calling-rest-web-services-from-java/
*/
/*
        public static void main(String[] args) throws IOException {
            URL url = new URL(INSERT_HERE_YOUR_URL);
            String query = INSERT_HERE_YOUR_URL_PARAMETERS;

            //make connection
            URLConnection urlc = url.openConnection();

            //use post mode
            urlc.setDoOutput(true);
            urlc.setAllowUserInteraction(false);

            //send query
            PrintStream ps = new PrintStream(urlc.getOutputStream());
            ps.print(query);
            ps.close();

            //get result
            BufferedReader br = new BufferedReader(new InputStreamReader(urlc
                    .getInputStream()));
            String l = null;
            while ((l=br.readLine())!=null) {
                System.out.println(l);
            }
            br.close();
        }*/
}