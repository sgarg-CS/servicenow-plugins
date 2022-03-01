/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.servicenow.source;

import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableDataResponse;
import io.cdap.plugin.servicenow.source.util.ServiceNowColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ServiceNowMultiInputFormatTest {

  @Test
  public void testFetchTablesInfo() {
    ServiceNowMultiSourceConfig config =
      new ServiceNowMultiSourceConfig("Reference Name", "tableNameTable", "client_id",
                                      "client_secret", "http://example.com", "user",
                                      "password", "Actual", "2021-12-30",
                                      "2021-12-31", "sys_user");
    ServiceNowColumn column1 = new ServiceNowColumn("sys_created_by", "string");
    ServiceNowColumn column2 = new ServiceNowColumn("sys_updated_by", "string");
    List<ServiceNowColumn> columns = new ArrayList<>();
    columns.add(column1);
    columns.add(column2);
    ServiceNowTableDataResponse response = new ServiceNowTableDataResponse();
    response.setColumns(columns);
    Assert.assertEquals(0, ServiceNowMultiInputFormat
      .fetchTablesInfo(config)
      .size());
  }

  @Test
  public void testFetchTablesInfoWithEmptyTableNames() {
    ServiceNowMultiSourceConfig config = new ServiceNowMultiSourceConfig("Reference Name",
     "tableName", "client_id", "Client Secret", "http://example.com",
     "user", "password", "Actual", "2021-12-30", "2021-12-31", "");
    Assert.assertTrue(ServiceNowMultiInputFormat
                        .fetchTablesInfo(config)
                        .isEmpty());
  }
}
