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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableDataResponse;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServiceNowRecordReaderTest {

  private static final String CLIENT_ID = System.getProperty("servicenow.test.clientId");
  private static final String CLIENT_SECRET = System.getProperty("servicenow.test.clientSecret");
  private static final String REST_API_ENDPOINT = System.getProperty("servicenow.test.restApiEndpoint");
  private static final String USER = System.getProperty("servicenow.test.user");
  private static final String PASSWORD = System.getProperty("servicenow.test.password");
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowMultiInputFormatTest.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private ServiceNowSourceConfig serviceNowSourceConfig;
  private ServiceNowRecordReader serviceNowRecordReader;

  @Before
  public void initializeTests() {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, REST_API_ENDPOINT, USER, PASSWORD);
      serviceNowSourceConfig = ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("referenceName")
        .setRestApiEndpoint(REST_API_ENDPOINT)
        .setUser(USER)
        .setPassword(PASSWORD)
        .setClientId(CLIENT_ID)
        .setClientSecret(CLIENT_SECRET)
        .setTableName("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-01-01")
        .setEndDate("2022-02-18")
        .setTableNameField("tablename")
        .build();

      serviceNowRecordReader = new ServiceNowRecordReader(serviceNowSourceConfig);

    } catch (AssumptionViolatedException e) {
      LOG.warn("Service Now batch multi source tests are skipped. ");
      throw e;
    }
  }

  @Test
  public void testConstructor() throws IOException {
    serviceNowRecordReader.close();
    Assert.assertEquals(0, serviceNowRecordReader.pos);
  }

  @Test
  public void testConstructor2() throws IOException {

    serviceNowRecordReader.close();
    Assert.assertEquals(0, serviceNowRecordReader.pos);
    Assert.assertEquals("pipeline.user.1", serviceNowSourceConfig.getUser());
    Assert.assertEquals("tablename", serviceNowSourceConfig.getTableNameField());
    Assert.assertEquals("sys_user", serviceNowSourceConfig.getTableName());
    Assert.assertEquals("2021-01-01", serviceNowSourceConfig.getStartDate());
    Assert.assertEquals("https://ven05127.service-now.com", serviceNowSourceConfig.getRestApiEndpoint());
    Assert.assertEquals("referenceName", serviceNowSourceConfig.getReferenceName());
    Assert.assertEquals("2022-02-18", serviceNowSourceConfig.getEndDate());
    PluginProperties properties = serviceNowSourceConfig.getProperties();
    Assert.assertTrue(properties.getProperties().isEmpty());
  }

  @Test
  public void testConvertToValue() {
    Schema fieldSchema = Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
    thrown.expect(IllegalStateException.class);
    serviceNowRecordReader.convertToValue("Field Name", fieldSchema, new HashMap<>(1));
  }

  @Test
  public void testConvertToValueBopoleanFieldType() {
    Schema fieldSchema = Schema.of(Schema.Type.BOOLEAN);
    serviceNowRecordReader.convertToValue("Field Name", fieldSchema, new HashMap<>(1));
  }


  @Test
  public void testConvertToValueStringFieldType() {
    Schema fieldSchema = Schema.of(Schema.Type.STRING);
    serviceNowRecordReader.convertToValue("Field Name", fieldSchema, new HashMap<>(1));
  }


  @Test
  public void testConvertToStringValue() {
    Assert.assertEquals("Field Value", serviceNowRecordReader.convertToStringValue("Field Value"));
  }

  @Test
  public void testConvertToDoubleValue() {
    Assert.assertEquals(42.0, serviceNowRecordReader.convertToDoubleValue("42").doubleValue(), 0.0);
    Assert.assertEquals(42.0, serviceNowRecordReader.convertToDoubleValue(42).doubleValue(), 0.0);
    Assert.assertNull(serviceNowRecordReader.convertToDoubleValue(""));
  }

  @Test
  public void testConvertToIntegerValue() {
    Assert.assertEquals(42, serviceNowRecordReader.convertToIntegerValue("42").intValue());
    Assert.assertEquals(42, serviceNowRecordReader.convertToIntegerValue(42).intValue());
    Assert.assertNull(serviceNowRecordReader.convertToIntegerValue(""));
  }

  @Test
  public void testConvertToBooleanValue() {
    Assert.assertFalse(serviceNowRecordReader.convertToBooleanValue("Field Value"));
    Assert.assertFalse(serviceNowRecordReader.convertToBooleanValue(42));
    Assert.assertNull(serviceNowRecordReader.convertToBooleanValue(""));
  }

  @Test
  public void testFetchData() throws NoSuchFieldException, IOException {
    String tableName = serviceNowSourceConfig.getTableName();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    ServiceNowInputSplit split = new ServiceNowInputSplit(tableName, 1);
    ServiceNowRecordReader serviceNowRecordReader = new ServiceNowRecordReader(serviceNowSourceConfig);
    List<Map<String, Object>> results = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("calendar_integration", "1");
    map.put("country", "India");
    map.put("sys_updated_on", "2019-04-05 21:54:45");
    map.put("web_service_access_only", "false");
    map.put("notification", "2");
    map.put("enable_multifactor_authn", "false");
    map.put("sys_updated_by", "system");
    map.put("sys_created_on", "2019-04-05 21:09:12");
    results.add(map);
    FieldSetter.setField(serviceNowRecordReader, ServiceNowRecordReader.class.getDeclaredField("split"), split);
    Mockito.when(restApi.fetchTableRecords(tableName, serviceNowSourceConfig.getStartDate(),
                                           serviceNowSourceConfig.getEndDate(),
                                           split.getOffset(), ServiceNowConstants.PAGE_SIZE)).thenReturn(results);

    ServiceNowTableDataResponse response = new ServiceNowTableDataResponse();
    response.setResult(results);
    serviceNowRecordReader.initialize(split, null);
    Assert.assertTrue(serviceNowRecordReader.nextKeyValue());
  }

  @Test
  public void testFetchDataOnInvalidTable() throws NoSuchFieldException, IOException {
    serviceNowSourceConfig = ServiceNowSourceConfigHelper.newConfigBuilder()
      .setReferenceName("referenceName")
      .setRestApiEndpoint(REST_API_ENDPOINT)
      .setUser(USER)
      .setPassword(PASSWORD)
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setTableName("")
      .setValueType("Actual")
      .setStartDate("2021-01-01")
      .setEndDate("2022-02-18")
      .setTableNameField("tablename")
      .build();

    String tableName = serviceNowSourceConfig.getTableName();
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    ServiceNowInputSplit split = new ServiceNowInputSplit(tableName, 1);
    ServiceNowRecordReader serviceNowRecordReader = new ServiceNowRecordReader(serviceNowSourceConfig);
    List<Map<String, Object>> results = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("calendar_integration", "1");
    map.put("country", "India");
    map.put("sys_updated_on", "2019-04-05 21:54:45");
    map.put("web_service_access_only", "false");
    map.put("notification", "2");
    map.put("enable_multifactor_authn", "false");
    map.put("sys_updated_by", "system");
    map.put("sys_created_on", "2019-04-05 21:09:12");
    results.add(map);
    FieldSetter.setField(serviceNowRecordReader, ServiceNowRecordReader.class.getDeclaredField("split"), split);
    Mockito.when(restApi.fetchTableRecords(tableName, serviceNowSourceConfig.getStartDate(),
                                           serviceNowSourceConfig.getEndDate(),
                                           split.getOffset(), ServiceNowConstants.PAGE_SIZE)).thenReturn(results);

    ServiceNowTableDataResponse response = new ServiceNowTableDataResponse();
    response.setResult(results);
    serviceNowRecordReader.initialize(split, null);
    Assert.assertFalse(serviceNowRecordReader.nextKeyValue());
  }

}
