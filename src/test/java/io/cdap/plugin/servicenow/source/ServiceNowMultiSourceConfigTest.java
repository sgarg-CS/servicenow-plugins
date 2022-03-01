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

import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

/**
 * Tests for {@link ServiceNowMultiSourceConfig}.
 */
public class ServiceNowMultiSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private ServiceNowMultiSourceConfig serviceNowMultiSourceConfig;

  @Before
  public void initializeTests() {
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("Reference Name")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("tablename")
        .buildMultiSource();
  }

  @Test
  public void testConstructor() {
    Assert.assertEquals("sys_user", serviceNowMultiSourceConfig.getTableNames());
    Assert.assertEquals("Actual", serviceNowMultiSourceConfig.getValueType().getValueType());
    Assert.assertEquals("2021-12-30", serviceNowMultiSourceConfig.getStartDate());
    Assert.assertEquals("2021-12-31", serviceNowMultiSourceConfig.getEndDate());
    Assert.assertEquals("tablename", serviceNowMultiSourceConfig.getTableNameField());
  }

  @Test
  public void testValidate() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig.validate(mockFailureCollector);
    Assert.assertEquals(3, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateWhenTableFieldNameIsEmpty() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("Reference Name")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("")
        .buildMultiSource();
    serviceNowMultiSourceConfig.validate(mockFailureCollector);
    Assert.assertEquals(4, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateTableNames() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig.validateTableNames(mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateTableNamesWhenTableNamesAreEmpty() throws NoSuchFieldException {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    serviceNowMultiSourceConfig =
      ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("Reference Name")
        .setRestApiEndpoint("https://example.com")
        .setUser("user")
        .setPassword("password")
        .setClientId("client_id")
        .setClientSecret("client_secret")
        .setTableNames("")
        .setValueType("Actual")
        .setStartDate("2021-12-30")
        .setEndDate("2021-12-31")
        .setTableNameField("tablename")
        .buildMultiSource();

    serviceNowMultiSourceConfig.validateTableNames(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    Assert.assertEquals(1, validationFailures.size());
  }

}
