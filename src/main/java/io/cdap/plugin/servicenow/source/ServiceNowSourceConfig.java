/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.util.SourceApplication;
import io.cdap.plugin.servicenow.source.util.SourceQueryMode;
import io.cdap.plugin.servicenow.source.util.SourceValueType;
import io.cdap.plugin.servicenow.source.util.Util;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import javax.annotation.Nullable;

import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.DATE_FORMAT;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_API_ENDPOINT;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_APPLICATION_NAME;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_CLIENT_ID;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_CLIENT_SECRET;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_END_DATE;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_PASSWORD;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_QUERY_MODE;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_START_DATE;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_TABLE_NAME;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_TABLE_NAME_FIELD;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_USER;
import static io.cdap.plugin.servicenow.source.util.ServiceNowConstants.PROPERTY_VALUE_TYPE;

/**
 * Configuration for the {@link ServiceNowSource}.
 */
public class ServiceNowSourceConfig extends PluginConfig {
  @Name(Constants.Reference.REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private String referenceName;

  @Name(PROPERTY_QUERY_MODE)
  @Macro
  @Description("Mode of query. The mode can be one of two values: "
    + "`Reporting` - will allow user to choose application for which data will be fetched for all tables, "
    + "`Table` - will allow user to enter table name for which data will be fetched.")
  private String queryMode;

  @Name(PROPERTY_APPLICATION_NAME)
  @Macro
  @Nullable
  @Description("Application name for which data to be fetched. The application can be one of three values: " +
    "`Contract Management` - will fetch data for all tables under Contract Management application, " +
    "`Product Catalog` - will fetch data for all tables under Product Catalog application, " +
    "`Procurement` - will fetch data for all tables under Procurement application. " +
    "Note, the Application name value will be ignored if the Mode is set to `Table`.")
  private String applicationName;

  @Name(PROPERTY_TABLE_NAME_FIELD)
  @Macro
  @Nullable
  @Description("The name of the field that holds the table name. Must not be the name of any table column that " +
    "will be read. Defaults to `tablename`. Note, the Table name field value will be ignored if the Mode " +
    "is set to `Table`.")
  private String tableNameField;

  @Name(PROPERTY_TABLE_NAME)
  @Macro
  @Nullable
  @Description("The name of the ServiceNow table from which data to be fetched. Note, the Table name value " +
    "will be ignored if the Mode is set to `Reporting`.")
  private String tableName;

  @Name(PROPERTY_CLIENT_ID)
  @Macro
  @Description(" The Client ID for ServiceNow Instance.")
  private String clientId;

  @Name(PROPERTY_CLIENT_SECRET)
  @Macro
  @Description("The Client Secret for ServiceNow Instance.")
  private String clientSecret;

  @Name(PROPERTY_API_ENDPOINT)
  @Macro
  @Description("The REST API Endpoint for ServiceNow Instance. For example, https://instance.service-now.com")
  private String restApiEndpoint;

  @Name(PROPERTY_USER)
  @Macro
  @Description("The user name for ServiceNow Instance.")
  private String user;

  @Name(PROPERTY_PASSWORD)
  @Macro
  @Description("The password for ServiceNow Instance.")
  private String password;

  @Name(PROPERTY_VALUE_TYPE)
  @Macro
  @Nullable
  @Description("The type of values to be returned. The type can be one of two values: "
    + "`Actual` -  will fetch the actual values from the ServiceNow tables, "
    + "`Display` - will fetch the display values from the ServiceNow tables.")
  private String valueType;

  @Name(PROPERTY_START_DATE)
  @Macro
  @Nullable
  @Description("The Start date to be used to filter the data. The format must be 'yyyy-MM-dd'.")
  private String startDate;

  @Name(PROPERTY_END_DATE)
  @Macro
  @Nullable
  @Description("The End date to be used to filter the data. The format must be 'yyyy-MM-dd'.")
  private String endDate;

  public ServiceNowSourceConfig(String referenceName, String queryMode, @Nullable String applicationName,
                                @Nullable String tableNameField, @Nullable String tableName, String clientId,
                                String clientSecret, String restApiEndpoint, String user, String password,
                                @Nullable String valueType, @Nullable String startDate, @Nullable String endDate) {
    this.referenceName = referenceName;
    this.queryMode = queryMode;
    this.applicationName = applicationName;
    this.tableNameField = tableNameField;
    this.tableName = tableName;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.restApiEndpoint = restApiEndpoint;
    this.user = user;
    this.password = password;
    this.valueType = valueType;
    this.startDate = startDate;
    this.endDate = endDate;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public SourceQueryMode getQueryMode(FailureCollector collector) {
    SourceQueryMode mode = getQueryMode();
    if (mode != null) {
      return mode;
    }

    collector.addFailure("Unsupported query mode value: " + queryMode,
      String.format("Supported modes are: %s", SourceQueryMode.getSupportedModes()))
      .withConfigProperty(PROPERTY_QUERY_MODE);
    throw collector.getOrThrowException();
  }

  public SourceQueryMode getQueryMode() {
    Optional<SourceQueryMode> sourceQueryMode = SourceQueryMode.fromValue(queryMode);

    return sourceQueryMode.isPresent() ? sourceQueryMode.get() : null;
  }

  public SourceApplication getApplicationName(FailureCollector collector) {
    SourceApplication application = getApplicationName();
    if (application != null) {
      return application;
    }

    collector.addFailure("Unsupported application name value: " + applicationName,
      String.format("Supported applications are: %s", SourceApplication.getSupportedApplications()))
      .withConfigProperty(PROPERTY_APPLICATION_NAME);
    throw collector.getOrThrowException();
  }

  @Nullable
  public SourceApplication getApplicationName() {
    Optional<SourceApplication> sourceApplication = SourceApplication.fromValue(applicationName);

    return sourceApplication.isPresent() ? sourceApplication.get() : null;
  }

  @Nullable
  public String getTableNameField() {
    return tableNameField;
  }

  @Nullable
  public String getTableName() {
    return tableName;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getRestApiEndpoint() {
    return restApiEndpoint;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public SourceValueType getValueType(FailureCollector collector) {
    SourceValueType type = getValueType();
    if (type != null) {
      return type;
    }

    collector.addFailure("Unsupported type value: " + valueType,
      String.format("Supported value types are: %s", SourceValueType.getSupportedValueTypes()))
      .withConfigProperty(PROPERTY_VALUE_TYPE);
    throw collector.getOrThrowException();
  }

  @Nullable
  public SourceValueType getValueType() {
    Optional<SourceValueType> sourceValueType = SourceValueType.fromValue(valueType);

    return sourceValueType.isPresent() ? sourceValueType.get() : null;
  }

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
  }

  /**
   * Validates {@link ServiceNowSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    //Validates the given referenceName to consists of characters allowed to represent a dataset.
    IdUtils.validateReferenceName(referenceName, collector);

    validateCredentials(collector);
    validateQueryMode(collector);
    getValueType(collector);
    validateDateRange(collector);
  }

  private void validateCredentials(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }

    if (Util.isNullOrEmpty(clientId)) {
      collector.addFailure("Client ID must be specified.", null)
        .withConfigProperty(PROPERTY_CLIENT_ID);
    }

    if (Util.isNullOrEmpty(clientSecret)) {
      collector.addFailure("Client Secret must be specified.", null)
        .withConfigProperty(PROPERTY_CLIENT_SECRET);
    }

    if (Util.isNullOrEmpty(restApiEndpoint)) {
      collector.addFailure("API Endpoint must be specified.", null)
        .withConfigProperty(PROPERTY_API_ENDPOINT);
    }

    if (Util.isNullOrEmpty(user)) {
      collector.addFailure("User name must be specified.", null)
        .withConfigProperty(PROPERTY_USER);
    }

    if (Util.isNullOrEmpty(password)) {
      collector.addFailure("Password must be specified.", null)
        .withConfigProperty(PROPERTY_PASSWORD);
    }

    validateServiceNowConnection(collector);
  }

  @VisibleForTesting
  void validateServiceNowConnection(FailureCollector collector) {
    try {
      ServiceNowTableAPIClientImpl restApi = new ServiceNowTableAPIClientImpl(this);
      restApi.getAccessToken();
    } catch (Exception e) {
      collector.addFailure("Unable to connect to ServiceNow Instance.",
        "Ensure properties like Client ID, Client Secret, API Endpoint, User Name, Password " +
          "are correct.")
        .withConfigProperty(PROPERTY_CLIENT_ID)
        .withConfigProperty(PROPERTY_CLIENT_SECRET)
        .withConfigProperty(PROPERTY_API_ENDPOINT)
        .withConfigProperty(PROPERTY_USER)
        .withConfigProperty(PROPERTY_PASSWORD)
        .withStacktrace(e.getStackTrace());
    }
  }

  private void validateQueryMode(FailureCollector collector) {
    //according to query mode check if either table name/application exists or not
    if (containsMacro(PROPERTY_QUERY_MODE)) {
      return;
    }

    SourceQueryMode mode = getQueryMode(collector);

    if (mode == SourceQueryMode.REPORTING) {
      validateReportingQueryMode(collector);
    } else {
      validateTableQueryMode(collector);
    }
  }

  private void validateReportingQueryMode(FailureCollector collector) {
    if (!containsMacro(PROPERTY_APPLICATION_NAME)) {
      getApplicationName(collector);
    }

    if (containsMacro(PROPERTY_TABLE_NAME_FIELD)) {
      return;
    }

    if (Util.isNullOrEmpty(tableNameField)) {
      collector.addFailure("Table name field must be specified.", null)
        .withConfigProperty(PROPERTY_TABLE_NAME_FIELD);
    }
  }

  private void validateTableQueryMode(FailureCollector collector) {
    if (containsMacro(PROPERTY_TABLE_NAME)) {
      return;
    }

    if (Util.isNullOrEmpty(tableName)) {
      collector.addFailure("Table name must be specified.", null)
        .withConfigProperty(PROPERTY_TABLE_NAME);
    }
  }

  private void validateDateRange(FailureCollector collector) {
    if (containsMacro(PROPERTY_START_DATE) || containsMacro(PROPERTY_END_DATE)) {
      return;
    }

    if (Util.isNullOrEmpty(startDate) || Util.isNullOrEmpty(endDate)) {
      return;
    }

    //validate the date formats for both start date & end date
    if (!Util.isValidDateFormat(DATE_FORMAT, startDate)) {
      collector.addFailure("Invalid format for Start date. Correct Format: " + DATE_FORMAT, null)
        .withConfigProperty(PROPERTY_START_DATE);
      return;
    }

    if (!Util.isValidDateFormat(DATE_FORMAT, endDate)) {
      collector.addFailure("Invalid format for End date. Correct Format:" + DATE_FORMAT, null)
        .withConfigProperty(PROPERTY_END_DATE);
      return;
    }

    //validate the date range by checking if start date is smaller than end date
    LocalDate fromDate = LocalDate.parse(startDate);
    LocalDate toDate = LocalDate.parse(endDate);
    long noOfDays = ChronoUnit.DAYS.between(fromDate, toDate);

    if (noOfDays < 0) {
      collector.addFailure("End date must be greater than Start date.", null)
        .withConfigProperty(PROPERTY_START_DATE)
        .withConfigProperty(PROPERTY_END_DATE);
    }
  }

  /**
   * Returns true if ServiceNow can be connected to.
   */
  public boolean shouldConnect() {
    return !containsMacro(PROPERTY_CLIENT_ID) &&
      !containsMacro(PROPERTY_CLIENT_SECRET) &&
      !containsMacro(PROPERTY_API_ENDPOINT) &&
      !containsMacro(PROPERTY_USER) &&
      !containsMacro(PROPERTY_PASSWORD);
  }
}
