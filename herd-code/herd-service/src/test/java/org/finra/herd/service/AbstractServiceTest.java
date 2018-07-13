/*
 * Copyright 2015 herd contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.engine.HistoryService;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.TaskService;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.history.HistoricProcessInstanceQuery;
import org.activiti.engine.repository.Deployment;
import org.activiti.spring.SpringProcessEngineConfiguration;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.TagDaoTestHelper;
import org.finra.herd.dao.TagTypeDaoTestHelper;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatus;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormat;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.SearchIndexStatistics;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitStatusChangeEvent;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.service.activiti.ActivitiHelper;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;
import org.finra.herd.service.activiti.HerdCommandInvoker;
import org.finra.herd.service.activiti.task.ExecuteJdbcTestHelper;
import org.finra.herd.service.config.ServiceTestSpringModuleConfig;
import org.finra.herd.service.helper.BusinessObjectDataAttributeDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataAttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper;
import org.finra.herd.service.helper.BusinessObjectDataRetryStoragePolicyTransitionHelper;
import org.finra.herd.service.helper.BusinessObjectDataSearchHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionColumnDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionHelper;
import org.finra.herd.service.helper.EmrStepHelperFactory;
import org.finra.herd.service.helper.Hive13DdlGenerator;
import org.finra.herd.service.helper.JobDefinitionHelper;
import org.finra.herd.service.helper.MessageTypeDaoHelper;
import org.finra.herd.service.helper.NotificationActionFactory;
import org.finra.herd.service.helper.NotificationMessageBuilder;
import org.finra.herd.service.helper.NotificationRegistrationDaoHelper;
import org.finra.herd.service.helper.NotificationRegistrationStatusDaoHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.S3PropertiesLocationHelper;
import org.finra.herd.service.helper.SearchIndexDaoHelper;
import org.finra.herd.service.helper.SearchIndexStatusDaoHelper;
import org.finra.herd.service.helper.SearchIndexTypeDaoHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;
import org.finra.herd.service.helper.VelocityHelper;

/**
 * This is an abstract base class that provides useful methods for service test drivers.
 */
@ContextConfiguration(classes = ServiceTestSpringModuleConfig.class, inheritLocations = false)
public abstract class AbstractServiceTest extends AbstractDaoTest
{
    public static final String ACTIVITI_JOB_DELETE_REASON = "UT_JobDeleteReason" + RANDOM_SUFFIX;

    public static final String ACTIVITI_XML_ADD_EMR_MASTER_SECURITY_GROUPS_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowAddEmrMasterSecurityGroup.bpmn20.xml";

    public static final String ACTIVITI_XML_ADD_EMR_STEPS_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowAddEmrStep.bpmn20.xml";

    public static final String ACTIVITI_XML_CHECK_CLUSTER_AND_RECEIVE_TASK_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowCheckEmrClusterAndReceiveTask.bpmn20.xml";

    public static final String ACTIVITI_XML_CHECK_CLUSTER_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowCheckEmrCluster.bpmn20.xml";

    public static final String ACTIVITI_XML_CREATE_CLUSTER_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowCreateEmrCluster.bpmn20.xml";

    public static final String ACTIVITI_XML_HERD_INTERMEDIATE_TIMER_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/testHerdIntermediateTimerWorkflow.bpmn20.xml";

    public static final String ACTIVITI_XML_HERD_TIMER = "org/finra/herd/service/testHerdTimerWorkflow.bpmn20.xml";

    public static final String ACTIVITI_XML_HERD_TIMER_WITH_CLASSPATH = "classpath:" + ACTIVITI_XML_HERD_TIMER;

    // Activiti workflow resources with and without classpath prefix.
    public static final String ACTIVITI_XML_HERD_WORKFLOW = "org/finra/herd/service/testHerdWorkflow.bpmn20.xml";

    public static final String ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH = "classpath:" + ACTIVITI_XML_HERD_WORKFLOW;

    public static final String ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowLogVariablesNoRegex.bpmn20.xml";

    public static final String ACTIVITI_XML_LOG_VARIABLES_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowLogVariables.bpmn20.xml";

    public static final String ACTIVITI_XML_LOG_VARIABLES_WITH_CLASSPATH_DM = "classpath:org/finra/herd/service/activitiWorkflowLogVariablesDm.bpmn20.xml";

    public static final String ACTIVITI_XML_TERMINATE_CLUSTER_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowTerminateEmrCluster.bpmn20.xml";

    public static final String ACTIVITI_XML_TEST_MULTIPLE_SUB_PROCESSES = "classpath:org/finra/herd/service/testHerdMultipleSubProcessesWorkflow.bpmn20.xml";

    public static final String ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testHerdReceiveTaskWorkflow.bpmn20.xml";

    public static final String ACTIVITI_XML_TEST_SERVICE_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testActivitiWorkflowServiceTask.bpmn20.xml";

    public static final String ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testHerdUserTaskWorkflow.bpmn20.xml";

    public static final String ALLOWED_ATTRIBUTE_VALUE = "Attribute_Value_1";

    public static final String ALLOWED_ATTRIBUTE_VALUE_2 = "Attribute_Value_2";

    public static final Boolean ALLOW_MISSING_DATA = true;

    public static final Boolean APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE = false;

    public static final Boolean APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_TRUE = true;

    public static final String AWS_SECURITY_GROUP_ID = "UT_AwsSecurityGroupId_" + RANDOM_SUFFIX;

    public static final String AWS_SQS_QUEUE_NAME = "AWS_SQS_QUEUE_NAME";

    public static final String BOGUS_SEARCH_FIELD = "BOGUS_SEARCH_FIELD".toLowerCase();

    public static final Boolean BOOLEAN_DEFAULT_VALUE = false;

    public static final Boolean BOOLEAN_VALUE = true;

    public static final String BUSINESS_OBJECT_DATA_KEY_AS_STRING = "UT_BusinessObjectDataKeyAsString_1_" + RANDOM_SUFFIX;

    public static final String BUSINESS_OBJECT_DATA_KEY_AS_STRING_2 = "UT_BusinessObjectDataKeyAsString_2_" + RANDOM_SUFFIX;

    public static final Integer BUSINESS_OBJECT_DATA_MAX_VERSION = 1;

    public static final String BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON = "{\n" +
        "  \"eventDate\" : \"$current_time\",\n" +
        "  \"businessObjectDataKey\" : {\n" +
        "    \"namespace\" : \"$businessObjectDataKey.namespace\",\n" +
        "    \"businessObjectDefinitionName\" : \"$businessObjectDataKey.businessObjectDefinitionName\",\n" +
        "    \"businessObjectFormatUsage\" : \"$businessObjectDataKey.businessObjectFormatUsage\",\n" +
        "    \"businessObjectFormatFileType\" : \"$businessObjectDataKey.businessObjectFormatFileType\",\n" +
        "    \"businessObjectFormatVersion\" : $businessObjectDataKey.businessObjectFormatVersion,\n" +
        "    \"partitionValue\" : \"$businessObjectDataKey.partitionValue\",\n" +
        "#if($CollectionUtils.isNotEmpty($businessObjectDataKey.subPartitionValues))    \"subPartitionValues\" : [ " +
        "\"$businessObjectDataKey.subPartitionValues.get(0)\"" +
        "#foreach ($subPartitionValue in $businessObjectDataKey.subPartitionValues.subList(1, $businessObjectDataKey.subPartitionValues.size())), \"$subPartitionValue\"" +
        "#end\n" +
        " ],\n" +
        "#end\n" +
        "    \"businessObjectDataVersion\" : $businessObjectDataKey.businessObjectDataVersion\n" +
        "  },\n" +
        "  \"newBusinessObjectDataStatus\" : \"$newBusinessObjectDataStatus\"" +
        "#if($StringUtils.isNotEmpty($oldBusinessObjectDataStatus)),\n  \"oldBusinessObjectDataStatus\" : \"$oldBusinessObjectDataStatus\"" +
        "#end\n" +
        "#if($CollectionUtils.isNotEmpty($businessObjectDataAttributes.keySet())),\n" +
        "  \"attributes\" : {\n" +
        "#set ($keys = $Collections.list($Collections.enumeration($businessObjectDataAttributes.keySet())))\n" +
        "    \"$keys.get(0)\" : \"$!businessObjectDataAttributes.get($keys.get(0))\"" +
        "#foreach($key in $keys.subList(1, $keys.size()))\n" +
        ",\n    \"$key\" : \"$!businessObjectDataAttributes.get($key)\"" +
        "#end\n" +
        "\n  }\n" +
        "#end\n" +
        "}\n";

    public static final String BUSINESS_OBJECT_DATA_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML = "<?xml version=\"1.1\" encoding=\"UTF-8\"?>\n" +
        "<datamgt:TestApplicationEvent xmlns:datamgt=\"http://testDomain/testApplication/testApplication-event\">\n" +
        "   <header>\n" +
        "      <producer>\n" +
        "         <name>testDomain/testApplication</name>\n" +
        "         <environment>$herd_notification_sqs_environment</environment>\n" +
        "      </producer>\n" +
        "      <creation>\n" +
        "         <datetime>$current_time</datetime>\n" +
        "      </creation>\n" +
        "      <correlation-id>BusinessObjectData_$businessObjectDataId</correlation-id>\n" +
        "      <context-message-type>testDomain/testApplication/BusinessObjectDataStatusChanged</context-message-type>\n" +
        "      <system-message-type>NoError</system-message-type>\n" +
        "      <xsd>http://testDomain/testApplication/testApplication-event.xsd</xsd>\n" +
        "      <event-id>\n" +
        "         <system-name>testDomain/testApplication</system-name>\n" +
        "         <system-unique-id>$uuid</system-unique-id>\n" +
        "      </event-id>\n" +
        "   </header>\n" +
        "   <payload>\n" +
        "      <eventDate>$current_time</eventDate>\n" +
        "      <datamgtEvent>\n" +
        "         <businessObjectDataStatusChanged>\n" +
        "            <businessObjectDataKey>\n" +
        "               <namespace>$businessObjectDataKey.namespace</namespace>\n" +
        "               <businessObjectDefinitionName>$businessObjectDataKey.businessObjectDefinitionName</businessObjectDefinitionName>\n" +
        "               <businessObjectFormatUsage>$businessObjectDataKey.businessObjectFormatUsage</businessObjectFormatUsage>\n" +
        "               <businessObjectFormatFileType>$businessObjectDataKey.businessObjectFormatFileType</businessObjectFormatFileType>\n" +
        "               <businessObjectFormatVersion>$businessObjectDataKey.businessObjectFormatVersion</businessObjectFormatVersion>\n" +
        "               <partitionValue>$businessObjectDataKey.partitionValue</partitionValue>\n" +
        "#if($CollectionUtils.isNotEmpty($businessObjectDataKey.subPartitionValues))               <subPartitionValues>\n" +
        "#foreach ($subPartitionValue in $businessObjectDataKey.subPartitionValues)                  <partitionValue>$subPartitionValue</partitionValue>\n" +
        "#end" +
        "               </subPartitionValues>\n" +
        "#end" +
        "               <businessObjectDataVersion>$businessObjectDataKey.businessObjectDataVersion</businessObjectDataVersion>\n" +
        "            </businessObjectDataKey>\n" +
        "            <newBusinessObjectDataStatus>$newBusinessObjectDataStatus</newBusinessObjectDataStatus>\n" +
        "#if($StringUtils.isNotEmpty($oldBusinessObjectDataStatus))            <oldBusinessObjectDataStatus>$oldBusinessObjectDataStatus</oldBusinessObjectDataStatus>\n" +
        "#end" +
        "#if($CollectionUtils.isNotEmpty($businessObjectDataAttributes.keySet()))" +
        "            <attributes>\n" +
        "#foreach($attributeName in $businessObjectDataAttributes.keySet())" +
        "                <attribute name=\"$attributeName\">$!businessObjectDataAttributes.get($attributeName)</attribute>\n" +
        "#end" +
        "            </attributes>\n" +
        "#end" +
        "         </businessObjectDataStatusChanged>\n" +
        "      </datamgtEvent>\n" +
        "   </payload>\n" +
        "   <soa-audit>\n" +
        "      <triggered-date-time>$current_time</triggered-date-time>\n" +
        "      <triggered-by-username>$username</triggered-by-username>\n" +
        "      <transmission-id>$uuid</transmission-id>\n" +
        "   </soa-audit>\n" +
        "</datamgt:TestApplicationEvent>";

    public static final String BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE = "{\n" +
        "  \"eventDate\" : \"$current_time\",\n" +
        "  \"businessObjectDefinitionDescriptionSuggestionKey\" : {\n" +
        "    \"namespace\" : \"$businessObjectDefinitionDescriptionSuggestionKey.namespace\",\n" +
        "    \"businessObjectDefinitionName\" : \"$businessObjectDefinitionDescriptionSuggestionKey.businessObjectDefinitionName\",\n" +
        "    \"userId\" : \"$businessObjectDefinitionDescriptionSuggestionKey.userId\"\n" +
        "  },\n" +
        "  \"status\" : \"$businessObjectDefinitionDescriptionSuggestion.status\",\n" +
        "  \"createdByUserId\" : \"$businessObjectDefinitionDescriptionSuggestion.createdByUserId\",\n" +
        "  \"createdOn\" : \"$businessObjectDefinitionDescriptionSuggestion.createdOn\",\n" +
        "  \"lastUpdatedByUserId\" : \"$lastUpdatedByUserId\",\n" +
        "  \"lastUpdatedOn\" : \"$lastUpdatedOn\",\n" +
        "#if($CollectionUtils.isNotEmpty($notificationList))  \"notificationList\" : [\n" +
        "    \"$notificationList.get(0)\"" +
        "#foreach ($userId in $notificationList.subList(1, $notificationList.size())),\n" +
        "    \"$userId\"" +
        "#end\n" +
        "\n  ],\n" +
        "  \"businessObjectDefinitionUri\" : \"https://udc.dev.finra.org/data-entities/${businessObjectDefinitionDescriptionSuggestionKey.namespace}/${businessObjectDefinitionDescriptionSuggestionKey.businessObjectDefinitionName}\"\n" +
        "#end\n" +
        "}\n";

    public static final String BUSINESS_OBJECT_FORMAT_KEY_AS_STRING = "UT_BusinessObjectFormatKeyAsString_" + RANDOM_SUFFIX;

    public static final String BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON = "{\n" +
        "  \"eventDate\" : \"$current_time\",\n" +
        "  \"businessObjectFormatKey\" : {\n" +
        "    \"namespace\" : \"$businessObjectFormatKey.namespace\",\n" +
        "    \"businessObjectDefinitionName\" : \"$businessObjectFormatKey.businessObjectDefinitionName\",\n" +
        "    \"businessObjectFormatUsage\" : \"$businessObjectFormatKey.businessObjectFormatUsage\",\n" +
        "    \"businessObjectFormatFileType\" : \"$businessObjectFormatKey.businessObjectFormatFileType\",\n" +
        "    \"businessObjectFormatVersion\" : $businessObjectFormatKey.businessObjectFormatVersion\n" +
        "  },\n" +
        "  \"newBusinessObjectFormatVersion\" : \"$newBusinessObjectFormatVersion\"" +
        "#if($StringUtils.isNotEmpty($oldBusinessObjectFormatVersion)),\n  \"oldBusinessObjectFormatVersion\" : \"$oldBusinessObjectFormatVersion\"" +
        "#end\n" +
        "}\n";

    public static final String BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML = "<?xml version=\"1.1\" encoding=\"UTF-8\"?>\n" +
        "<datamgt:TestApplicationEvent xmlns:datamgt=\"http://testDomain/testApplication/testApplication-event\">\n" +
        "   <header>\n" +
        "      <producer>\n" +
        "         <name>testDomain/testApplication</name>\n" +
        "         <environment>$herd_notification_sqs_environment</environment>\n" +
        "      </producer>\n" +
        "      <creation>\n" +
        "         <datetime>$current_time</datetime>\n" +
        "      </creation>\n" +
        "      <context-message-type>testDomain/testApplication/BusinessObjectFormatVersionChanged</context-message-type>\n" +
        "      <system-message-type>NoError</system-message-type>\n" +
        "      <xsd>http://testDomain/testApplication/testApplication-event.xsd</xsd>\n" +
        "      <event-id>\n" +
        "         <system-name>testDomain/testApplication</system-name>\n" +
        "         <system-unique-id>$uuid</system-unique-id>\n" +
        "      </event-id>\n" +
        "   </header>\n" +
        "   <payload>\n" +
        "      <eventDate>$current_time</eventDate>\n" +
        "      <datamgtEvent>\n" +
        "         <businessObjectFormatVersionChanged>\n" +
        "            <businessObjectFormatKey>\n" +
        "               <namespace>$businessObjectFormatKey.namespace</namespace>\n" +
        "               <businessObjectDefinitionName>$businessObjectFormatKey.businessObjectDefinitionName</businessObjectDefinitionName>\n" +
        "               <businessObjectFormatUsage>$businessObjectFormatKey.businessObjectFormatUsage</businessObjectFormatUsage>\n" +
        "               <businessObjectFormatFileType>$businessObjectFormatKey.businessObjectFormatFileType</businessObjectFormatFileType>\n" +
        "               <businessObjectFormatVersion>$businessObjectFormatKey.businessObjectFormatVersion</businessObjectFormatVersion>\n" +
        "            </businessObjectFormatKey>\n" +
        "            <newBusinessObjectFormatVersion>$newBusinessObjectFormatVersion</newBusinessObjectFormatVersion>\n" +
        "#if($StringUtils.isNotEmpty($oldBusinessObjectFormatVersion))            <oldBusinessObjectFormatVersion>$oldBusinessObjectFormatVersion</oldBusinessObjectFormatVersion>\n" +
        "#end" +
        "         </businessObjectFormatVersionChanged>\n" +
        "      </datamgtEvent>\n" +
        "   </payload>\n" +
        "   <soa-audit>\n" +
        "      <triggered-date-time>$current_time</triggered-date-time>\n" +
        "      <triggered-by-username>$username</triggered-by-username>\n" +
        "      <transmission-id>$uuid</transmission-id>\n" +
        "   </soa-audit>\n" +
        "</datamgt:TestApplicationEvent>";

    public static final Boolean CONTINUE_ON_ERROR = true;

    public static final Boolean CREATE_NEW_VERSION = true;

    public static final Boolean DELETE_FILES = true;

    public static final String DIRECTORY_PATH = "UT_Directory_Path/Some_Path_1/" + RANDOM_SUFFIX + "/";

    public static final String DIRECTORY_PATH_2 = "UT_Directory_Path/Some_Path_2/" + RANDOM_SUFFIX + "/";

    public static final Boolean DISCOVER_STORAGE_FILES = true;

    public static final Boolean DRY_RUN = true;

    public static final String EC2_PRICING_LIST_URL = "UT_Ec2PricingListUrl_" + RANDOM_SUFFIX;

    public static final String EC2_PRODUCT_KEY = "UT_EC2_ProductKey_1_" + RANDOM_SUFFIX;

    public static final String EC2_PRODUCT_KEY_2 = "UT_EC2_ProductKey_2_" + RANDOM_SUFFIX;

    public static final String EMR_CLUSTER_ID = "UT_EMR_Cluster_ID_" + RANDOM_SUFFIX;

    public static final String EMR_CLUSTER_NAME = "UT_EMR_Cluster_Name_" + RANDOM_SUFFIX;

    public static final Boolean EMR_CLUSTER_VERBOSE_FLAG = true;

    public static final String EMR_STEP_ID = "UT_EMR_Step_ID_" + RANDOM_SUFFIX;

    public static final String EMR_STEP_JAR_LOCATION = "UT_EMR_Step_JAR_Location_" + RANDOM_SUFFIX;

    public static final String EMR_STEP_MAIN_CLASS = "UT_EMR_Step_MainClass_" + RANDOM_SUFFIX;

    public static final String EMR_STEP_NAME = "UT_EMR_Step_Name_" + RANDOM_SUFFIX;

    public static final String EMR_STEP_SCRIPT_LOCATION = "UT_EMR_Step_Script_Location_" + RANDOM_SUFFIX;

    public static final String END_PARTITION_VALUE = "2014-04-08";

    public static final DateTime END_TIME = getRandomDateTime();

    public static final String ERROR_MESSAGE = "UT_ErrorMessage_" + RANDOM_SUFFIX;

    public static final Boolean EXCLUSION_SEARCH_FILTER = true;

    public static final int EXPECTED_UUID_SIZE = 36;

    /**
     * Constant to hold the data provider name option for the business object definition search
     */
    public static final String FIELD_DATA_PROVIDER_NAME = "dataProviderName";

    /**
     * Constant to hold the display name option for the business object definition search
     */
    public static final String FIELD_DISPLAY_NAME = "displayName";

    /**
     * Constant to hold the short description option for the business object definition search
     */
    public static final String FIELD_SHORT_DESCRIPTION = "shortDescription";

    public static final String FILE_NAME = "UT_FileName_1_" + RANDOM_SUFFIX;

    public static final String FILE_NAME_2 = "UT_FileName_2_" + RANDOM_SUFFIX;

    public static final String FILE_NAME_3 = "UT_FileName_3_" + RANDOM_SUFFIX;

    public static final Long FILE_SIZE = (long) (Math.random() * Long.MAX_VALUE);

    public static final Long FILE_SIZE_2 = (long) (Math.random() * Long.MAX_VALUE);

    public static final Boolean FILTER_ON_LATEST_VALID_VERSION = true;

    public static final String HERD_OUTGOING_QUEUE = "HERD_OUTGOING_QUEUE";

    public static final String HERD_WORKFLOW_ENVIRONMENT = "herd_workflowEnvironment";

    public static final Integer ID = (int) (Math.random() * Integer.MAX_VALUE);

    public static final Integer ID_2 = (int) (Math.random() * Integer.MAX_VALUE);

    public static final Boolean INCLUDE_ALL_REGISTERED_SUBPARTITIONS = true;

    public static final Boolean INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY = true;

    public static final Boolean INCLUDE_DROP_PARTITIONS = true;

    public static final Boolean INCLUDE_DROP_TABLE_STATEMENT = true;

    public static final Boolean INCLUDE_IF_NOT_EXISTS_OPTION = true;

    public static final Boolean INCLUDE_STORAGE_UNIT_STATUS_HISTORY = true;

    public static final String INDEX_SEARCH_RESULT_TYPE = "UT_IndexSearchResultType" + RANDOM_SUFFIX;

    /**
     * Constant to hold the column match option for the index search
     */
    public static final String MATCH_COLUMN = "column";

    public static final Long MAX_RESULTS_PER_PAGE = getRandomLong();

    /**
     * Message header keys notification message builder testing
     */
    public static final String MESSAGE_HEADER_KEY_ENVIRONMENT = "environment";

    public static final String MESSAGE_HEADER_KEY_MESSAGE_ID = "messageId";

    public static final String MESSAGE_HEADER_KEY_MESSAGE_TYPE = "messageType";

    public static final String MESSAGE_HEADER_KEY_MESSAGE_VERSION = "messageVersion";

    public static final String MESSAGE_HEADER_KEY_NAMESPACE = "namespace";

    public static final String MESSAGE_HEADER_KEY_SOURCE_SYSTEM = "sourceSystem";

    public static final String MESSAGE_HEADER_KEY_USER_ID = "userId";

    public static final String MESSAGE_VERSION = "UT_MessageVersion" + RANDOM_SUFFIX;

    public static final String METHOD_NAME = "UT_MethodName_1_" + RANDOM_SUFFIX;

    public static final String METHOD_NAME_2 = "UT_MethodName_2_" + RANDOM_SUFFIX;

    public static final String NEGATIVE_COLUMN_SIZE = "-1" + RANDOM_SUFFIX;

    public static final String NO_ACTIVITI_JOB_NAME = null;

    public static final JobStatusEnum NO_ACTIVITI_JOB_STATUS = null;

    public static final Boolean NO_ALLOW_MISSING_DATA = false;

    public static final List<BusinessObjectDataStatus> NO_AVAILABLE_STATUSES = new ArrayList<>();

    public static final Boolean NO_BOOLEAN_DEFAULT_VALUE = null;

    public static final List<BusinessObjectDataKey> NO_BUSINESS_OBJECT_DATA_CHILDREN = new ArrayList<>();

    public static final List<BusinessObjectDataKey> NO_BUSINESS_OBJECT_DATA_PARENTS = new ArrayList<>();

    public static final List<BusinessObjectDataStatus> NO_BUSINESS_OBJECT_DATA_STATUSES = new ArrayList<>();

    public static final List<BusinessObjectDataStatusChangeEvent> NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY = null;

    public static final List<BusinessObjectDefinitionChangeEvent> NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS = new ArrayList<>();

    public static final List<BusinessObjectDefinitionColumnChangeEvent> NO_BUSINESS_OBJECT_DEFINITION_COLUMN_CHANGE_EVENTS = new ArrayList<>();

    public static final List<BusinessObjectFormatKey> NO_BUSINESS_OBJECT_FORMAT_CHILDREN = null;

    public static final List<BusinessObjectFormatKey> NO_BUSINESS_OBJECT_FORMAT_PARENTS = null;

    public static final String NO_COLUMN_DEFAULT_VALUE = null;

    public static final String NO_COLUMN_DESCRIPTION = null;

    public static final Boolean NO_COLUMN_REQUIRED = false;

    public static final String NO_COLUMN_SIZE = null;

    public static final Boolean NO_CREATE_NEW_VERSION = false;

    public static final Boolean NO_DELETE_FILES = false;

    public static final DescriptiveBusinessObjectFormat NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT = null;

    public static final DescriptiveBusinessObjectFormatUpdateRequest NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST = null;

    public static final Boolean NO_DISCOVER_STORAGE_FILES = false;

    public static final Boolean NO_DRY_RUN = false;

    public static final DateTime NO_END_TIME = null;

    public static final RuntimeException NO_EXCEPTION = null;

    public static final Boolean NO_EXCLUSION_SEARCH_FILTER = false;

    public static final Long NO_FILE_SIZE = null;

    public static final Integer NO_ID = null;

    public static final Boolean NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS = false;

    public static final Boolean NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY = false;

    public static final Boolean NO_INCLUDE_DROP_PARTITIONS = false;

    public static final Boolean NO_INCLUDE_DROP_TABLE_STATEMENT = false;

    public static final Boolean NO_INCLUDE_IF_NOT_EXISTS_OPTION = false;

    public static final Boolean NO_INCLUDE_STORAGE_UNIT_STATUS_HISTORY = false;

    public static final Boolean NO_INCLUDE_TAG_HIERARCHY = false;

    public static final LatestAfterPartitionValue NO_LATEST_AFTER_PARTITION_VALUE = null;

    public static final LatestBeforePartitionValue NO_LATEST_BEFORE_PARTITION_VALUE = null;

    public static final List<BusinessObjectDataStatus> NO_NOT_AVAILABLE_STATUSES = new ArrayList<>();

    public static final String NO_OLD_BUSINESS_OBJECT_FORMAT_VERSION = null;

    public static final TagKey NO_PARENT_TAG_KEY = null;

    public static final List<String> NO_PARTITION_VALUES = null;

    public static final PartitionValueRange NO_PARTITION_VALUE_RANGE = null;

    public static final boolean NO_PERFORM_FULL_SEARCH_INDEX_VALIDATION = Boolean.FALSE;

    public static final Boolean NO_RECORD_FLAG_SET = false;

    public static final XMLGregorianCalendar NO_RETENTION_EXPIRATION_DATE = null;

    public static final Integer NO_RETENTION_PERIOD_IN_DAYS = null;

    public static final String NO_RETENTION_TYPE = null;

    public static final Boolean NO_ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET = false;

    public static final Long NO_ROW_COUNT = null;

    public static final SearchIndexStatistics NO_SEARCH_INDEX_STATISTICS = null;

    public static final XMLGregorianCalendar NO_SEARCH_INDEX_STATISTICS_CREATION_DATE = null;

    public static final Set<String> NO_SEARCH_RESPONSE_FIELDS = new HashSet<>();

    public static final List<String> NO_SECURITY_FUNCTIONS = null;

    public static final List<String> NO_SECURITY_ROLES = null;

    public static final String NO_SKU = null;

    public static final PartitionValueFilter NO_STANDALONE_PARTITION_VALUE_FILTER = null;

    public static final DateTime NO_START_TIME = null;

    public static final StorageDirectory NO_STORAGE_DIRECTORY = null;

    public static final List<StorageFile> NO_STORAGE_FILES = new ArrayList<>();

    public static final Integer NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS = null;

    public static final List<StorageUnit> NO_STORAGE_UNITS = new ArrayList<>();

    public static final List<StorageUnitStatusChangeEvent> NO_STORAGE_UNIT_STATUS_HISTORY = null;

    public static final Boolean NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS = false;

    public static final XMLGregorianCalendar NO_UPDATED_TIME = null;

    public static final String NO_USER_ID = null;

    public static final Boolean NO_VARIABLE_REQUIRED = false;

    public static final Boolean OVERRIDE_TERMINATION_PROTECTION = true;

    public static final Long PAGE_COUNT = getRandomLong();

    public static final Integer PAGE_NUMBER_ONE = 1;

    public static final Integer PAGE_SIZE_ONE_THOUSAND = 1_000;

    public static final String PARAMETER_NAME = "UT_ParameterName_" + RANDOM_SUFFIX;

    public static final String PARAMETER_VALUE = "UT_ParameterValue_" + RANDOM_SUFFIX;

    public static final boolean PERFORM_FULL_SEARCH_INDEX_VALIDATION = Boolean.TRUE;

    public static final List<String> PROCESS_DATE_AVAILABLE_PARTITION_VALUES = Arrays.asList("2014-04-02", "2014-04-03", "2014-04-08");

    public static final List<String> PROCESS_DATE_NOT_AVAILABLE_PARTITION_VALUES = Arrays.asList("2014-04-04", "2014-04-07");

    public static final List<String> PROCESS_DATE_PARTITION_VALUES = Arrays.asList("2014-04-02", "2014-04-03", "2014-04-04", "2014-04-07", "2014-04-08");

    public static final Boolean RECORD_FLAG_SET = true;

    public static final Boolean ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET = true;

    public static final String RELATIONAL_SCHEMA_NAME = "UT_RelationalSchemaName_" + RANDOM_SUFFIX;

    public static final String RELATIONAL_TABLE_NAME = "UT_RelationalTableName_" + RANDOM_SUFFIX;

    public static final XMLGregorianCalendar RETENTION_EXPIRATION_DATE = HerdDateUtils.getXMLGregorianCalendarValue(getRandomDate());

    public static final Boolean RETRIEVE_INSTANCE_FLEETS = true;

    public static final Long ROW_COUNT = (long) (Math.random() * Long.MAX_VALUE);

    public static final Long ROW_COUNT_2 = (long) (Math.random() * Long.MAX_VALUE);

    public static final String ROW_FORMAT = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'";

    public static final String S3_ARCHIVE_TO_GLACIER_TAG_KEY = "UT_S3_Archive_To_Glacier_Tag_Key_" + RANDOM_SUFFIX;

    public static final String S3_ARCHIVE_TO_GLACIER_TAG_VALUE = "UT_S3_Archive_To_Glacier_Tag_Value_" + RANDOM_SUFFIX;

    public static final String S3_KEY_PREFIX_VELOCITY_TEMPLATE =
        "$namespace/$dataProviderName/$businessObjectFormatUsage/$businessObjectFormatFileType/$businessObjectDefinitionName" +
            "/schm-v$businessObjectFormatVersion/data-v$businessObjectDataVersion/$businessObjectFormatPartitionKey=$businessObjectDataPartitionValue" +
            "#if($CollectionUtils.isNotEmpty($businessObjectDataSubPartitions.keySet()))" +
            "#foreach($subPartitionKey in $businessObjectDataSubPartitions.keySet())/$subPartitionKey=$businessObjectDataSubPartitions.get($subPartitionKey)" +
            "#end" +
            "#end";

    public static final XMLGregorianCalendar SEARCH_INDEX_STATISTICS_CREATION_DATE = HerdDateUtils.getXMLGregorianCalendarValue(getRandomDate());

    public static final String SEARCH_INDEX_STATISTICS_INDEX_UUID = "UT_SearchIndexSetting_Uuid_" + RANDOM_SUFFIX;

    public static final Long SEARCH_INDEX_STATISTICS_NUMBER_OF_ACTIVE_DOCUMENTS = (long) (Math.random() * Integer.MAX_VALUE);

    public static final Long SEARCH_INDEX_STATISTICS_NUMBER_OF_DELETED_DOCUMENTS = (long) (Math.random() * Integer.MAX_VALUE);

    public static final String SECOND_PARTITION_COLUMN_NAME = "PRTN_CLMN002";

    /**
     * The length of a business object definition short description
     */
    public static final int SHORT_DESCRIPTION_LENGTH = 300;

    public static final String SKU = "UT_SKU_Value_" + RANDOM_SUFFIX;

    public static final String SOURCE_SYSTEM = "UT_SourceSystem" + RANDOM_SUFFIX;

    public static final String START_PARTITION_VALUE = "2014-04-02";

    public static final DateTime START_TIME = getRandomDateTime();

    public static final String STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME = "STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME";

    public static final Integer STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS = getRandomInteger();

    public static final String STORAGE_UNIT_STATUS_CHANGE_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_JSON =
        "{\n" + "  \"eventDate\" : \"$current_time\",\n" + "  \"businessObjectDataKey\" : {\n" + "    \"namespace\" : \"$businessObjectDataKey.namespace\",\n" +
            "    \"businessObjectDefinitionName\" : \"$businessObjectDataKey.businessObjectDefinitionName\",\n" +
            "    \"businessObjectFormatUsage\" : \"$businessObjectDataKey.businessObjectFormatUsage\",\n" +
            "    \"businessObjectFormatFileType\" : \"$businessObjectDataKey.businessObjectFormatFileType\",\n" +
            "    \"businessObjectFormatVersion\" : $businessObjectDataKey.businessObjectFormatVersion,\n" +
            "    \"partitionValue\" : \"$businessObjectDataKey.partitionValue\",\n" +
            "#if($CollectionUtils.isNotEmpty($businessObjectDataKey.subPartitionValues))    \"subPartitionValues\" : [ " +
            "\"$businessObjectDataKey.subPartitionValues.get(0)\"" +
            "#foreach ($subPartitionValue in $businessObjectDataKey.subPartitionValues.subList(1, $businessObjectDataKey.subPartitionValues.size())), \"$subPartitionValue\"" +
            "#end\n" + " ],\n" + "#end\n" + "    \"businessObjectDataVersion\" : $businessObjectDataKey.businessObjectDataVersion\n" + "  },\n" +
            "  \"storageName\" : \"$storageName\",\n" + "  \"newStorageUnitStatus\" : \"$newStorageUnitStatus\"" +
            "#if($StringUtils.isNotEmpty($oldStorageUnitStatus)),\n  \"oldStorageUnitStatus\" : \"$oldStorageUnitStatus\"" + "#end\n" + "}\n";

    public static final Boolean SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS = true;

    public static final String SYSTEM_MONITOR_NOTIFICATION_MESSAGE_VELOCITY_TEMPLATE_XML = "<?xml version=\"1.1\" encoding=\"UTF-8\"?>\n" +
        "<datamgt:monitor xmlns:datamgt=\"http://testDomain/system-monitor\">\n" +
        "   <header>\n" +
        "      <producer>\n" +
        "         <name>testDomain/testApplication</name>\n" +
        "         <environment>$herd_notification_sqs_environment</environment>\n" +
        "      </producer>\n" +
        "      <creation>\n" +
        "         <datetime>$current_time</datetime>\n" +
        "      </creation>\n" +
        "#if($StringUtils.isNotEmpty($incoming_message_correlation_id))      <correlation-id>$incoming_message_correlation_id</correlation-id>\n" +
        "#end\n" +
        "      <context-message-type>$incoming_message_context_message_type</context-message-type>\n" +
        "      <system-message-type>NoError</system-message-type>\n" +
        "      <xsd>http://testDomain/system-monitor.xsd</xsd>\n" +
        "   </header>\n" +
        "   <payload>\n" +
        "      <contextMessageTypeToPublish />\n" +
        "   </payload>\n" +
        "</datamgt:monitor>";

    /**
     * The test job name as per the above workflow XML file.
     */
    public static final String TEST_ACTIVITI_JOB_NAME = "testHerdWorkflow";

    /**
     * The test namespace code as per the above workflow XML file.
     */
    public static final String TEST_ACTIVITI_NAMESPACE_CD = "testNamespace";

    /**
     * This is the test Activiti workflow Id which is the test app name + "." + the test activity job name.
     */
    public static final String TEST_ACTIVITY_WORKFLOW_ID = TEST_ACTIVITI_NAMESPACE_CD + "." + TEST_ACTIVITI_JOB_NAME;

    public static final String TEST_SQS_CONTEXT_MESSAGE_TYPE_TO_PUBLISH = "testContextMessageTypeToPublish";

    public static final String TEST_SQS_ENVIRONMENT = "testEnvironment";

    public static final String TEST_SQS_MESSAGE_CORRELATION_ID = "testCorrelationId";

    public static final Long TOTAL_RECORDS_ON_PAGE = getRandomLong();

    public static final Long TOTAL_RECORD_COUNT = getRandomLong();

    public static final String UUID_VALUE = "UT_UUID_Value_" + RANDOM_SUFFIX;

    public static final String VARIABLE_NAME = "UT_Variable_Name_" + RANDOM_SUFFIX;

    public static final Boolean VARIABLE_REQUIRED = true;

    public static final Boolean VERBOSE = true;

    public static final String ZERO_COLUMN_SIZE = "0";

    @Autowired
    protected SpringProcessEngineConfiguration activitiConfiguration;

    @Autowired
    protected ActivitiHelper activitiHelper;

    @Autowired
    protected HistoryService activitiHistoryService;

    @Autowired
    protected ManagementService activitiManagementService;

    @Autowired
    protected SpringProcessEngineConfiguration activitiProcessEngineConfiguration;

    @Autowired
    protected RepositoryService activitiRepositoryService;

    @Autowired
    protected ActivitiRuntimeHelper activitiRuntimeHelper;

    @Autowired
    protected RuntimeService activitiRuntimeService;

    @Autowired
    protected ActivitiService activitiService;

    @Autowired
    protected TaskService activitiTaskService;

    @Autowired
    protected AttributeValueListService attributeValueListService;

    @Autowired
    protected AwsHelper awsHelper;

    @Autowired
    protected BusinessObjectDataAttributeDaoHelper businessObjectDataAttributeDaoHelper;

    @Autowired
    protected BusinessObjectDataAttributeHelper businessObjectDataAttributeHelper;

    @Autowired
    protected BusinessObjectDataAttributeService businessObjectDataAttributeService;

    @Autowired
    protected BusinessObjectDataAttributeServiceTestHelper businessObjectDataAttributeServiceTestHelper;

    @Autowired
    protected BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    protected BusinessObjectDataFinalizeRestoreHelperService businessObjectDataFinalizeRestoreHelperService;

    @Autowired
    protected BusinessObjectDataFinalizeRestoreService businessObjectDataFinalizeRestoreService;

    @Autowired
    protected BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    protected BusinessObjectDataInitiateRestoreHelperService businessObjectDataInitiateRestoreHelperService;

    @Autowired
    protected BusinessObjectDataInvalidateUnregisteredHelper businessObjectDataInvalidateUnregisteredHelper;

    @Autowired
    protected BusinessObjectDataNotificationRegistrationService businessObjectDataNotificationRegistrationService;

    @Autowired
    protected BusinessObjectDataRetryStoragePolicyTransitionHelper businessObjectDataRetryStoragePolicyTransitionHelper;

    @Autowired
    protected BusinessObjectDataSearchHelper businessObjectDataSearchHelper;

    @Autowired
    protected BusinessObjectDataService businessObjectDataService;

    @Autowired
    protected BusinessObjectDataServiceTestHelper businessObjectDataServiceTestHelper;

    @Autowired
    protected BusinessObjectDataStatusService businessObjectDataStatusService;

    @Autowired
    protected BusinessObjectDataStorageFileService businessObjectDataStorageFileService;

    @Autowired
    protected BusinessObjectDataStorageUnitStatusService businessObjectDataStorageUnitStatusService;

    @Autowired
    protected BusinessObjectDefinitionColumnDaoHelper businessObjectDefinitionColumnDaoHelper;

    @Autowired
    protected BusinessObjectDefinitionColumnService businessObjectDefinitionColumnService;

    @Autowired
    protected BusinessObjectDefinitionService businessObjectDefinitionService;

    @Autowired
    protected BusinessObjectDefinitionServiceTestHelper businessObjectDefinitionServiceTestHelper;

    @Autowired
    protected BusinessObjectDefinitionSubjectMatterExpertService businessObjectDefinitionSubjectMatterExpertService;

    @Autowired
    protected BusinessObjectDefinitionTagService businessObjectDefinitionTagService;

    @Autowired
    protected BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    protected BusinessObjectFormatService businessObjectFormatService;

    @Autowired
    protected BusinessObjectFormatServiceTestHelper businessObjectFormatServiceTestHelper;

    @Autowired
    protected CleanupDestroyedBusinessObjectDataService cleanupDestroyedBusinessObjectDataService;

    @Autowired
    protected CurrentUserService currentUserService;

    @Autowired
    protected CustomDdlService customDdlService;

    @Autowired
    protected CustomDdlServiceTestHelper customDdlServiceTestHelper;

    @Autowired
    protected DataProviderService dataProviderService;

    @Autowired
    protected EmrClusterDefinitionHelper emrClusterDefinitionHelper;

    @Autowired
    protected EmrClusterDefinitionService emrClusterDefinitionService;

    @Autowired
    protected EmrHelper emrHelper;

    @Autowired
    protected EmrService emrService;

    @Autowired
    protected EmrStepHelperFactory emrStepHelperFactory;

    @Autowired
    protected ExecuteJdbcTestHelper executeJdbcTestHelper;

    @Autowired
    protected ExpectedPartitionValueService expectedPartitionValueService;

    @Autowired
    protected ExpectedPartitionValueServiceTestHelper expectedPartitionValueServiceTestHelper;

    @Autowired
    protected FileTypeService fileTypeService;

    @Autowired
    protected FileUploadCleanupService fileUploadCleanupService;

    @Autowired
    protected HerdCommandInvoker herdCommandInvoker;

    @Autowired
    protected HerdStringHelper herdStringHelper;

    @Autowired
    protected Hive13DdlGenerator hive13DdlGenerator;

    @Autowired
    protected JdbcService jdbcService;

    @Autowired
    protected JobDefinitionHelper jobDefinitionHelper;

    @Autowired
    protected JobDefinitionService jobDefinitionService;

    @Autowired
    protected JobDefinitionServiceTestHelper jobDefinitionServiceTestHelper;

    @Autowired
    protected JobService jobService;

    @Autowired
    protected JobServiceTestHelper jobServiceTestHelper;

    @Autowired
    protected JsonHelper jsonHelper;

    @Autowired
    protected MessageNotificationEventService messageNotificationEventService;

    @Autowired
    protected MessageTypeDaoHelper messageTypeDaoHelper;

    @Autowired
    protected NamespaceService namespaceService;

    @Autowired
    protected NamespaceServiceTestHelper namespaceServiceTestHelper;

    @Autowired
    protected NotificationActionFactory notificationActionFactory;

    @Autowired
    protected NotificationEventService notificationEventService;

    @Autowired
    protected NotificationMessagePublishingService notificationMessagePublishingService;

    @Autowired
    protected NotificationRegistrationDaoHelper notificationRegistrationDaoHelper;

    @Autowired
    protected NotificationRegistrationServiceTestHelper notificationRegistrationServiceTestHelper;

    @Autowired
    protected NotificationRegistrationStatusDaoHelper notificationRegistrationStatusDaoHelper;

    @Autowired
    protected NotificationRegistrationStatusService notificationRegistrationStatusService;

    @Autowired
    protected PartitionKeyGroupService partitionKeyGroupService;

    @Autowired
    protected PartitionKeyGroupServiceTestHelper partitionKeyGroupServiceTestHelper;

    @Autowired
    protected RelationalTableRegistrationHelperService relationalTableRegistrationHelperService;

    @Autowired
    protected RelationalTableRegistrationService relationalTableRegistrationService;

    @Autowired
    protected RelationalTableRegistrationServiceTestHelper relationalTableRegistrationServiceTestHelper;

    @Autowired
    protected S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    protected S3PropertiesLocationHelper s3PropertiesLocationHelper;

    @Autowired
    protected S3Service s3Service;

    @Autowired
    protected SecurityRoleService securityRoleService;

    @Autowired
    protected SearchIndexDaoHelper searchIndexDaoHelper;

    @Autowired
    protected SearchIndexStatusDaoHelper searchIndexStatusDaoHelper;

    @Autowired
    protected SearchIndexTypeDaoHelper searchIndexTypeDaoHelper;

    @Autowired
    protected NotificationMessageBuilder sqsMessageBuilder;

    @Autowired
    protected StorageDaoHelper storageDaoHelper;

    @Autowired
    protected StorageFileHelper storageFileHelper;

    @Autowired
    protected StorageHelper storageHelper;

    @Autowired
    protected StoragePlatformService storagePlatformService;

    @Autowired
    protected StoragePolicyProcessorHelperService storagePolicyProcessorHelperService;

    @Autowired
    protected StoragePolicyProcessorService storagePolicyProcessorService;

    @Autowired
    protected StoragePolicySelectorService storagePolicySelectorService;

    @Autowired
    protected StoragePolicyService storagePolicyService;

    @Autowired
    protected StoragePolicyServiceTestHelper storagePolicyServiceTestHelper;

    @Autowired
    protected StorageService storageService;

    @Autowired
    protected StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    protected StorageUnitHelper storageUnitHelper;

    @Autowired
    protected StorageUnitNotificationRegistrationService storageUnitNotificationRegistrationService;

    @Autowired
    protected StorageUnitService storageUnitService;

    @Autowired
    protected StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Autowired
    protected SubjectMatterExpertService subjectMatterExpertService;

    @Autowired
    protected SystemJobService systemJobService;

    @Autowired
    protected TagDaoTestHelper tagDaoTestHelper;

    @Autowired
    protected TagService tagService;

    @Autowired
    protected TagTypeDaoTestHelper tagTypeDaoTestHelper;

    @Autowired
    protected TagTypeService tagTypeService;

    @Autowired
    protected UploadDownloadHelperService uploadDownloadHelperService;

    @Autowired
    protected UploadDownloadService uploadDownloadService;

    @Autowired
    protected UploadDownloadServiceTestHelper uploadDownloadServiceTestHelper;

    @Autowired
    protected UserNamespaceAuthorizationService userNamespaceAuthorizationService;

    @Autowired
    protected VelocityHelper velocityHelper;

    @Autowired
    protected XmlHelper xmlHelper;

    /**
     * Returns S3 key prefix constructed according to the S3 Naming Convention Wiki page.
     *
     * @param businessObjectDataKey the business object data key
     * @param dataProviderName the data provider name
     * @param partitionKey the format partition key
     * @param subPartitionKeys the list of subpartition keys for the business object data
     *
     * @return the S3 key prefix constructed according to the S3 Naming Convention
     */
    public static String getExpectedS3KeyPrefix(BusinessObjectDataKey businessObjectDataKey, String dataProviderName, String partitionKey,
        SchemaColumn[] subPartitionKeys)
    {
        return getExpectedS3KeyPrefix(businessObjectDataKey.getNamespace(), dataProviderName, businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), partitionKey, businessObjectDataKey.getPartitionValue(), subPartitionKeys,
            businessObjectDataKey.getSubPartitionValues().toArray(new String[businessObjectDataKey.getSubPartitionValues().size()]),
            businessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Returns S3 key prefix constructed according to the S3 Naming Convention Wiki page.
     *
     * @param namespaceCd the namespace code
     * @param dataProviderName the data provider name
     * @param businessObjectDefinitionName the business object definition name
     * @param formatUsage the format usage
     * @param formatFileType the format file type
     * @param businessObjectFormatVersion the format version
     * @param partitionKey the format partition key
     * @param partitionValue the business object data partition value
     * @param subPartitionKeys the list of subpartition keys for the business object data
     * @param subPartitionValues the list of subpartition values for the business object data
     * @param businessObjectDataVersion the business object data version
     *
     * @return the S3 key prefix constructed according to the S3 Naming Convention
     */
    public static String getExpectedS3KeyPrefix(String namespaceCd, String dataProviderName, String businessObjectDefinitionName, String formatUsage,
        String formatFileType, Integer businessObjectFormatVersion, String partitionKey, String partitionValue, SchemaColumn[] subPartitionKeys,
        String[] subPartitionValues, Integer businessObjectDataVersion)
    {
        StringBuilder s3KeyPrefix = new StringBuilder(String
            .format("%s/%s/%s/%s/%s/schm-v%d/data-v%d/%s=%s", namespaceCd.trim().toLowerCase().replace('_', '-'),
                dataProviderName.trim().toLowerCase().replace('_', '-'), formatUsage.trim().toLowerCase().replace('_', '-'),
                formatFileType.trim().toLowerCase().replace('_', '-'), businessObjectDefinitionName.trim().toLowerCase().replace('_', '-'),
                businessObjectFormatVersion, businessObjectDataVersion, partitionKey.trim().toLowerCase().replace('_', '-'), partitionValue.trim()));

        if (subPartitionKeys != null)
        {
            for (int i = 0; i < subPartitionKeys.length; i++)
            {
                s3KeyPrefix.append("/").append(subPartitionKeys[i].getName().trim().toLowerCase().replace('_', '-')).append("=").append(subPartitionValues[i]);
            }
        }

        return s3KeyPrefix.toString();
    }

    @After
    public void after()
    {
        SecurityContextHolder.clearContext();
    }

    /**
     * Returns a copy of the string, with a trailing slash character added.
     *
     * @param string the string that we want to add trailing slash character to
     *
     * @return the string with a trailing slash added
     */
    protected String addSlash(String string)
    {
        return String.format("%s/", string);
    }

    /**
     * Returns a copy of the string, with some leading and trailing whitespace added.
     *
     * @param string the string that we want to add leading and trailing whitespace to
     *
     * @return the string with leading and trailing whitespace added
     */
    protected String addWhitespace(String string)
    {
        return String.format("  %s    ", string);
    }

    /**
     * Adds leading and trailing whitespace characters to all members in this list.
     *
     * @param list the list of string values
     *
     * @return the list of string values with leading and trailing whitespace characters
     */
    protected List<String> addWhitespace(List<String> list)
    {
        List<String> whitespaceList = new ArrayList<>();

        for (String value : list)
        {
            whitespaceList.add(addWhitespace(value));
        }

        return whitespaceList;
    }

    /**
     * Adds whitespace characters to the relative fields of the business object format schema.
     *
     * @param schema the business object format schema
     *
     * @return the business object format schema with the relative fields having leading and trailing whitespace added
     */
    protected Schema addWhitespace(Schema schema)
    {
        // Add whitespace to the partition key group field.
        schema.setPartitionKeyGroup(addWhitespace(schema.getPartitionKeyGroup()));

        // Add whitespace characters to the relative schema column fields.
        List<SchemaColumn> allSchemaColumns = new ArrayList<>();
        allSchemaColumns.addAll(schema.getColumns());
        allSchemaColumns.addAll(schema.getPartitions());

        for (SchemaColumn schemaColumn : allSchemaColumns)
        {
            schemaColumn.setName(addWhitespace(schemaColumn.getName()));
            schemaColumn.setType(addWhitespace(schemaColumn.getType()));
            schemaColumn.setSize(schemaColumn.getSize() == null ? null : addWhitespace(schemaColumn.getSize()));
            schemaColumn.setDefaultValue(schemaColumn.getDefaultValue() == null ? null : addWhitespace(schemaColumn.getDefaultValue()));
        }

        return schema;
    }

    /**
     * Converts all of the members in this list to lower case.
     *
     * @param list the list of string values
     *
     * @return the list of lower case strings
     */
    protected List<String> convertListToLowerCase(List<String> list)
    {
        List<String> lowerCaseList = new ArrayList<>();

        for (String value : list)
        {
            lowerCaseList.add(value.toLowerCase());
        }

        return lowerCaseList;
    }

    /**
     * Converts all of the members in this list to upper case.
     *
     * @param list the list of string values
     *
     * @return the list of upper case strings
     */
    protected List<String> convertListToUpperCase(List<String> list)
    {
        List<String> upperCaseList = new ArrayList<>();

        for (String value : list)
        {
            upperCaseList.add(value.toUpperCase());
        }

        return upperCaseList;
    }

    /**
     * Deletes all deployments in the database and any associated tables.
     */
    protected void deleteActivitiDeployments()
    {
        for (Deployment deployment : activitiRepositoryService.createDeploymentQuery().list())
        {
            activitiRepositoryService.deleteDeployment(deployment.getId(), true);
        }
    }

    /**
     * Deletes all Activiti jobs from the history table.
     */
    protected void deleteAllHistoricJobs()
    {
        HistoricProcessInstanceQuery query = activitiHistoryService.createHistoricProcessInstanceQuery();
        List<HistoricProcessInstance> historicProcessInstances = query.list();
        for (HistoricProcessInstance historicProcessInstance : historicProcessInstances)
        {
            activitiHistoryService.deleteHistoricProcessInstance(historicProcessInstance.getId());
        }
    }

    /**
     * Gets the Activiti XML from the specified BPMN model. The returned XML will have CDATA wrappers removed.
     *
     * @param bpmnModel the model.
     *
     * @return the Activiti XML.
     */
    protected String getActivitiXmlFromBpmnModel(BpmnModel bpmnModel)
    {
        return new String(new BpmnXMLConverter().convertToXML(bpmnModel)).replace("<![CDATA[", "").replaceAll("]]>", "");
    }

    /**
     * Generates the BpmnModel for the given Activiti xml resource.
     *
     * @param activitiXmlResource the classpath location of Activiti Xml
     *
     * @return BpmnModel the constructed model
     * @throws Exception if any exception occurs in creation
     */
    protected BpmnModel getBpmnModelForXmlResource(String activitiXmlResource) throws Exception
    {
        String activitiXml = IOUtils.toString(resourceLoader.getResource(activitiXmlResource).getInputStream());

        BpmnModel bpmnModel;
        try
        {
            bpmnModel = activitiHelper.constructBpmnModelFromXmlAndValidate(activitiXml);
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException("Error processing Activiti XML: " + ex.getMessage(), ex);
        }
        return bpmnModel;
    }

    /**
     * Gets a test system monitor incoming message paylog.
     *
     * @return the system monitor incoming message.
     */
    protected String getTestSystemMonitorIncomingMessage()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("<datamgt:monitor xmlns:datamgt=\"http://testDomain/system-monitor\" " + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xsi:schemaLocation=\"http://testDomain/system-monitor.xsd\">\n");
        builder.append("   <header>\n");
        builder.append("      <producer>\n");
        builder.append("         <name>testName</name>\n");
        builder.append("         <environment>" + TEST_SQS_ENVIRONMENT + "</environment>\n");
        builder.append("         <origin>testOrigin</origin>\n");
        builder.append("      </producer>\n");
        builder.append("      <creation>\n");
        builder.append("         <datetime>2015-05-13T11:23:36.217-04:00</datetime>\n");
        builder.append("      </creation>\n");
        builder.append("      <correlation-id>" + TEST_SQS_MESSAGE_CORRELATION_ID + "</correlation-id>\n");
        builder.append("      <context-message-type>testDomain/testApplication/SysmonTest</context-message-type>\n");
        builder.append("      <system-message-type>testSystemMessageType</system-message-type>\n");
        builder.append("      <xsd>testXsd</xsd>\n");
        builder.append("   </header>\n");
        builder.append("   <payload>\n");
        builder.append("      <contextMessageTypeToPublish>" + TEST_SQS_CONTEXT_MESSAGE_TYPE_TO_PUBLISH + "</contextMessageTypeToPublish>\n");
        builder.append("   </payload>\n");
        builder.append("</datamgt:monitor>");

        return builder.toString();
    }

    /**
     * Converts a list of Parameters to a list of String values.
     *
     * @return the list of string values representing parameter elements.
     */
    protected List<String> parametersToStringList(List<Parameter> parameters)
    {
        List<String> list = new ArrayList<>();

        for (Parameter parameter : parameters)
        {
            list.add(String.format("\"%s\"=\"%s\"", parameter.getName(), parameter.getValue()));
        }

        return list;
    }

    /**
     * Validates that the specified system monitor response message is valid. If not, an exception will be thrown.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param notificationMessage the system monitor response message
     */
    protected void validateSystemMonitorResponseNotificationMessage(String expectedMessageType, String expectedMessageDestination,
        NotificationMessage notificationMessage)
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        String messageText = notificationMessage.getMessageText();

        // Validate the message text.
        assertTrue("Correlation Id \"" + TEST_SQS_MESSAGE_CORRELATION_ID + "\" expected, but not found.",
            messageText.contains("<correlation-id>" + TEST_SQS_MESSAGE_CORRELATION_ID + "</correlation-id>"));
        assertTrue("Context Message Type \"" + TEST_SQS_CONTEXT_MESSAGE_TYPE_TO_PUBLISH + "\" expected, but not found.",
            messageText.contains("<context-message-type>" + TEST_SQS_CONTEXT_MESSAGE_TYPE_TO_PUBLISH + "</context-message-type>"));

        // Note that we don't response with the environment that was specified in the request message. Instead, we respond with the environment configured
        // in our configuration table.
        assertTrue("Environment \"Development\" expected, but not found.", messageText.contains("<environment>Development</environment>"));
    }
}
