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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.service.activiti.ActivitiHelper;
import org.finra.herd.service.config.ServiceTestSpringModuleConfig;
import org.finra.herd.service.helper.BusinessObjectDataAttributeDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataAttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper;
import org.finra.herd.service.helper.BusinessObjectDataSearchHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionColumnDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.EmrClusterDefinitionHelper;
import org.finra.herd.service.helper.EmrStepHelperFactory;
import org.finra.herd.service.helper.Hive13DdlGenerator;
import org.finra.herd.service.helper.JobDefinitionHelper;
import org.finra.herd.service.helper.NotificationActionFactory;
import org.finra.herd.service.helper.NotificationRegistrationDaoHelper;
import org.finra.herd.service.helper.NotificationRegistrationStatusDaoHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.S3PropertiesLocationHelper;
import org.finra.herd.service.helper.SqsMessageBuilder;
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
    public static final String ACTIVITI_XML_ADD_EMR_MASTER_SECURITY_GROUPS_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowAddEmrMasterSecurityGroup.bpmn20.xml";

    public static final String ACTIVITI_XML_ADD_EMR_STEPS_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowAddEmrStep.bpmn20.xml";

    public static final String ACTIVITI_XML_CHECK_CLUSTER_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowCheckEmrCluster.bpmn20.xml";

    public static final String ACTIVITI_XML_CHECK_OOZIE_WORKFLOW_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowCheckOozieJob.bpmn20.xml";

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

    public static final String ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowRunOozieJob.bpmn20.xml";

    public static final String ACTIVITI_XML_TERMINATE_CLUSTER_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowTerminateEmrCluster.bpmn20.xml";

    public static final String ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testHerdReceiveTaskWorkflow.bpmn20.xml";

    public static final String ACTIVITI_XML_TEST_SERVICE_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testActivitiWorkflowServiceTask.bpmn20.xml";

    public static final String ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testHerdUserTaskWorkflow.bpmn20.xml";

    public static final Boolean ALLOW_MISSING_DATA = true;

    public static final Boolean CREATE_NEW_VERSION = true;

    public static final Boolean DISCOVER_STORAGE_FILES = true;

    public static final String END_PARTITION_VALUE = "2014-04-08";

    public static final int EXPECTED_UUID_SIZE = 36;

    public static final String FILE_NAME = "UT_FileName_1_" + RANDOM_SUFFIX;

    public static final String FILE_NAME_2 = "UT_FileName_2_" + RANDOM_SUFFIX;

    public static final String FILE_NAME_3 = "UT_FileName_3_" + RANDOM_SUFFIX;

    public static final Long FILE_SIZE = (long) (Math.random() * Long.MAX_VALUE);

    public static final Long FILE_SIZE_2 = (long) (Math.random() * Long.MAX_VALUE);

    public static final Boolean INCLUDE_ALL_REGISTERED_SUBPARTITIONS = true;

    public static final Boolean INCLUDE_ARCHIVED_BUSINESS_OBJECT_DATA = true;

    public static final Boolean INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY = true;

    public static final Boolean INCLUDE_DROP_PARTITIONS = true;

    public static final Boolean INCLUDE_DROP_TABLE_STATEMENT = true;

    public static final Boolean INCLUDE_IF_NOT_EXISTS_OPTION = true;

    public static final String NEGATIVE_COLUMN_SIZE = "-1" + RANDOM_SUFFIX;

    public static final String NO_ACTIVITI_JOB_NAME = null;

    public static final JobStatusEnum NO_ACTIVITI_JOB_STATUS = null;

    public static final Boolean NO_ALLOW_MISSING_DATA = false;

    public static final List<BusinessObjectDataStatus> NO_AVAILABLE_STATUSES = new ArrayList<>();

    public static final List<BusinessObjectDataKey> NO_BUSINESS_OBJECT_DATA_CHILDREN = new ArrayList<>();

    public static final List<BusinessObjectDataKey> NO_BUSINESS_OBJECT_DATA_PARENTS = new ArrayList<>();

    public static final List<BusinessObjectDataStatus> NO_BUSINESS_OBJECT_DATA_STATUSES = new ArrayList<>();

    public static final List<BusinessObjectDataStatusChangeEvent> NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY = null;

    public static final String NO_COLUMN_DEFAULT_VALUE = null;

    public static final String NO_COLUMN_DESCRIPTION = null;

    public static final Boolean NO_COLUMN_REQUIRED = false;

    public static final String NO_COLUMN_SIZE = null;

    public static final Boolean NO_CREATE_NEW_VERSION = false;

    public static final Boolean NO_DISCOVER_STORAGE_FILES = false;

    public static final DateTime NO_END_TIME = null;

    public static final Long NO_FILE_SIZE = null;

    public static final Boolean NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS = false;

    public static final Boolean NO_INCLUDE_ARCHIVED_BUSINESS_OBJECT_DATA = false;

    public static final Boolean NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY = false;

    public static final Boolean NO_INCLUDE_DROP_PARTITIONS = false;

    public static final Boolean NO_INCLUDE_DROP_TABLE_STATEMENT = false;

    public static final Boolean NO_INCLUDE_IF_NOT_EXISTS_OPTION = false;

    public static final LatestAfterPartitionValue NO_LATEST_AFTER_PARTITION_VALUE = null;

    public static final LatestBeforePartitionValue NO_LATEST_BEFORE_PARTITION_VALUE = null;

    public static final List<BusinessObjectDataStatus> NO_NOT_AVAILABLE_STATUSES = new ArrayList<>();

    public static final List<String> NO_PARTITION_VALUES = null;

    public static final List<PartitionValueFilter> NO_PARTITION_VALUE_FILTERS = new ArrayList<>();

    public static final PartitionValueRange NO_PARTITION_VALUE_RANGE = null;

    public static final Long NO_ROW_COUNT = null;

    public static final PartitionValueFilter NO_STANDALONE_PARTITION_VALUE_FILTER = null;

    public static final DateTime NO_START_TIME = null;

    public static final StorageDirectory NO_STORAGE_DIRECTORY = null;

    public static final List<StorageFile> NO_STORAGE_FILES = new ArrayList<>();

    public static final List<StorageUnit> NO_STORAGE_UNITS = new ArrayList<>();

    public static final List<String> PROCESS_DATE_AVAILABLE_PARTITION_VALUES = Arrays.asList("2014-04-02", "2014-04-03", "2014-04-08");

    public static final List<String> PROCESS_DATE_NOT_AVAILABLE_PARTITION_VALUES = Arrays.asList("2014-04-04", "2014-04-07");

    public static final List<String> PROCESS_DATE_PARTITION_VALUES = Arrays.asList("2014-04-02", "2014-04-03", "2014-04-04", "2014-04-07", "2014-04-08");

    public static final Long ROW_COUNT = (long) (Math.random() * Long.MAX_VALUE);

    public static final Long ROW_COUNT_2 = (long) (Math.random() * Long.MAX_VALUE);

    public static final String ROW_FORMAT = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'";

    public static final String S3_KEY_PREFIX_VELOCITY_TEMPLATE =
        "$namespace/$dataProviderName/$businessObjectFormatUsage/$businessObjectFormatFileType/$businessObjectDefinitionName" +
            "/schm-v$businessObjectFormatVersion/data-v$businessObjectDataVersion/$businessObjectFormatPartitionKey=$businessObjectDataPartitionValue" +
            "#if($CollectionUtils.isNotEmpty($businessObjectDataSubPartitions.keySet()))" +
            "#foreach($subPartitionKey in $businessObjectDataSubPartitions.keySet())/$subPartitionKey=$businessObjectDataSubPartitions.get($subPartitionKey)" +
            "#end" +
            "#end";

    public static final String SECOND_PARTITION_COLUMN_NAME = "PRTN_CLMN002";

    public static final String SQS_QUEUE_NAME = "UT_Sqs_Queue_Name_" + RANDOM_SUFFIX;

    public static final String START_PARTITION_VALUE = "2014-04-02";

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
    protected RuntimeService activitiRuntimeService;

    @Autowired
    protected TaskService activitiTaskService;

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
    protected BusinessObjectDefinitionColumnDaoHelper businessObjectDefinitionColumnDaoHelper;

    @Autowired
    protected BusinessObjectDefinitionColumnService businessObjectDefinitionColumnService;

    @Autowired
    protected BusinessObjectDefinitionService businessObjectDefinitionService;

    @Autowired
    protected BusinessObjectDefinitionServiceTestHelper businessObjectDefinitionServiceTestHelper;

    @Autowired
    protected BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    protected BusinessObjectFormatService businessObjectFormatService;

    @Autowired
    protected BusinessObjectFormatServiceTestHelper businessObjectFormatServiceTestHelper;

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
    protected ExpectedPartitionValueService expectedPartitionValueService;

    @Autowired
    protected ExpectedPartitionValueServiceTestHelper expectedPartitionValueServiceTestHelper;

    @Autowired
    protected FileTypeService fileTypeService;

    @Autowired
    protected FileUploadCleanupService fileUploadCleanupService;

    @Autowired
    protected HerdStringHelper herdStringHelper;

    @Autowired
    protected Hive13DdlGenerator hive13DdlGenerator;

    @Autowired
    protected JdbcService jdbcService;

    @Autowired
    protected JmsPublishingService jmsPublishingService;

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
    protected NamespaceService namespaceService;

    @Autowired
    protected NamespaceServiceTestHelper namespaceServiceTestHelper;

    @Autowired
    protected NotificationActionFactory notificationActionFactory;

    @Autowired
    protected NotificationEventService notificationEventService;

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
    protected S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    protected S3PropertiesLocationHelper s3PropertiesLocationHelper;

    @Autowired
    protected S3Service s3Service;

    @Autowired
    protected SqsMessageBuilder sqsMessageBuilder;

    @Autowired
    protected SqsNotificationEventService sqsNotificationEventService;

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
    protected SystemJobService systemJobService;

    @Autowired
    protected TagTypeService tagTypeService;

    @Autowired
    protected TagTypeDaoTestHelper tagTypeDaoTestHelper;

    @Autowired
    protected TagService tagService;

    @Autowired
    protected TagDaoTestHelper tagDaoTestHelper;

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
     * @param systemMonitorResponseMessage the system monitor response message.
     */
    protected void validateSystemMonitorResponse(String systemMonitorResponseMessage)
    {
        // Validate the message.
        assertTrue("Correlation Id \"" + TEST_SQS_MESSAGE_CORRELATION_ID + "\" expected, but not found.",
            systemMonitorResponseMessage.contains("<correlation-id>" + TEST_SQS_MESSAGE_CORRELATION_ID + "</correlation-id>"));
        assertTrue("Context Message Type \"" + TEST_SQS_CONTEXT_MESSAGE_TYPE_TO_PUBLISH + "\" expected, but not found.",
            systemMonitorResponseMessage.contains("<context-message-type>" + TEST_SQS_CONTEXT_MESSAGE_TYPE_TO_PUBLISH + "</context-message-type>"));

        // Note that we don't response with the environment that was specified in the request message. Instead, we respond with the environment configured
        // in our configuration table.
        assertTrue("Environment \"Development\" expected, but not found.", systemMonitorResponseMessage.contains("<environment>Development</environment>"));
    }
}
