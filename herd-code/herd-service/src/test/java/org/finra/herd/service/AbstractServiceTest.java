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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
import org.apache.commons.lang3.StringUtils;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.joda.time.DateTime;
import org.junit.After;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.dao.impl.MockJdbcOperations;
import org.finra.herd.dao.impl.MockStsOperationsImpl;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailability;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatus;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.CustomDdl;
import org.finra.herd.model.api.xml.CustomDdlCreateRequest;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.CustomDdlUpdateRequest;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesCreateRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesDeleteRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;
import org.finra.herd.model.api.xml.File;
import org.finra.herd.model.api.xml.JdbcConnection;
import org.finra.herd.model.api.xml.JdbcDatabaseType;
import org.finra.herd.model.api.xml.JdbcExecutionRequest;
import org.finra.herd.model.api.xml.JdbcStatement;
import org.finra.herd.model.api.xml.JdbcStatementType;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.PartitionKeyGroup;
import org.finra.herd.model.api.xml.PartitionKeyGroupCreateRequest;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyCreateRequest;
import org.finra.herd.model.api.xml.StoragePolicyFilter;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StoragePolicyRule;
import org.finra.herd.model.api.xml.StoragePolicyTransition;
import org.finra.herd.model.api.xml.StoragePolicyUpdateRequest;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
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
import org.finra.herd.service.impl.BusinessObjectDataServiceImpl;
import org.finra.herd.service.impl.UploadDownloadHelperServiceImpl;

/**
 * This is an abstract base class that provides useful methods for service test drivers.
 */
@ContextConfiguration(classes = ServiceTestSpringModuleConfig.class, inheritLocations = false)
public abstract class AbstractServiceTest extends AbstractDaoTest
{
    protected static final String ACTIVITI_XML_ADD_EMR_MASTER_SECURITY_GROUPS_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowAddEmrMasterSecurityGroup.bpmn20.xml";

    protected static final String ACTIVITI_XML_ADD_EMR_STEPS_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowAddEmrStep.bpmn20.xml";

    protected static final String ACTIVITI_XML_CHECK_CLUSTER_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowCheckEmrCluster.bpmn20.xml";

    protected static final String ACTIVITI_XML_CHECK_OOZIE_WORKFLOW_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowCheckOozieJob.bpmn20.xml";

    protected static final String ACTIVITI_XML_CREATE_CLUSTER_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowCreateEmrCluster.bpmn20.xml";

    protected static final String ACTIVITI_XML_HERD_INTERMEDIATE_TIMER_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/testHerdIntermediateTimerWorkflow.bpmn20.xml";

    protected static final String ACTIVITI_XML_HERD_TIMER = "org/finra/herd/service/testHerdTimerWorkflow.bpmn20.xml";

    protected static final String ACTIVITI_XML_HERD_TIMER_WITH_CLASSPATH = "classpath:" + ACTIVITI_XML_HERD_TIMER;

    // Activiti workflow resources with and without classpath prefix.
    protected static final String ACTIVITI_XML_HERD_WORKFLOW = "org/finra/herd/service/testHerdWorkflow.bpmn20.xml";

    protected static final String ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH = "classpath:" + ACTIVITI_XML_HERD_WORKFLOW;

    protected static final String ACTIVITI_XML_LOG_VARIABLES_NO_REGEX_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowLogVariablesNoRegex.bpmn20.xml";

    protected static final String ACTIVITI_XML_LOG_VARIABLES_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowLogVariables.bpmn20.xml";

    protected static final String ACTIVITI_XML_LOG_VARIABLES_WITH_CLASSPATH_DM = "classpath:org/finra/herd/service/activitiWorkflowLogVariablesDm.bpmn20.xml";

    protected static final String ACTIVITI_XML_RUN_OOZIE_WORKFLOW_WITH_CLASSPATH = "classpath:org/finra/herd/service/activitiWorkflowRunOozieJob.bpmn20.xml";

    protected static final String ACTIVITI_XML_TERMINATE_CLUSTER_WITH_CLASSPATH =
        "classpath:org/finra/herd/service/activitiWorkflowTerminateEmrCluster.bpmn20.xml";

    protected static final String ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testHerdReceiveTaskWorkflow.bpmn20.xml";

    protected static final String ACTIVITI_XML_TEST_SERVICE_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testActivitiWorkflowServiceTask.bpmn20.xml";

    protected static final String ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH = "classpath:org/finra/herd/service/testHerdUserTaskWorkflow.bpmn20.xml";

    protected static final Boolean ALLOW_MISSING_DATA = true;

    protected static final Boolean CREATE_NEW_VERSION = true;

    protected static final Boolean DISCOVER_STORAGE_FILES = true;

    protected static final int EXPECTED_UUID_SIZE = 36;

    protected static final String FILE_NAME = "UT_FileName_1_" + RANDOM_SUFFIX;

    protected static final String FILE_NAME_2 = "UT_FileName_2_" + RANDOM_SUFFIX;

    protected static final String FILE_NAME_3 = "UT_FileName_3_" + RANDOM_SUFFIX;

    protected static final Long FILE_SIZE = (long) (Math.random() * Long.MAX_VALUE);

    protected static final Long FILE_SIZE_2 = (long) (Math.random() * Long.MAX_VALUE);

    protected static final Boolean INCLUDE_ALL_REGISTERED_SUBPARTITIONS = true;

    protected static final Boolean INCLUDE_ARCHIVED_BUSINESS_OBJECT_DATA = true;

    protected static final Boolean INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY = true;

    protected static final Boolean INCLUDE_DROP_PARTITIONS = true;

    protected static final Boolean INCLUDE_DROP_TABLE_STATEMENT = true;

    protected static final Boolean INCLUDE_IF_NOT_EXISTS_OPTION = true;

    protected static final String NEGATIVE_COLUMN_SIZE = "-1" + RANDOM_SUFFIX;

    protected static final String NO_ACTIVITI_JOB_NAME = null;

    protected static final JobStatusEnum NO_ACTIVITI_JOB_STATUS = null;

    protected static final Boolean NO_ALLOW_MISSING_DATA = false;

    protected static final List<BusinessObjectDataStatus> NO_AVAILABLE_STATUSES = new ArrayList<>();

    protected static final List<BusinessObjectDataKey> NO_BUSINESS_OBJECT_DATA_CHILDREN = new ArrayList<>();

    protected static final List<BusinessObjectDataKey> NO_BUSINESS_OBJECT_DATA_PARENTS = new ArrayList<>();

    protected static final List<BusinessObjectDataStatus> NO_BUSINESS_OBJECT_DATA_STATUSES = new ArrayList<>();

    protected static final List<BusinessObjectDataStatusChangeEvent> NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY = null;

    protected static final String NO_COLUMN_DEFAULT_VALUE = null;

    protected static final String NO_COLUMN_DESCRIPTION = null;

    protected static final Boolean NO_COLUMN_REQUIRED = false;

    protected static final String NO_COLUMN_SIZE = null;

    protected static final Boolean NO_CREATE_NEW_VERSION = false;

    protected static final Boolean NO_DISCOVER_STORAGE_FILES = false;

    protected static final DateTime NO_END_TIME = null;

    protected static final Long NO_FILE_SIZE = null;

    protected static final Boolean NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS = false;

    protected static final Boolean NO_INCLUDE_ARCHIVED_BUSINESS_OBJECT_DATA = false;

    protected static final Boolean NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY = false;

    protected static final Boolean NO_INCLUDE_DROP_PARTITIONS = false;

    protected static final Boolean NO_INCLUDE_DROP_TABLE_STATEMENT = false;

    protected static final Boolean NO_INCLUDE_IF_NOT_EXISTS_OPTION = false;

    protected static final LatestAfterPartitionValue NO_LATEST_AFTER_PARTITION_VALUE = null;

    protected static final LatestBeforePartitionValue NO_LATEST_BEFORE_PARTITION_VALUE = null;

    protected static final List<BusinessObjectDataStatus> NO_NOT_AVAILABLE_STATUSES = new ArrayList<>();

    protected static final List<String> NO_PARTITION_VALUES = null;

    protected static final List<PartitionValueFilter> NO_PARTITION_VALUE_FILTERS = new ArrayList<>();

    protected static final PartitionValueRange NO_PARTITION_VALUE_RANGE = null;

    protected static final Long NO_ROW_COUNT = null;

    protected static final PartitionValueFilter NO_STANDALONE_PARTITION_VALUE_FILTER = null;

    protected static final DateTime NO_START_TIME = null;

    protected static final StorageDirectory NO_STORAGE_DIRECTORY = null;

    protected static final List<StorageFile> NO_STORAGE_FILES = new ArrayList<>();

    protected static final List<StorageUnit> NO_STORAGE_UNITS = new ArrayList<>();

    protected static final List<String> PROCESS_DATE_AVAILABLE_PARTITION_VALUES = Arrays.asList("2014-04-02", "2014-04-03", "2014-04-08");

    protected static final List<String> PROCESS_DATE_NOT_AVAILABLE_PARTITION_VALUES = Arrays.asList("2014-04-04", "2014-04-07");

    protected static final List<String> PROCESS_DATE_PARTITION_VALUES = Arrays.asList("2014-04-02", "2014-04-03", "2014-04-04", "2014-04-07", "2014-04-08");

    protected static final Long ROW_COUNT = (long) (Math.random() * Long.MAX_VALUE);

    protected static final Long ROW_COUNT_2 = (long) (Math.random() * Long.MAX_VALUE);

    protected static final String ROW_FORMAT = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'";

    protected static final String S3_KEY_PREFIX_VELOCITY_TEMPLATE =
        "$namespace/$dataProviderName/$businessObjectFormatUsage/$businessObjectFormatFileType/$businessObjectDefinitionName" +
            "/schm-v$businessObjectFormatVersion/data-v$businessObjectDataVersion/$businessObjectFormatPartitionKey=$businessObjectDataPartitionValue" +
            "#if($CollectionUtils.isNotEmpty($businessObjectDataSubPartitions.keySet()))" +
            "#foreach($subPartitionKey in $businessObjectDataSubPartitions.keySet())/$subPartitionKey=$businessObjectDataSubPartitions.get($subPartitionKey)" +
            "#end" +
            "#end";

    protected static final String SECOND_PARTITION_COLUMN_NAME = "PRTN_CLMN002";

    protected static final String SQS_QUEUE_NAME = "UT_Sqs_Queue_Name_" + RANDOM_SUFFIX;

    /**
     * The test job name as per the above workflow XML file.
     */
    protected static final String TEST_ACTIVITI_JOB_NAME = "testHerdWorkflow";

    /**
     * The test namespace code as per the above workflow XML file.
     */
    protected static final String TEST_ACTIVITI_NAMESPACE_CD = "testNamespace";

    /**
     * This is the test Activiti workflow Id which is the test app name + "." + the test activity job name.
     */
    protected static final String TEST_ACTIVITY_WORKFLOW_ID = TEST_ACTIVITI_NAMESPACE_CD + "." + TEST_ACTIVITI_JOB_NAME;

    protected static final String TEST_SQS_CONTEXT_MESSAGE_TYPE_TO_PUBLISH = "testContextMessageTypeToPublish";

    protected static final String TEST_SQS_ENVIRONMENT = "testEnvironment";

    protected static final String TEST_SQS_MESSAGE_CORRELATION_ID = "testCorrelationId";

    protected static final String ZERO_COLUMN_SIZE = "0";

    protected final String END_PARTITION_VALUE = PROCESS_DATE_PARTITION_VALUES.get(PROCESS_DATE_PARTITION_VALUES.size() - 1);

    protected final String START_PARTITION_VALUE = PROCESS_DATE_PARTITION_VALUES.get(0);

    protected final String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, INITIAL_DATA_VERSION);

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
    protected BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    protected BusinessObjectFormatService businessObjectFormatService;

    @Autowired
    protected CurrentUserService currentUserService;

    @Autowired
    protected CustomDdlService customDdlService;

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
    protected JobService jobService;

    @Autowired
    protected JsonHelper jsonHelper;

    @Autowired
    protected NamespaceService namespaceService;

    @Autowired
    protected NotificationActionFactory notificationActionFactory;

    @Autowired
    protected NotificationEventService notificationEventService;

    @Autowired
    protected NotificationRegistrationDaoHelper notificationRegistrationDaoHelper;

    @Autowired
    protected NotificationRegistrationStatusDaoHelper notificationRegistrationStatusDaoHelper;

    @Autowired
    protected NotificationRegistrationStatusService notificationRegistrationStatusService;

    @Autowired
    protected PartitionKeyGroupService partitionKeyGroupService;

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
    protected UploadDownloadHelperService uploadDownloadHelperService;

    @Autowired
    protected UploadDownloadService uploadDownloadService;

    @Autowired
    protected UserNamespaceAuthorizationService userNamespaceAuthorizationService;

    @Autowired
    protected VelocityHelper velocityHelper;

    @Autowired
    protected XmlHelper xmlHelper;

    @After
    public void after()
    {
        SecurityContextHolder.clearContext();
    }

    /**
     * create database entities for business object search testing
     */
    public void createDatabaseEntitiesForBusinessObjectDataSearchTesting()
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "VALID");
    }

    /**
     * Creates a job based on the specified Activiti XML classpath resource location.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJob(String activitiXmlClasspathResourceName) throws Exception
    {
        createJobDefinition(activitiXmlClasspathResourceName);
        // Start the job synchronously.
        return jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));
    }

    /**
     * Creates a job based on the specified Activiti XML classpath resource location.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJob(String activitiXmlClasspathResourceName, List<Parameter> parameters) throws Exception
    {
        createJobDefinition(activitiXmlClasspathResourceName);
        // Start the job synchronously.
        return jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, parameters));
    }

    /**
     * Creates a job definition based on the specified Activiti XML classpath resource location.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     *
     * @return the job definition.
     * @throws Exception if any errors were encountered.
     */
    public JobDefinition createJobDefinition(String activitiXmlClasspathResourceName) throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequest(activitiXmlClasspathResourceName);
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Validate the returned object against the input.
        assertEquals(new JobDefinition(jobDefinition.getId(), jobDefinitionCreateRequest.getNamespace(), jobDefinitionCreateRequest.getJobName(),
            jobDefinitionCreateRequest.getDescription(), jobDefinitionCreateRequest.getActivitiJobXml(), jobDefinitionCreateRequest.getParameters(),
            jobDefinitionCreateRequest.getS3PropertiesLocation(), HerdDaoSecurityHelper.SYSTEM_USER), jobDefinition);

        return jobDefinition;
    }

    /**
     * Creates a job definition based on the specified Activiti XML.
     *
     * @param activitiXml the Activiti XML.
     *
     * @return the job definition.
     * @throws Exception if any errors were encountered.
     */
    public JobDefinition createJobDefinitionForActivitiXml(String activitiXml) throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create and persist a valid job definition.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequestFromActivitiXml(activitiXml);
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Validate the returned object against the input.
        assertNotNull(jobDefinition);
        assertTrue(jobDefinition.getNamespace().equals(jobDefinitionCreateRequest.getNamespace()));
        assertTrue(jobDefinition.getJobName().equals(jobDefinitionCreateRequest.getJobName()));
        assertTrue(jobDefinition.getDescription().equals(jobDefinitionCreateRequest.getDescription()));

        return jobDefinition;
    }

    /**
     * Creates a job based on the specified Activiti XML classpath resource location and defines a EMR cluster definition.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobForCreateCluster(String activitiXmlClasspathResourceName, List<Parameter> parameters) throws Exception
    {
        return createJobForCreateCluster(activitiXmlClasspathResourceName, parameters, null);
    }

    /**
     * Creates a job based on the specified Activiti XML classpath resource location and defines a EMR cluster definition.
     *
     * @param activitiXmlClasspathResourceName the Activiti XML classpath resource location.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobForCreateCluster(String activitiXmlClasspathResourceName, List<Parameter> parameters, String amiVersion) throws Exception
    {
        createJobDefinition(activitiXmlClasspathResourceName);

        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(TEST_ACTIVITI_NAMESPACE_CD);

        String configXml = IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream());

        EmrClusterDefinition emrClusterDefinition = xmlHelper.unmarshallXmlToObject(EmrClusterDefinition.class, configXml);
        emrClusterDefinition.setAmiVersion(amiVersion);

        configXml = xmlHelper.objectToXml(emrClusterDefinition);

        EmrClusterDefinitionEntity emrClusterDefinitionEntity =
            emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME, configXml);

        Parameter parameter = new Parameter("emrClusterDefinitionName", emrClusterDefinitionEntity.getName());
        parameters.add(parameter);
        parameter = new Parameter("namespace", TEST_ACTIVITI_NAMESPACE_CD);
        parameters.add(parameter);

        // Start the job synchronously.
        return jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, parameters));
    }

    /**
     * Creates a job based on the specified Activiti XML and defines a EMR cluster definition.
     *
     * @param activitiXml the Activiti XML.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobForCreateClusterForActivitiXml(String activitiXml, List<Parameter> parameters) throws Exception
    {
        createJobDefinitionForActivitiXml(activitiXml);

        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(TEST_ACTIVITI_NAMESPACE_CD);
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
                IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        Parameter parameter = new Parameter("namespace", namespaceEntity.getCode());
        parameters.add(parameter);

        parameter = new Parameter("emrClusterDefinitionName", emrClusterDefinitionEntity.getName());
        parameters.add(parameter);

        parameter = new Parameter("dryRun", null);
        parameters.add(parameter);

        parameter = new Parameter("contentType", null);
        parameters.add(parameter);

        parameter = new Parameter("emrClusterDefinitionOverride", null);
        parameters.add(parameter);

        // Start the job synchronously.
        return jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, parameters));
    }

    /**
     * Creates a job based on the specified Activiti XML.
     *
     * @param activitiXml the Activiti XML.
     * @param parameters the job parameters.
     *
     * @return the job.
     * @throws Exception if any errors were encountered.
     */
    public Job createJobFromActivitiXml(String activitiXml, List<Parameter> parameters) throws Exception
    {
        createJobDefinitionForActivitiXml(activitiXml);
        // Start the job synchronously.
        return jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, parameters));
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
     * Creates a business object data attribute create request.
     *
     * @return the newly created business object data attribute create request
     */
    protected BusinessObjectDataAttributeCreateRequest createBusinessObjectDataAttributeCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion, String businessObjectDataAttributeName,
        String businessObjectDataAttributeValue)
    {
        BusinessObjectDataAttributeCreateRequest request = new BusinessObjectDataAttributeCreateRequest();

        request.setBusinessObjectDataAttributeKey(
            new BusinessObjectDataAttributeKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues, businessObjectDataVersion,
                businessObjectDataAttributeName));
        request.setBusinessObjectDataAttributeValue(businessObjectDataAttributeValue);

        return request;
    }

    /**
     * Creates a business object data attribute update request.
     *
     * @return the newly created business object data attribute update request
     */
    protected BusinessObjectDataAttributeUpdateRequest createBusinessObjectDataAttributeUpdateRequest(String businessObjectDataAttributeValue)
    {
        BusinessObjectDataAttributeUpdateRequest request = new BusinessObjectDataAttributeUpdateRequest();

        request.setBusinessObjectDataAttributeValue(businessObjectDataAttributeValue);

        return request;
    }

    /**
     * Returns a newly created business object data create request.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionKey the partition key
     * @param partitionValue the partition value
     * @param storageName the storage name
     * @param storageDirectoryPath the storage directory path
     * @param storageFiles the list of storage files
     *
     * @return the business object create request
     */
    protected BusinessObjectDataCreateRequest createBusinessObjectDataCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionKey, String partitionValue,
        String businessObjectDataStatusCode, String storageName, String storageDirectoryPath, List<StorageFile> storageFiles)
    {
        // Create a business object data create request.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = new BusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setNamespace(namespaceCode);
        businessObjectDataCreateRequest.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectDataCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectDataCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectDataCreateRequest.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataCreateRequest.setPartitionKey(partitionKey);
        businessObjectDataCreateRequest.setPartitionValue(partitionValue);
        businessObjectDataCreateRequest.setStatus(businessObjectDataStatusCode);

        List<StorageUnitCreateRequest> storageUnits = new ArrayList<>();
        businessObjectDataCreateRequest.setStorageUnits(storageUnits);

        StorageUnitCreateRequest storageUnit = new StorageUnitCreateRequest();
        storageUnits.add(storageUnit);
        storageUnit.setStorageName(storageName);
        if (storageDirectoryPath != null)
        {
            StorageDirectory storageDirectory = new StorageDirectory();
            storageUnit.setStorageDirectory(storageDirectory);
            storageDirectory.setDirectoryPath(storageDirectoryPath);
        }
        storageUnit.setStorageFiles(storageFiles);

        return businessObjectDataCreateRequest;
    }

    /**
     * Creates a business object data status update request.
     *
     * @param businessObjectDataStatus the business object data status
     *
     * @return the newly created business object data status update request
     */
    protected BusinessObjectDataStatusUpdateRequest createBusinessObjectDataStatusUpdateRequest(String businessObjectDataStatus)
    {
        BusinessObjectDataStatusUpdateRequest request = new BusinessObjectDataStatusUpdateRequest();
        request.setStatus(businessObjectDataStatus);
        return request;
    }

    protected BusinessObjectDataStorageFilesCreateRequest createBusinessObjectDataStorageFilesCreateRequest(String namespace,
        String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion,
        String partitionValue, List<String> subPartitionValues, Integer businessObjectDataVersion, String storageName, List<StorageFile> storageFiles)
    {
        BusinessObjectDataStorageFilesCreateRequest request = new BusinessObjectDataStorageFilesCreateRequest();
        request.setNamespace(namespace);
        request.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        request.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        request.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        request.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        request.setPartitionValue(partitionValue);
        request.setSubPartitionValues(subPartitionValues);
        request.setBusinessObjectDataVersion(businessObjectDataVersion);
        request.setStorageName(storageName);
        request.setStorageFiles(storageFiles);
        return request;
    }

    /**
     * Creates a business object data definition create request.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param dataProviderName the data provider name
     * @param businessObjectDefinitionDescription the description of the business object definition
     *
     * @return the newly created business object definition create request
     */
    protected BusinessObjectDefinitionCreateRequest createBusinessObjectDefinitionCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription)
    {
        return createBusinessObjectDefinitionCreateRequest(namespaceCode, businessObjectDefinitionName, dataProviderName, businessObjectDefinitionDescription,
            null);
    }

    /**
     * Creates a business object data definition create request.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param dataProviderName the data provider name
     * @param businessObjectDefinitionDescription the description of the business object definition
     *
     * @return the newly created business object definition create request
     */
    protected BusinessObjectDefinitionCreateRequest createBusinessObjectDefinitionCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, List<Attribute> attributes)
    {
        BusinessObjectDefinitionCreateRequest request = new BusinessObjectDefinitionCreateRequest();
        request.setNamespace(namespaceCode);
        request.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        request.setDataProviderName(dataProviderName);
        request.setDescription(businessObjectDefinitionDescription);
        request.setAttributes(attributes);
        return request;
    }

    /**
     * Creates a business object data definition update request.
     *
     * @param businessObjectDefinitionDescription the description of the business object definition
     *
     * @return the newly created business object definition update request
     */
    protected BusinessObjectDefinitionUpdateRequest createBusinessObjectDefinitionUpdateRequest(String businessObjectDefinitionDescription,
        List<Attribute> attributes)
    {
        BusinessObjectDefinitionUpdateRequest request = new BusinessObjectDefinitionUpdateRequest();
        request.setDescription(businessObjectDefinitionDescription);
        request.setAttributes(attributes);
        return request;
    }

    /**
     * Creates and persists {@link BusinessObjectFormatEntity} from the given request. Also creates and persists namespace, data provider, bdef, and file type
     * required for the format. If the request has sub-partitions, schema columns will be persisted. Otherwise, no schema will be set for this format.
     *
     * @param request {@link BusinessObjectDataInvalidateUnregisteredRequest} format alt key
     *
     * @return created {@link BusinessObjectFormatEntity}
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormat(BusinessObjectDataInvalidateUnregisteredRequest request)
    {
        // Create namespace
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(request.getNamespace());

        // Create data provider with a name which is irrelevant for the test cases
        DataProviderEntity dataProviderEntity = dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);

        // Create business object definition
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity, request.getBusinessObjectDefinitionName(), dataProviderEntity, null, null);

        // Create file type
        FileTypeEntity fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(request.getBusinessObjectFormatFileType());

        // Manually creating format since it is easier than providing large amounts of params to existing method
        // Create format
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setUsage(request.getBusinessObjectFormatUsage());
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        // If sub-partition values exist in the request
        if (!CollectionUtils.isEmpty(request.getSubPartitionValues()))
        {
            // Create schema columns
            List<SchemaColumnEntity> schemaColumnEntities = new ArrayList<>();
            for (int partitionLevel = 0; partitionLevel < request.getSubPartitionValues().size() + 1; partitionLevel++)
            {
                SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
                schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                schemaColumnEntity.setName(PARTITION_KEY + partitionLevel);
                schemaColumnEntity.setType("STRING");
                schemaColumnEntity.setPartitionLevel(partitionLevel);
                schemaColumnEntity.setPosition(partitionLevel);
                schemaColumnEntities.add(schemaColumnEntity);
            }
            businessObjectFormatEntity.setSchemaColumns(schemaColumnEntities);
            businessObjectFormatEntity.setPartitionKey(PARTITION_KEY + "0");
        }
        // If sub-partition values do not exist in the request
        else
        {
            businessObjectFormatEntity.setPartitionKey(PARTITION_KEY);
        }
        businessObjectFormatEntity.setLatestVersion(true);
        herdDao.saveAndRefresh(businessObjectFormatEntity);

        return businessObjectFormatEntity;
    }

    /**
     * Creates a business object format create request.
     *
     * @param businessObjectDefinitionName the business object format definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param partitionKey the business object format partition key
     * @param description the business object format description
     * @param attributes the list of attributes
     * @param attributeDefinitions the list of attribute definitions
     * @param schema the business object format schema
     *
     * @return the created business object format create request
     */
    protected BusinessObjectFormatCreateRequest createBusinessObjectFormatCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, String partitionKey, String description, List<Attribute> attributes,
        List<AttributeDefinition> attributeDefinitions, Schema schema)
    {
        BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = new BusinessObjectFormatCreateRequest();

        businessObjectFormatCreateRequest.setNamespace(namespaceCode);
        businessObjectFormatCreateRequest.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectFormatCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectFormatCreateRequest.setPartitionKey(partitionKey);
        businessObjectFormatCreateRequest.setDescription(description);
        businessObjectFormatCreateRequest.setAttributes(attributes);
        businessObjectFormatCreateRequest.setAttributeDefinitions(attributeDefinitions);
        businessObjectFormatCreateRequest.setSchema(schema);

        return businessObjectFormatCreateRequest;
    }

    /**
     * Creates a business object format update request.
     *
     * @param description the business object format description
     * @param attributes the list of attributes
     * @param schema the business object format schema
     *
     * @return the created business object format create request
     */
    protected BusinessObjectFormatUpdateRequest createBusinessObjectFormatUpdateRequest(String description, List<Attribute> attributes, Schema schema)
    {
        BusinessObjectFormatUpdateRequest businessObjectFormatCreateRequest = new BusinessObjectFormatUpdateRequest();

        businessObjectFormatCreateRequest.setDescription(description);
        businessObjectFormatCreateRequest.setAttributes(attributes);
        businessObjectFormatCreateRequest.setSchema(schema);

        return businessObjectFormatCreateRequest;
    }

    /**
     * Creates a custom DDL create request.
     *
     * @return the newly created custom DDL create request
     */
    protected CustomDdlCreateRequest createCustomDdlCreateRequest(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String customDdlName, String ddl)
    {
        CustomDdlCreateRequest request = new CustomDdlCreateRequest();
        request.setCustomDdlKey(
            new CustomDdlKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion,
                customDdlName));
        request.setDdl(ddl);
        return request;
    }

    /**
     * Creates a custom DDL update request.
     *
     * @return the newly created custom DDL update request
     */
    protected CustomDdlUpdateRequest createCustomDdlUpdateRequest(String ddl)
    {
        CustomDdlUpdateRequest request = new CustomDdlUpdateRequest();
        request.setDdl(ddl);
        return request;
    }

    /**
     * Creates and persists database entities required for business object data availability collection testing.
     */
    protected void createDatabaseEntitiesForBusinessObjectDataAvailabilityCollectionTesting()
    {
        // Create a storage unit entity.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
    }

    /**
     * Creates and persists database entities required for generating business object data ddl collection testing.
     */
    protected void createDatabaseEntitiesForBusinessObjectDataDdlCollectionTesting()
    {
        createDatabaseEntitiesForBusinessObjectDataDdlTesting(PARTITION_VALUE);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    protected void createDatabaseEntitiesForBusinessObjectDataDdlTesting()
    {
        createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, PARTITION_KEY_GROUP,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, UNSORTED_PARTITION_VALUES, SUBPARTITION_VALUES, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            schemaColumnDaoTestHelper.getTestPartitionColumns(), false, CUSTOM_DDL_NAME, true, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    protected void createDatabaseEntitiesForBusinessObjectDataDdlTesting(String businessObjectFormatFileType, String partitionKey, String partitionKeyGroupName,
        int partitionColumnPosition, List<String> partitionValues, List<String> subPartitionValues, String schemaDelimiterCharacter,
        String schemaEscapeCharacter, String schemaNullValue, List<SchemaColumn> schemaColumns, List<SchemaColumn> partitionColumns,
        boolean replaceUnderscoresWithHyphens, String customDdlName, boolean generateStorageFileEntities, boolean allowDuplicateBusinessObjectData)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    LATEST_VERSION_FLAG_SET, partitionKey, partitionKeyGroupName, NO_ATTRIBUTES, schemaDelimiterCharacter, schemaEscapeCharacter,
                    schemaNullValue, schemaColumns, partitionColumns);
        }

        if (StringUtils.isNotBlank(customDdlName))
        {
            boolean partitioned = (partitionColumns != null);
            customDdlDaoTestHelper.createCustomDdlEntity(businessObjectFormatEntity, customDdlName, getTestCustomDdl(partitioned));
        }

        // Create S3 storages with the relative "bucket.name" attribute configured.
        StorageEntity storageEntity1 = storageDao.getStorageByName(STORAGE_NAME);
        if (storageEntity1 == null)
        {
            storageEntity1 = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        S3_KEY_PREFIX_VELOCITY_TEMPLATE)));
        }
        StorageEntity storageEntity2 = storageDao.getStorageByName(STORAGE_NAME_2);
        if (storageEntity2 == null)
        {
            storageEntity2 = storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME_2),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        S3_KEY_PREFIX_VELOCITY_TEMPLATE)));
        }

        // Create business object data for each partition value.
        for (String partitionValue : partitionValues)
        {
            BusinessObjectDataEntity businessObjectDataEntity;

            // Create a business object data instance for the specified partition value.
            if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
            {
                businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, partitionValue,
                        subPartitionValues, DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);
            }
            else
            {
                List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                // Please note that the second partition column is located at index 0.
                testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);
            }

            // Get the expected S3 key prefix.
            String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), STORAGE_NAME);

            // Check if we need to create the relative storage units.
            if (STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue) || Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE.equals(partitionValue))
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity1, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

                // If flag is set, create one storage file for each "auto-discoverable" partition.
                // Please note that is n! - thus we want to keep the number of partition levels small.
                if (generateStorageFileEntities)
                {
                    createStorageFiles(storageUnitEntity, s3KeyPrefix, partitionColumns, subPartitionValues, replaceUnderscoresWithHyphens);
                }
                // Add storage directory path value to the storage unit, since we have no storage files generated.
                else
                {
                    storageUnitEntity.setDirectoryPath(s3KeyPrefix);
                }
            }

            if (STORAGE_2_AVAILABLE_PARTITION_VALUES.contains(partitionValue) &&
                (allowDuplicateBusinessObjectData || !STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue)))
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity2, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

                // If flag is set, create one storage file for each "auto-discoverable" partition.
                // Please note that is n! - thus we want to keep the number of partition levels small.
                if (generateStorageFileEntities)
                {
                    createStorageFiles(storageUnitEntity, s3KeyPrefix, partitionColumns, subPartitionValues, replaceUnderscoresWithHyphens);
                }
                // Add storage directory path value to the storage unit, since we have no storage files generated.
                else
                {
                    storageUnitEntity.setDirectoryPath(s3KeyPrefix);
                }
            }
        }
    }

    /**
     * Creates and persists database entities required for generate business object data and format ddl testing.
     */
    protected StorageUnitEntity createDatabaseEntitiesForBusinessObjectDataDdlTesting(String partitionValue)
    {
        if (partitionValue != null)
        {
            // Build an S3 key prefix according to the herd S3 naming convention.
            String s3KeyPrefix =
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    FIRST_PARTITION_COLUMN_NAME, partitionValue, null, null, DATA_VERSION);

            // Creates and persists database entities required for generating business object data ddl testing.
            return createDatabaseEntitiesForBusinessObjectDataDdlTesting(partitionValue, s3KeyPrefix);
        }
        else
        {
            // Creates and persists database entities required for generating business object format ddl testing.
            return createDatabaseEntitiesForBusinessObjectDataDdlTesting(null, null);
        }
    }

    /**
     * Creates and persists database entities required for generate business object data ddl testing.
     */
    protected StorageUnitEntity createDatabaseEntitiesForBusinessObjectDataDdlTesting(String partitionValue, String s3KeyPrefix)
    {
        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns
            .add(new SchemaColumn(FIRST_PARTITION_COLUMN_NAME, "DATE", NO_COLUMN_SIZE, COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(COLUMN_NAME, "NUMBER", COLUMN_SIZE, NO_COLUMN_REQUIRED, COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION));

        // Use the first column as a partition column.
        List<SchemaColumn> partitionColumns = schemaColumns.subList(0, 1);

        // Create a business object format entity with the schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns);

        if (partitionValue != null)
        {
            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, partitionValue, NO_SUBPARTITION_VALUES, DATA_VERSION, true,
                    BusinessObjectDataStatusEntity.VALID);

            // Create an S3 storage entity.
            StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

            // Create a storage unit with a storage directory path.
            return storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);
        }

        return null;
    }

    /**
     * Creates and persists VALID two-level partitioned business object data with "available" storage units in an S3 storage.
     *
     * @param partitions the list of partitions, where each is represented by a primary value and a sub-partition value
     *
     * @return the list of created storage unit entities
     */
    protected List<StorageUnitEntity> createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(List<List<String>> partitions)
    {
        // Create a list of storage unit entities to be returned.
        List<StorageUnitEntity> result = new ArrayList<>();

        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns
            .add(new SchemaColumn(FIRST_PARTITION_COLUMN_NAME, "DATE", NO_COLUMN_SIZE, COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION));
        schemaColumns
            .add(new SchemaColumn(SECOND_PARTITION_COLUMN_NAME, "STRING", NO_COLUMN_SIZE, COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(COLUMN_NAME, "NUMBER", COLUMN_SIZE, NO_COLUMN_REQUIRED, COLUMN_DEFAULT_VALUE, COLUMN_DESCRIPTION));

        // Use the first two columns as partition columns.
        List<SchemaColumn> partitionColumns = schemaColumns.subList(0, 2);

        // Create a business object format entity with the schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, FIRST_PARTITION_COLUMN_NAME, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns);

        // Create an S3 storage entity.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        for (List<String> partition : partitions)
        {
            // Build an S3 key prefix according to the herd S3 naming convention.
            String s3KeyPrefix =
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    FIRST_PARTITION_COLUMN_NAME, partition.get(0), partitionColumns.subList(1, 2).toArray(new SchemaColumn[1]),
                    Arrays.asList(partition.get(1)).toArray(new String[1]), DATA_VERSION);

            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, partition.get(0), Arrays.asList(partition.get(1)), DATA_VERSION,
                    LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

            // Create an "available" storage unit with a storage directory path.
            result.add(storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix));
        }

        return result;
    }

    /**
     * Create and persist database entities required for testing.
     */
    protected void createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting()
    {
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), NOTIFICATION_EVENT_TYPE), BDEF_NAMESPACE, BDEF_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(BDATA_STATUS, BDATA_STATUS_2),
            notificationRegistrationDaoTestHelper.getTestJobActions());
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param namespace the namespace of the business object data notification registration
     * @param notificationEventTypes the list of notification event types
     * @param businessObjectDefinitionNamespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param fileTypes the list of file types
     * @param storageNames the list of storage names
     * @param businessObjectDataStatuses the list of business object data statuses
     * @param jobActions the list of job actions
     */
    protected void createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(String namespace, List<String> notificationEventTypes,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, List<String> fileTypes, List<String> storageNames,
        List<String> businessObjectDataStatuses, List<JobAction> jobActions)
    {
        // Create a namespace entity, if not exists.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);
        if (namespaceEntity == null)
        {
            namespaceDaoTestHelper.createNamespaceEntity(namespace);
        }

        // Create specified notification event types, if not exist.
        if (!CollectionUtils.isEmpty(notificationEventTypes))
        {
            for (String notificationEventType : notificationEventTypes)
            {
                NotificationEventTypeEntity notificationEventTypeEntity = notificationEventTypeDao.getNotificationEventTypeByCode(notificationEventType);
                if (notificationEventTypeEntity == null)
                {
                    notificationRegistrationDaoTestHelper.createNotificationEventTypeEntity(notificationEventType);
                }
            }
        }

        // Create specified business object definition, if not exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao
            .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            // Create and persist a business object definition entity.
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        }

        // Create specified file type entities, if not exist.
        if (!CollectionUtils.isEmpty(fileTypes))
        {
            for (String businessObjectFormatFileType : fileTypes)
            {
                fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create specified storage entities, if not exist.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            for (String storageName : storageNames)
            {
                storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
            }
        }

        // Create specified business object data status entities, if not exist.
        if (!CollectionUtils.isEmpty(businessObjectDataStatuses))
        {
            for (String businessObjectDataStatus : businessObjectDataStatuses)
            {
                BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
                    businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatus);
                if (businessObjectDataStatusEntity == null)
                {
                    businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(businessObjectDataStatus);
                }
            }
        }

        // Create specified job definition entities.
        if (!CollectionUtils.isEmpty(jobActions))
        {
            for (JobAction jobAction : jobActions)
            {
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName(),
                    String.format("Description of \"%s.%s\" job definition.", jobAction.getNamespace(), jobAction.getJobName()),
                    String.format("%s.%s.%s", jobAction.getNamespace(), jobAction.getJobName(), ACTIVITI_ID));
            }
        }
    }

    /**
     * Create and persist database entities required for testing.
     */
    protected void createDatabaseEntitiesForBusinessObjectDefinitionTesting()
    {
        createDatabaseEntitiesForBusinessObjectDefinitionTesting(NAMESPACE, DATA_PROVIDER_NAME);
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param namespaceCode the namespace code
     * @param dataProviderName the data provider name
     */
    protected void createDatabaseEntitiesForBusinessObjectDefinitionTesting(String namespaceCode, String dataProviderName)
    {
        // Create a namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(namespaceCode);

        // Create a data provider entity.
        dataProviderDaoTestHelper.createDataProviderEntity(dataProviderName);
    }

    /**
     * Creates and persists database entities required for generating business object format ddl collection testing.
     */
    protected void createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting()
    {
        createDatabaseEntitiesForBusinessObjectDataDdlTesting(null);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    protected void createDatabaseEntitiesForBusinessObjectFormatDdlTesting()
    {
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, FIRST_PARTITION_COLUMN_NAME, SCHEMA_DELIMITER_PIPE,
            SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            schemaColumnDaoTestHelper.getTestPartitionColumns(), CUSTOM_DDL_NAME);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    protected void createDatabaseEntitiesForBusinessObjectFormatDdlTesting(String businessObjectFormatFileType, String partitionKey,
        String schemaDelimiterCharacter, String schemaEscapeCharacter, String schemaNullValue, List<SchemaColumn> schemaColumns,
        List<SchemaColumn> partitionColumns, String customDdlName)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, businessObjectFormatFileType, FORMAT_VERSION, FORMAT_DESCRIPTION,
                    LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, schemaDelimiterCharacter, schemaEscapeCharacter,
                    schemaNullValue, schemaColumns, partitionColumns);
        }

        if (StringUtils.isNotBlank(customDdlName))
        {
            boolean partitioned = (partitionColumns != null);
            customDdlDaoTestHelper.createCustomDdlEntity(businessObjectFormatEntity, customDdlName, getTestCustomDdl(partitioned));
        }
    }

    /**
     * Create and persist database entities required for the finalize restore testing.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data entity
     */
    protected BusinessObjectDataEntity createDatabaseEntitiesForFinalizeRestoreTesting(BusinessObjectDataKey businessObjectDataKey)
    {
        return createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
            StorageUnitStatusEntity.RESTORING, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
            S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);
    }

    /**
     * Create and persist database entities required for the finalize restore testing.
     *
     * @param businessObjectDataKey the business object data key
     * @param originStorageName the origin S3 storage name
     * @param originBucketName the S3 bucket name for the origin storage
     * @param originStorageUnitStatus the origin S3 storage unit status
     * @param glacierStorageName the Glacier storage name
     * @param glacierStorageBucketName the S3 bucket name for the Glacier storage
     * @param glacierStorageUnitStatus the Glacier storage unit status
     * @param glacierStorageDirectoryPath the storage directory path for the Glacier storage unit
     *
     * @return the business object data entity
     */
    protected BusinessObjectDataEntity createDatabaseEntitiesForFinalizeRestoreTesting(BusinessObjectDataKey businessObjectDataKey, String originStorageName,
        String originBucketName, String originStorageUnitStatus, String glacierStorageName, String glacierStorageBucketName, String glacierStorageUnitStatus,
        String glacierStorageDirectoryPath)
    {
        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create and persist an origin S3 storage entity.
        StorageEntity originStorageEntity;
        if (originBucketName != null)
        {
            originStorageEntity = storageDaoTestHelper.createStorageEntity(originStorageName, StoragePlatformEntity.S3,
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), originBucketName);
        }
        else
        {
            originStorageEntity = storageDaoTestHelper.createStorageEntity(originStorageName, StoragePlatformEntity.S3);
        }

        // Create and persist a Glacier storage entity.
        StorageEntity glacierStorageEntity;
        if (glacierStorageBucketName != null)
        {
            glacierStorageEntity = storageDaoTestHelper.createStorageEntity(glacierStorageName, StoragePlatformEntity.GLACIER,
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), glacierStorageBucketName);
        }
        else
        {
            glacierStorageEntity = storageDaoTestHelper.createStorageEntity(glacierStorageName, StoragePlatformEntity.GLACIER);
        }

        // Create and persist an S3 storage unit entity.
        StorageUnitEntity originStorageUnitEntity =
            storageUnitDaoTestHelper.createStorageUnitEntity(originStorageEntity, businessObjectDataEntity, originStorageUnitStatus, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(glacierStorageEntity, businessObjectDataEntity, glacierStorageUnitStatus, glacierStorageDirectoryPath);

        // Set a parent storage unit for the Glacier storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(originStorageUnitEntity);

        // Create and add storage file entities to the origin storage unit.
        for (String relativeFilePath : LOCAL_FILES)
        {
            storageFileDaoTestHelper
                .createStorageFileEntity(originStorageUnitEntity, String.format("%s/%s", TEST_S3_KEY_PREFIX, relativeFilePath), FILE_SIZE_1_KB, ROW_COUNT);
        }

        // Return the business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param createBusinessObjectDataEntity specifies if a business object data instance should be created or not
     */
    protected void createDatabaseEntitiesForGetS3KeyPrefixTesting(boolean createBusinessObjectDataEntity)
    {
        // Get a list of test schema partition columns and use the first column name as the partition key.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, partitionKey, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

        // Create and persist an S3 storage with the S3 key prefix velocity template attribute.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3,
            configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), S3_KEY_PREFIX_VELOCITY_TEMPLATE);

        // If requested, create and persist a business object data entity.
        if (createBusinessObjectDataEntity)
        {
            businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        }
    }

    /**
     * Create and persist database entities required for the initiate a restore request testing.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data entity
     */
    protected BusinessObjectDataEntity createDatabaseEntitiesForInitiateRestoreTesting(BusinessObjectDataKey businessObjectDataKey)
    {
        return createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
            StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
            S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);
    }

    /**
     * Create and persist database entities required for the initiate a restore request testing.
     *
     * @param businessObjectDataKey the business object data key
     * @param originStorageName the origin S3 storage name
     * @param originBucketName the S3 bucket name for the origin storage
     * @param originStorageUnitStatus the origin S3 storage unit status
     * @param glacierStorageName the Glacier storage name
     * @param glacierStorageBucketName the S3 bucket name for the Glacier storage
     * @param glacierStorageUnitStatus the Glacier storage unit status
     * @param glacierStorageDirectoryPath the storage directory path for the Glacier storage unit
     *
     * @return the business object data entity
     */
    protected BusinessObjectDataEntity createDatabaseEntitiesForInitiateRestoreTesting(BusinessObjectDataKey businessObjectDataKey, String originStorageName,
        String originBucketName, String originStorageUnitStatus, String glacierStorageName, String glacierStorageBucketName, String glacierStorageUnitStatus,
        String glacierStorageDirectoryPath)
    {
        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create and persist an origin S3 storage entity.
        StorageEntity originStorageEntity;
        if (originBucketName != null)
        {
            originStorageEntity = storageDaoTestHelper.createStorageEntity(originStorageName, StoragePlatformEntity.S3,
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), originBucketName);
        }
        else
        {
            originStorageEntity = storageDaoTestHelper.createStorageEntity(originStorageName, StoragePlatformEntity.S3);
        }

        // Create and persist a Glacier storage entity.
        StorageEntity glacierStorageEntity;
        if (glacierStorageBucketName != null)
        {
            glacierStorageEntity = storageDaoTestHelper.createStorageEntity(glacierStorageName, StoragePlatformEntity.GLACIER,
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), glacierStorageBucketName);
        }
        else
        {
            glacierStorageEntity = storageDaoTestHelper.createStorageEntity(glacierStorageName, StoragePlatformEntity.GLACIER);
        }

        // Create and persist an S3 storage unit entity.
        StorageUnitEntity originStorageUnitEntity =
            storageUnitDaoTestHelper.createStorageUnitEntity(originStorageEntity, businessObjectDataEntity, originStorageUnitStatus, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(glacierStorageEntity, businessObjectDataEntity, glacierStorageUnitStatus, glacierStorageDirectoryPath);

        // Set a parent storage unit for the Glacier storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(originStorageUnitEntity);

        // Create and add storage file entities to the origin storage unit.
        for (String relativeFilePath : LOCAL_FILES)
        {
            storageFileDaoTestHelper
                .createStorageFileEntity(originStorageUnitEntity, String.format("%s/%s", TEST_S3_KEY_PREFIX, relativeFilePath), FILE_SIZE_1_KB, ROW_COUNT);
        }

        // Return the business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Create and persist database entities required for storage policy service testing.
     */
    protected void createDatabaseEntitiesForStoragePolicyTesting()
    {
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));
    }

    /**
     * Create and persist database entities required for storage policy service testing.
     *
     * @param storagePolicyNamespace the storage policy namespace
     * @param storagePolicyRuleTypes the list of storage policy rule types
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param fileTypes the list of file types
     * @param storageNames the list of storage names
     * @param destinationStorageNames the list of destination storage names
     */
    protected void createDatabaseEntitiesForStoragePolicyTesting(String storagePolicyNamespace, List<String> storagePolicyRuleTypes,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, List<String> fileTypes, List<String> storageNames,
        List<String> destinationStorageNames)
    {
        // Create a storage policy namespace entity, if not exists.
        NamespaceEntity storagePolicyNamespaceEntity = namespaceDao.getNamespaceByCd(storagePolicyNamespace);
        if (storagePolicyNamespaceEntity == null)
        {
            namespaceDaoTestHelper.createNamespaceEntity(storagePolicyNamespace);
        }

        // Create specified storage policy rule types, if not exist.
        if (!CollectionUtils.isEmpty(storagePolicyRuleTypes))
        {
            for (String storagePolicyRuleType : storagePolicyRuleTypes)
            {
                if (storagePolicyRuleTypeDao.getStoragePolicyRuleTypeByCode(storagePolicyRuleType) == null)
                {
                    storagePolicyRuleTypeDaoTestHelper.createStoragePolicyRuleTypeEntity(storagePolicyRuleType, DESCRIPTION);
                }
            }
        }

        // Create specified business object definition entity, if not exist.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity;
        if (StringUtils.isNotBlank(businessObjectDefinitionName))
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDao
                .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
            if (businessObjectDefinitionEntity == null)
            {
                // Create a business object definition.
                businessObjectDefinitionDaoTestHelper
                    .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, DATA_PROVIDER_NAME,
                        BDEF_DESCRIPTION);
            }
        }

        // Create specified file type entities, if not exist.
        if (!CollectionUtils.isEmpty(fileTypes))
        {
            for (String businessObjectFormatFileType : fileTypes)
            {
                fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create specified storage entities of S3 storage platform type, if not exist.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            for (String storageName : storageNames)
            {
                if (storageDao.getStorageByName(storageName) == null)
                {
                    // Create S3 storage with the relative attributes.
                    storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, Arrays
                        .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                                S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()),
                            new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE),
                                Boolean.TRUE.toString())));
                }
            }
        }

        // Create specified destination storage entities of GLACIER storage platform type, if not exist.
        if (!CollectionUtils.isEmpty(destinationStorageNames))
        {
            for (String destinationStorageName : destinationStorageNames)
            {
                if (storageDao.getStorageByName(destinationStorageName) == null)
                {
                    // Create Glacier storage with configured S3 bucket name attribute for the "archive" S3 bucket.
                    storageDaoTestHelper.createStorageEntity(destinationStorageName, StoragePlatformEntity.GLACIER,
                        configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME_2);
                }
            }
        }
    }

    /**
     * Create and persist database entities required for testing.
     */
    protected void createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting()
    {
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE,
            Arrays.asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), NOTIFICATION_EVENT_TYPE), BDEF_NAMESPACE, BDEF_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2),
            notificationRegistrationDaoTestHelper.getTestJobActions());
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param namespace the namespace of the storage unit notification registration
     * @param notificationEventTypes the list of notification event types
     * @param businessObjectDefinitionNamespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param fileTypes the list of file types
     * @param storageNames the list of storage names
     * @param storageUnitStatuses the list of storage unit statuses
     * @param jobActions the list of job actions
     */
    protected void createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(String namespace, List<String> notificationEventTypes,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, List<String> fileTypes, List<String> storageNames,
        List<String> storageUnitStatuses, List<JobAction> jobActions)
    {
        // Create a namespace entity, if not exists.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);
        if (namespaceEntity == null)
        {
            namespaceDaoTestHelper.createNamespaceEntity(namespace);
        }

        // Create specified notification event types, if not exist.
        if (!CollectionUtils.isEmpty(notificationEventTypes))
        {
            for (String notificationEventType : notificationEventTypes)
            {
                NotificationEventTypeEntity notificationEventTypeEntity = notificationEventTypeDao.getNotificationEventTypeByCode(notificationEventType);
                if (notificationEventTypeEntity == null)
                {
                    notificationRegistrationDaoTestHelper.createNotificationEventTypeEntity(notificationEventType);
                }
            }
        }

        // Create specified business object definition, if not exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao
            .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            // Create and persist a business object definition entity.
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        }

        // Create specified file type entities, if not exist.
        if (!CollectionUtils.isEmpty(fileTypes))
        {
            for (String businessObjectFormatFileType : fileTypes)
            {
                fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create specified storage entities, if not exist.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            for (String storageName : storageNames)
            {
                storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
            }
        }

        // Create specified business object data status entities, if not exist.
        if (!CollectionUtils.isEmpty(storageUnitStatuses))
        {
            for (String storageUnitStatus : storageUnitStatuses)
            {
                StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDao.getStorageUnitStatusByCode(storageUnitStatus);
                if (storageUnitStatusEntity == null)
                {
                    storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(storageUnitStatus);
                }
            }
        }

        // Create specified job definition entities.
        if (!CollectionUtils.isEmpty(jobActions))
        {
            for (JobAction jobAction : jobActions)
            {
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName(),
                    String.format("Description of \"%s.%s\" job definition.", jobAction.getNamespace(), jobAction.getJobName()),
                    String.format("%s.%s.%s", jobAction.getNamespace(), jobAction.getJobName(), ACTIVITI_ID));
            }
        }
    }

    /**
     * Create and persist database entities required for upload download testing.
     */
    protected void createDatabaseEntitiesForUploadDownloadTesting()
    {
        createDatabaseEntitiesForUploadDownloadTesting(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);
        createDatabaseEntitiesForUploadDownloadTesting(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2);
    }

    /**
     * Create and persist database entities required for upload download testing.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format file type
     * @param businessObjectFormatFileType the business object format file type
     */
    protected void createDatabaseEntitiesForUploadDownloadTesting(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion)
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, FORMAT_DESCRIPTION, true, PARTITION_KEY);
    }

    /**
     * Creates a default test JDBC connection which is guaranteed to work. The connection points to the in-memory database setup as part of DAO mocks.
     *
     * @return a valid JDBC connection
     */
    protected JdbcConnection createDefaultJdbcConnection()
    {
        JdbcConnection jdbcConnection = new JdbcConnection();
        jdbcConnection.setUrl("jdbc:h2:mem:herdTestDb");
        jdbcConnection.setUsername("");
        jdbcConnection.setPassword("");
        jdbcConnection.setDatabaseType(JdbcDatabaseType.POSTGRES);
        return jdbcConnection;
    }

    /**
     * Creates a default JDBC execution request which is guaranteed to work. The request contains a single statement of QUERY type.
     *
     * @return a valid JDBC request
     */
    protected JdbcExecutionRequest createDefaultQueryJdbcExecutionRequest()
    {
        JdbcConnection jdbcConnection = createDefaultJdbcConnection();
        List<JdbcStatement> jdbcStatements = createDefaultQueryJdbcStatements();
        JdbcExecutionRequest jdbcExecutionRequest = createJdbcExecutionRequest(jdbcConnection, jdbcStatements);
        return jdbcExecutionRequest;
    }

    /**
     * Returns a valid list of JDBC QUERY statements.It contains only 1 statement, and the statement is CASE_1_SQL in mock JDBC (success, result 1)
     *
     * @return list of statements
     */
    protected List<JdbcStatement> createDefaultQueryJdbcStatements()
    {
        List<JdbcStatement> jdbcStatements = new ArrayList<>();
        {
            JdbcStatement jdbcStatement = new JdbcStatement();
            jdbcStatement.setType(JdbcStatementType.QUERY);
            jdbcStatement.setSql(MockJdbcOperations.CASE_1_SQL);
            jdbcStatements.add(jdbcStatement);
        }
        return jdbcStatements;
    }

    /**
     * Creates a default JDBC execution request which is guaranteed to work. The request contains a single statement of UPDATE type.
     *
     * @return a valid JDBC request
     */
    protected JdbcExecutionRequest createDefaultUpdateJdbcExecutionRequest()
    {
        JdbcConnection jdbcConnection = createDefaultJdbcConnection();
        List<JdbcStatement> jdbcStatements = createDefaultUpdateJdbcStatements();
        JdbcExecutionRequest jdbcExecutionRequest = createJdbcExecutionRequest(jdbcConnection, jdbcStatements);
        return jdbcExecutionRequest;
    }

    /**
     * Returns a valid list of JDBC UPDATE statements. It contains only 1 statement, and the statement is CASE_1_SQL in mock JDBC (success, result 1)
     *
     * @return list of statements.
     */
    protected List<JdbcStatement> createDefaultUpdateJdbcStatements()
    {
        List<JdbcStatement> jdbcStatements = new ArrayList<>();
        {
            JdbcStatement jdbcStatement = new JdbcStatement();
            jdbcStatement.setType(JdbcStatementType.UPDATE);
            jdbcStatement.setSql(MockJdbcOperations.CASE_1_SQL);
            jdbcStatements.add(jdbcStatement);
        }
        return jdbcStatements;
    }

    /**
     * Creates an expected partition values create request.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param expectedPartitionValues the list of expected partition values
     *
     * @return the expected partition values create request
     */
    protected ExpectedPartitionValuesCreateRequest createExpectedPartitionValuesCreateRequest(String partitionKeyGroupName,
        List<String> expectedPartitionValues)
    {
        ExpectedPartitionValuesCreateRequest expectedPartitionValuesCreateRequest = new ExpectedPartitionValuesCreateRequest();
        expectedPartitionValuesCreateRequest.setPartitionKeyGroupKey(createPartitionKeyGroupKey(partitionKeyGroupName));
        expectedPartitionValuesCreateRequest.setExpectedPartitionValues(expectedPartitionValues);
        return expectedPartitionValuesCreateRequest;
    }

    /**
     * Creates an expected partition values delete request.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param expectedPartitionValues the list of expected partition values
     *
     * @return the expected partition values delete request
     */
    protected ExpectedPartitionValuesDeleteRequest createExpectedPartitionValuesDeleteRequest(String partitionKeyGroupName,
        List<String> expectedPartitionValues)
    {
        ExpectedPartitionValuesDeleteRequest expectedPartitionValuesDeleteRequest = new ExpectedPartitionValuesDeleteRequest();
        expectedPartitionValuesDeleteRequest.setPartitionKeyGroupKey(createPartitionKeyGroupKey(partitionKeyGroupName));
        expectedPartitionValuesDeleteRequest.setExpectedPartitionValues(expectedPartitionValues);
        return expectedPartitionValuesDeleteRequest;
    }

    /**
     * Creates a JDBC request with the specified values.
     *
     * @param jdbcConnection JDBC connection
     * @param jdbcStatements JDBC statements
     *
     * @return an execution request.
     */
    protected JdbcExecutionRequest createJdbcExecutionRequest(JdbcConnection jdbcConnection, List<JdbcStatement> jdbcStatements)
    {
        JdbcExecutionRequest jdbcExecutionRequest = new JdbcExecutionRequest();
        jdbcExecutionRequest.setConnection(jdbcConnection);
        jdbcExecutionRequest.setStatements(jdbcStatements);
        return jdbcExecutionRequest;
    }

    /**
     * Creates job create request using a specified namespace code and job name, but test hard coded parameters will be used.
     *
     * @param namespaceCd the namespace code.
     * @param jobName the job definition name.
     *
     * @return the created job create request.
     */
    protected JobCreateRequest createJobCreateRequest(String namespaceCd, String jobName)
    {
        // Create a test list of parameters.
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);
        parameters.add(parameter);
        parameter = new Parameter("Extra Attribute With No Value", null);
        parameters.add(parameter);

        return createJobCreateRequest(namespaceCd, jobName, parameters);
    }

    /**
     * Creates job create request using test hard coded values.
     *
     * @param namespaceCd the namespace code.
     * @param jobName the job definition name.
     * @param parameters the job parameters.
     *
     * @return the created job create request.
     */
    protected JobCreateRequest createJobCreateRequest(String namespaceCd, String jobName, List<Parameter> parameters)
    {
        // Create a job create request.
        JobCreateRequest jobCreateRequest = new JobCreateRequest();
        jobCreateRequest.setNamespace(namespaceCd);
        jobCreateRequest.setJobName(jobName);
        jobCreateRequest.setParameters(parameters);
        return jobCreateRequest;
    }

    /**
     * Creates a new job definition create request based on fixed parameters.
     */
    protected JobDefinitionCreateRequest createJobDefinitionCreateRequest()
    {
        return createJobDefinitionCreateRequest(null);
    }

    /**
     * Creates a new job definition create request based on fixed parameters and a specified XML resource location.
     *
     * @param activitiXmlClasspathResourceName the classpath resource location to the Activiti XML. If null is specified, then the default
     * ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH will be used.
     */
    protected JobDefinitionCreateRequest createJobDefinitionCreateRequest(String activitiXmlClasspathResourceName)
    {
        // Create a test list of parameters.
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        parameters.add(parameter);

        if (activitiXmlClasspathResourceName == null)
        {
            activitiXmlClasspathResourceName = ACTIVITI_XML_HERD_WORKFLOW_WITH_CLASSPATH;
        }
        try
        {
            return createJobDefinitionCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JOB_DESCRIPTION,
                IOUtils.toString(resourceLoader.getResource(activitiXmlClasspathResourceName).getInputStream()), parameters);
        }
        catch (IOException ex)
        {
            throw new RuntimeException("Unable to load Activiti XML from classpath resource: " + activitiXmlClasspathResourceName);
        }
    }

    /**
     * Creates a new job definition create request based on user specified parameters.
     *
     * @param namespaceCd the namespace code.
     * @param jobName the job name.
     * @param jobDescription the job description.
     * @param activitiXml the Activiti XML.
     * @param parameters the parameters.
     *
     * @return the job definition create request.
     */
    protected JobDefinitionCreateRequest createJobDefinitionCreateRequest(String namespaceCd, String jobName, String jobDescription, String activitiXml,
        List<Parameter> parameters)
    {
        // Create and return a new job definition create request.
        JobDefinitionCreateRequest request = new JobDefinitionCreateRequest();
        request.setNamespace(namespaceCd);
        request.setJobName(jobName);
        request.setDescription(jobDescription);
        request.setActivitiJobXml(activitiXml);
        request.setParameters(parameters);
        return request;
    }

    /**
     * Creates a new job definition create request based on fixed parameters and a specified activiti XML.
     *
     * @param activitiXml the Activiti XML.
     */
    protected JobDefinitionCreateRequest createJobDefinitionCreateRequestFromActivitiXml(String activitiXml)
    {
        // Create a test list of parameters.
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_1_MIXED_CASE);
        parameters.add(parameter);

        return createJobDefinitionCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JOB_DESCRIPTION, activitiXml, parameters);
    }

    /**
     * Creates a namespace create request.
     *
     * @param namespaceCode the namespace code
     *
     * @return the newly created namespace create request
     */
    protected NamespaceCreateRequest createNamespaceCreateRequest(String namespaceCode)
    {
        NamespaceCreateRequest request = new NamespaceCreateRequest();
        request.setNamespaceCode(namespaceCode);
        return request;
    }

    /**
     * Creates partition key group by calling the relative service method.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the newly created partition key group
     */
    protected PartitionKeyGroup createPartitionKeyGroup(String partitionKeyGroupName)
    {
        PartitionKeyGroupCreateRequest request = createPartitionKeyGroupCreateRequest(partitionKeyGroupName);
        return partitionKeyGroupService.createPartitionKeyGroup(request);
    }

    /**
     * Creates a partition key group create request.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the created partition key group create request
     */
    protected PartitionKeyGroupCreateRequest createPartitionKeyGroupCreateRequest(String partitionKeyGroupName)
    {
        PartitionKeyGroupCreateRequest partitionKeyGroupCreateRequest = new PartitionKeyGroupCreateRequest();
        partitionKeyGroupCreateRequest.setPartitionKeyGroupKey(createPartitionKeyGroupKey(partitionKeyGroupName));
        return partitionKeyGroupCreateRequest;
    }

    /**
     * Creates a partition key group key.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the created partition key group key
     */
    protected PartitionKeyGroupKey createPartitionKeyGroupKey(String partitionKeyGroupName)
    {
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
        partitionKeyGroupKey.setPartitionKeyGroupName(partitionKeyGroupName);
        return partitionKeyGroupKey;
    }

    /**
     * Creates an object in S3 with the prefix constructed from the given parameters. The object's full path will be {prefix}/{UUID}
     *
     * @param businessObjectFormatEntity business object format
     * @param request request with partition values and storage
     * @param businessObjectDataVersion business object data version to put
     */
    protected void createS3Object(BusinessObjectFormatEntity businessObjectFormatEntity, BusinessObjectDataInvalidateUnregisteredRequest request,
        int businessObjectDataVersion)
    {
        StorageEntity storageEntity = storageDao.getStorageByName(request.getStorageName());
        String s3BucketName = storageHelper.getS3BucketAccessParams(storageEntity).getS3BucketName();

        BusinessObjectDataKey businessObjectDataKey = getBusinessObjectDataKey(request);
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataVersion);

        String s3KeyPrefix =
            s3KeyPrefixHelper.buildS3KeyPrefix(S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity, businessObjectDataKey, storageEntity.getName());
        String s3ObjectKey = s3KeyPrefix + "/test";
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3ObjectKey, new ByteArrayInputStream(new byte[1]), new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);
    }

    protected void createStorageFiles(StorageUnitEntity storageUnitEntity, String s3KeyPrefix, List<SchemaColumn> partitionColumns,
        List<String> subPartitionValues, boolean replaceUnderscoresWithHyphens)
    {
        int discoverableSubPartitionsCount = partitionColumns != null ? partitionColumns.size() - subPartitionValues.size() - 1 : 0;
        int storageFilesCount = (int) Math.pow(2, discoverableSubPartitionsCount);

        for (int i = 0; i < storageFilesCount; i++)
        {
            // Build a relative sub-directory path.
            StringBuilder subDirectory = new StringBuilder();
            String binaryString = StringUtils.leftPad(Integer.toBinaryString(i), discoverableSubPartitionsCount, "0");
            for (int j = 0; j < discoverableSubPartitionsCount; j++)
            {
                String subpartitionKey = partitionColumns.get(j + subPartitionValues.size() + 1).getName().toLowerCase();
                if (replaceUnderscoresWithHyphens)
                {
                    subpartitionKey = subpartitionKey.replace("_", "-");
                }
                subDirectory.append(String.format("/%s=%s", subpartitionKey, binaryString.substring(j, j + 1)));
            }
            // Create a storage file entity.
            storageFileDaoTestHelper
                .createStorageFileEntity(storageUnitEntity, String.format("%s%s/data.dat", s3KeyPrefix, subDirectory.toString()), FILE_SIZE_1_KB,
                    ROW_COUNT_1000);
        }
    }

    /**
     * Creates a storage policy create request.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyRuleType the storage policy rule type
     * @param storagePolicyRuleValue the storage policy rule value
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param storageName the storage name
     * @param destinationStorageName the destination storage name
     * @param storagePolicyStatus the storage policy status
     *
     * @return the newly created storage policy create request
     */
    protected StoragePolicyCreateRequest createStoragePolicyCreateRequest(StoragePolicyKey storagePolicyKey, String storagePolicyRuleType,
        Integer storagePolicyRuleValue, String businessObjectDefinitionNamespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, String storageName, String destinationStorageName, String storagePolicyStatus)
    {
        StoragePolicyCreateRequest request = new StoragePolicyCreateRequest();

        request.setStoragePolicyKey(storagePolicyKey);

        StoragePolicyRule storagePolicyRule = new StoragePolicyRule();
        request.setStoragePolicyRule(storagePolicyRule);
        storagePolicyRule.setRuleType(storagePolicyRuleType);
        storagePolicyRule.setRuleValue(storagePolicyRuleValue);

        StoragePolicyFilter storagePolicyFilter = new StoragePolicyFilter();
        request.setStoragePolicyFilter(storagePolicyFilter);
        storagePolicyFilter.setNamespace(businessObjectDefinitionNamespace);
        storagePolicyFilter.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        storagePolicyFilter.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        storagePolicyFilter.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        storagePolicyFilter.setStorageName(storageName);

        StoragePolicyTransition storagePolicyTransition = new StoragePolicyTransition();
        request.setStoragePolicyTransition(storagePolicyTransition);
        storagePolicyTransition.setDestinationStorageName(destinationStorageName);

        request.setStatus(storagePolicyStatus);

        return request;
    }

    /**
     * Creates a storage policy update request.
     *
     * @param storagePolicyRuleType the storage policy rule type
     * @param storagePolicyRuleValue the storage policy rule value
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param storageName the storage name
     * @param destinationStorageName the destination storage name
     * @param storagePolicyStatus the storage policy status
     *
     * @return the newly created storage policy create request
     */
    protected StoragePolicyUpdateRequest createStoragePolicyUpdateRequest(String storagePolicyRuleType, Integer storagePolicyRuleValue,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType,
        String storageName, String destinationStorageName, String storagePolicyStatus)
    {
        StoragePolicyUpdateRequest request = new StoragePolicyUpdateRequest();

        StoragePolicyRule storagePolicyRule = new StoragePolicyRule();
        request.setStoragePolicyRule(storagePolicyRule);
        storagePolicyRule.setRuleType(storagePolicyRuleType);
        storagePolicyRule.setRuleValue(storagePolicyRuleValue);

        StoragePolicyFilter storagePolicyFilter = new StoragePolicyFilter();
        request.setStoragePolicyFilter(storagePolicyFilter);
        storagePolicyFilter.setNamespace(businessObjectDefinitionNamespace);
        storagePolicyFilter.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        storagePolicyFilter.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        storagePolicyFilter.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        storagePolicyFilter.setStorageName(storageName);

        StoragePolicyTransition storagePolicyTransition = new StoragePolicyTransition();
        request.setStoragePolicyTransition(storagePolicyTransition);
        storagePolicyTransition.setDestinationStorageName(destinationStorageName);

        request.setStatus(storagePolicyStatus);

        return request;
    }

    /**
     * Creates a storage unit with the specified list of files.
     *
     * @param s3KeyPrefix the S3 key prefix.
     * @param files the list of files.
     * @param fileSizeBytes the file size in bytes.
     *
     * @return the storage unit with the list of file paths
     */
    protected StorageUnit createStorageUnit(String s3KeyPrefix, List<String> files, Long fileSizeBytes)
    {
        StorageUnit storageUnit = new StorageUnit();

        Storage storage = new Storage();
        storageUnit.setStorage(storage);

        storage.setName("TEST_STORAGE");
        List<StorageFile> storageFiles = new ArrayList<>();
        storageUnit.setStorageFiles(storageFiles);
        if (!CollectionUtils.isEmpty(files))
        {
            for (String file : files)
            {
                StorageFile storageFile = new StorageFile();
                storageFiles.add(storageFile);
                storageFile.setFilePath(String.format("%s/%s", s3KeyPrefix, file));
                storageFile.setFileSizeBytes(fileSizeBytes);
            }
        }
        storageUnit.setStorageUnitStatus(StorageUnitStatusEntity.ENABLED);

        return storageUnit;
    }

    /**
     * Creates business object format by calling the relative service method and using hard coded test values.
     *
     * @return the newly created business object format
     */
    protected BusinessObjectFormat createTestBusinessObjectFormat()
    {
        return createTestBusinessObjectFormat(NO_ATTRIBUTES);
    }

    /**
     * Creates business object format by calling the relative service method and using hard coded test values.
     *
     * @param attributes the attributes
     *
     * @return the newly created business object format
     */
    protected BusinessObjectFormat createTestBusinessObjectFormat(List<Attribute> attributes)
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                attributes, getTestAttributeDefinitions(), getTestSchema());

        return businessObjectFormatService.createBusinessObjectFormat(request);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    protected void createTestDatabaseEntitiesForBusinessObjectFormatTesting()
    {
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);
    }

    /**
     * Creates relative database entities required for the unit tests.
     *
     * @param namespaceCode the namespace Code
     * @param dataProviderName the data provider name
     * @param businessObjectDefinitionName the business object format definition name
     * @param businessObjectFormatFileType the business object format file type
     * @param partitionKeyGroupName the partition key group name
     */
    protected void createTestDatabaseEntitiesForBusinessObjectFormatTesting(String namespaceCode, String dataProviderName, String businessObjectDefinitionName,
        String businessObjectFormatFileType, String partitionKeyGroupName)
    {
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, dataProviderName, BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType, FORMAT_FILE_TYPE_DESCRIPTION);
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(partitionKeyGroupName);
    }

    /**
     * Creates a test "valid" business object data entry with default sub-partition values.
     *
     * @return the newly created business object data.
     */
    protected BusinessObjectDataEntity createTestValidBusinessObjectData()
    {
        return createTestValidBusinessObjectData(SUBPARTITION_VALUES, NO_ATTRIBUTE_DEFINITIONS, NO_ATTRIBUTES);
    }

    /**
     * Creates a test "valid" business object data entry.
     *
     * @param subPartitionValues the sub-partition values.
     *
     * @return the newly created business object data.
     */
    protected BusinessObjectDataEntity createTestValidBusinessObjectData(List<String> subPartitionValues, List<AttributeDefinition> attributeDefinitions,
        List<Attribute> attributes)
    {
        // Create a persisted business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);

        // If specified, add business object data attribute definitions to the business object format.
        if (!CollectionUtils.isEmpty(attributeDefinitions))
        {
            for (AttributeDefinition attributeDefinition : attributeDefinitions)
            {
                businessObjectFormatDaoTestHelper
                    .createBusinessObjectDataAttributeDefinitionEntity(businessObjectDataEntity.getBusinessObjectFormat(), attributeDefinition.getName(),
                        attributeDefinition.isPublish());
            }
        }

        // If specified, add business object data attributes to the business object data.
        if (!CollectionUtils.isEmpty(attributes))
        {
            for (Attribute attribute : attributes)
            {
                businessObjectDataAttributeDaoTestHelper
                    .createBusinessObjectDataAttributeEntity(businessObjectDataEntity, attribute.getName(), attribute.getValue());
            }
        }

        return businessObjectDataEntity;
    }

    protected UploadSingleInitiationRequest createUploadSingleInitiationRequest()
    {
        return createUploadSingleInitiationRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE, BDEF_NAME_2,
            FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, FILE_NAME);
    }

    /**
     * Creates a upload single initiation request.
     *
     * @param sourceNamespaceCode the source namespace code
     * @param sourceBusinessObjectDefinitionName the source business object definition name
     * @param sourceBusinessObjectFormatUsage the source business object usage
     * @param sourceBusinessObjectFormatFileType the source business object format file type
     * @param sourceBusinessObjectFormatVersion the source business object format version
     * @param targetNamespaceCode the target namespace code
     * @param targetBusinessObjectDefinitionName the target business object definition name
     * @param targetBusinessObjectFormatUsage the target business object usage
     * @param targetBusinessObjectFormatFileType the target business object format file type
     * @param targetBusinessObjectFormatVersion the target business object format version
     *
     * @return the newly created upload single initiation request
     */
    protected UploadSingleInitiationRequest createUploadSingleInitiationRequest(String sourceNamespaceCode, String sourceBusinessObjectDefinitionName,
        String sourceBusinessObjectFormatUsage, String sourceBusinessObjectFormatFileType, Integer sourceBusinessObjectFormatVersion,
        String targetNamespaceCode, String targetBusinessObjectDefinitionName, String targetBusinessObjectFormatUsage,
        String targetBusinessObjectFormatFileType, Integer targetBusinessObjectFormatVersion)
    {
        return createUploadSingleInitiationRequest(sourceNamespaceCode, sourceBusinessObjectDefinitionName, sourceBusinessObjectFormatUsage,
            sourceBusinessObjectFormatFileType, sourceBusinessObjectFormatVersion, targetNamespaceCode, targetBusinessObjectDefinitionName,
            targetBusinessObjectFormatUsage, targetBusinessObjectFormatFileType, targetBusinessObjectFormatVersion, FILE_NAME);
    }

    /**
     * Creates a upload single initiation request.
     *
     * @param sourceNamespaceCode the source namespace code
     * @param sourceBusinessObjectDefinitionName the source business object definition name
     * @param sourceBusinessObjectFormatUsage the source business object usage
     * @param sourceBusinessObjectFormatFileType the source business object format file type
     * @param sourceBusinessObjectFormatVersion the source business object format version
     * @param targetNamespaceCode the target namespace code
     * @param targetBusinessObjectDefinitionName the target business object definition name
     * @param targetBusinessObjectFormatUsage the target business object usage
     * @param targetBusinessObjectFormatFileType the target business object format file type
     * @param targetBusinessObjectFormatVersion the target business object format version
     * @param fileName the file name
     *
     * @return the newly created upload single initiation request
     */
    protected UploadSingleInitiationRequest createUploadSingleInitiationRequest(String sourceNamespaceCode, String sourceBusinessObjectDefinitionName,
        String sourceBusinessObjectFormatUsage, String sourceBusinessObjectFormatFileType, Integer sourceBusinessObjectFormatVersion,
        String targetNamespaceCode, String targetBusinessObjectDefinitionName, String targetBusinessObjectFormatUsage,
        String targetBusinessObjectFormatFileType, Integer targetBusinessObjectFormatVersion, String fileName)
    {
        UploadSingleInitiationRequest request = new UploadSingleInitiationRequest();

        request.setSourceBusinessObjectFormatKey(
            new BusinessObjectFormatKey(sourceNamespaceCode, sourceBusinessObjectDefinitionName, sourceBusinessObjectFormatUsage,
                sourceBusinessObjectFormatFileType, sourceBusinessObjectFormatVersion));
        request.setTargetBusinessObjectFormatKey(
            new BusinessObjectFormatKey(targetNamespaceCode, targetBusinessObjectDefinitionName, targetBusinessObjectFormatUsage,
                targetBusinessObjectFormatFileType, targetBusinessObjectFormatVersion));
        request.setBusinessObjectDataAttributes(getNewAttributes());
        request.setFile(new File(fileName, FILE_SIZE_1_KB));

        return request;
    }

    /**
     * Creates the appropriate business object data entries for an upload.
     *
     * @param businessObjectDataStatusCode the target business object data status.
     *
     * @return the upload single initiation response created during the upload flow.
     */
    protected UploadSingleInitiationResponse createUploadedFileData(String businessObjectDataStatusCode)
    {
        setLogLevel(UploadDownloadHelperServiceImpl.class, LogLevel.OFF);

        // Create source and target business object formats database entities which are required to initiate an upload.
        createDatabaseEntitiesForUploadDownloadTesting();

        // Initiate a file upload.
        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(createUploadSingleInitiationRequest());

        // Complete the upload.
        uploadDownloadService.performCompleteUploadSingleMessage(
            resultUploadSingleInitiationResponse.getSourceBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath());

        // Update the target business object data status to valid. Normally this would happen as part of the completion request, but since the status update
        // happens asynchronously, this will not happen within a unit test context which is why we are setting it explicitly.
        businessObjectDataDao.getBusinessObjectDataByAltKey(
            businessObjectDataHelper.getBusinessObjectDataKey(resultUploadSingleInitiationResponse.getTargetBusinessObjectData()))
            .setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode));
        resultUploadSingleInitiationResponse.getTargetBusinessObjectData().setStatus(businessObjectDataStatusCode);

        // Return the initiate upload single response.
        return resultUploadSingleInitiationResponse;
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
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @return the Hive DDL
     */
    protected String getBusinessObjectFormatExpectedDdl()
    {
        return getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, null, null, false, true, true);
    }

    /**
     * Creates a default valid {@link BusinessObjectDataInvalidateUnregisteredRequest} with all parameters except sub-partitions.
     *
     * @return {@link BusinessObjectDataInvalidateUnregisteredRequest}
     */
    protected BusinessObjectDataInvalidateUnregisteredRequest getDefaultBusinessObjectDataInvalidateUnregisteredRequest()
    {
        BusinessObjectDataInvalidateUnregisteredRequest request = new BusinessObjectDataInvalidateUnregisteredRequest();
        request.setNamespace(NAMESPACE);
        request.setBusinessObjectDefinitionName(BDEF_NAME);
        request.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        request.setBusinessObjectFormatVersion(FORMAT_VERSION);
        request.setPartitionValue(PARTITION_VALUE);
        request.setStorageName(StorageEntity.MANAGED_STORAGE);
        return request;
    }

    /**
     * Creates an expected business object data availability collection response using hard coded test values.
     *
     * @return the business object data availability collection response
     */
    protected BusinessObjectDataAvailabilityCollectionResponse getExpectedBusinessObjectDataAvailabilityCollectionResponse()
    {
        // Prepare a check availability collection response using hard coded test values.
        BusinessObjectDataAvailabilityCollectionResponse businessObjectDataAvailabilityCollectionResponse =
            new BusinessObjectDataAvailabilityCollectionResponse();

        // Create a list of check business object data availability responses.
        List<BusinessObjectDataAvailability> businessObjectDataAvailabilityResponses = new ArrayList<>();
        businessObjectDataAvailabilityCollectionResponse.setBusinessObjectDataAvailabilityResponses(businessObjectDataAvailabilityResponses);

        // Create a business object data availability response.
        BusinessObjectDataAvailability businessObjectDataAvailability =
            new BusinessObjectDataAvailability(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(PARTITION_KEY, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), null, DATA_VERSION, NO_STORAGE_NAMES, STORAGE_NAME, Arrays
                .asList(new BusinessObjectDataStatus(FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BusinessObjectDataStatusEntity.VALID)),
                new ArrayList<BusinessObjectDataStatus>());
        businessObjectDataAvailabilityResponses.add(businessObjectDataAvailability);

        // Set the expected values for the flags.
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataAvailable(true);
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataNotAvailable(false);

        return businessObjectDataAvailabilityCollectionResponse;
    }

    /**
     * Creates an expected generate business object data ddl collection response using hard coded test values.
     *
     * @return the business object data ddl collection response
     */
    protected BusinessObjectDataDdlCollectionResponse getExpectedBusinessObjectDataDdlCollectionResponse()
    {
        // Prepare a generate business object data collection response using hard coded test values.
        BusinessObjectDataDdlCollectionResponse businessObjectDataDdlCollectionResponse = new BusinessObjectDataDdlCollectionResponse();

        // Create a list of business object data ddl responses.
        List<BusinessObjectDataDdl> businessObjectDataDdlResponses = new ArrayList<>();
        businessObjectDataDdlCollectionResponse.setBusinessObjectDataDdlResponses(businessObjectDataDdlResponses);

        // Get the actual HIVE DDL expected to be generated.
        String expectedDdl = getExpectedHiveDdl(PARTITION_VALUE);

        // Create a business object data ddl response.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_STORAGE_NAMES,
                STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl);

        // Add two business object ddl responses to the collection response.
        businessObjectDataDdlResponses.add(expectedBusinessObjectDataDdl);
        businessObjectDataDdlResponses.add(expectedBusinessObjectDataDdl);

        // Set the expected DDL collection value.
        businessObjectDataDdlCollectionResponse.setDdlCollection(String.format("%s\n\n%s", expectedDdl, expectedDdl));

        return businessObjectDataDdlCollectionResponse;
    }

    /**
     * Returns an expected string representation of the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the string representation of the specified business object data key
     */
    protected String getExpectedBusinessObjectDataKeyAsString(BusinessObjectDataKey businessObjectDataKey)
    {
        return getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Returns an expected string representation of the specified business object data key.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartitionValues the list of subpartition values
     * @param businessObjectDataVersion the business object data version
     *
     * @return the string representation of the specified business object data key
     */
    protected String getExpectedBusinessObjectDataKeyAsString(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, List<String> subPartitionValues,
        Integer businessObjectDataVersion)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
            "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
            "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d", namespaceCode, businessObjectDefinitionName,
            businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion, partitionValue,
            CollectionUtils.isEmpty(subPartitionValues) ? "" : org.apache.commons.lang3.StringUtils.join(subPartitionValues, ","), businessObjectDataVersion);
    }

    /**
     * Returns the business object data not found error message per specified parameters.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status
     *
     * @return the business object data not found error message
     */
    protected String getExpectedBusinessObjectDataNotFoundErrorMessage(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        return String.format("Business object data {%s, businessObjectDataStatus: \"%s\"} doesn't exist.",
            getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey), businessObjectDataStatus);
    }

    /**
     * Returns the business object data not found error message per specified parameters.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartitionValues the list of subpartition values
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataStatus the business object data status
     *
     * @return the business object data not found error message
     */
    protected String getExpectedBusinessObjectDataNotFoundErrorMessage(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue,
        List<String> subPartitionValues, Integer businessObjectDataVersion, String businessObjectDataStatus)
    {
        return getExpectedBusinessObjectDataNotFoundErrorMessage(
            new BusinessObjectDataKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, subPartitionValues, businessObjectDataVersion), businessObjectDataStatus);
    }

    /**
     * Returns an expected string representation of the specified business object definition key.
     *
     * @param businessObjectDefinitionKey the key of the business object definition
     *
     * @return the string representation of the specified business object definition key
     */
    protected String getExpectedBusinessObjectDefinitionKeyAsString(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        return getExpectedBusinessObjectDefinitionKeyAsString(businessObjectDefinitionKey.getNamespace(),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName());
    }

    /**
     * Returns an expected string representation of the specified business object definition key.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the string representation of the specified business object definition key
     */
    protected String getExpectedBusinessObjectDefinitionKeyAsString(String namespace, String businessObjectDefinitionName)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\"", namespace, businessObjectDefinitionName);
    }

    /**
     * Returns the business object definition not found error message per specified parameters.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the business object definition not found error message
     */
    protected String getExpectedBusinessObjectDefinitionNotFoundErrorMessage(String namespace, String businessObjectDefinitionName)
    {
        return String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", businessObjectDefinitionName, namespace);
    }

    /**
     * Creates an expected generate business object format ddl collection response using hard coded test values.
     *
     * @return the business object format ddl collection response
     */
    protected BusinessObjectFormatDdlCollectionResponse getExpectedBusinessObjectFormatDdlCollectionResponse()
    {
        // Prepare a generate business object data collection response using hard coded test values.
        BusinessObjectFormatDdlCollectionResponse businessObjectFormatDdlCollectionResponse = new BusinessObjectFormatDdlCollectionResponse();

        // Create a list of business object data ddl responses.
        List<BusinessObjectFormatDdl> businessObjectFormatDdlResponses = new ArrayList<>();
        businessObjectFormatDdlCollectionResponse.setBusinessObjectFormatDdlResponses(businessObjectFormatDdlResponses);

        // Get the actual HIVE DDL expected to be generated.
        String expectedDdl = getExpectedHiveDdl(null);

        // Create a business object data ddl response.
        BusinessObjectFormatDdl expectedBusinessObjectFormatDdl =
            new BusinessObjectFormatDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, expectedDdl);

        // Add two business object ddl responses to the collection response.
        businessObjectFormatDdlResponses.add(expectedBusinessObjectFormatDdl);
        businessObjectFormatDdlResponses.add(expectedBusinessObjectFormatDdl);

        // Set the expected DDL collection value.
        businessObjectFormatDdlCollectionResponse.setDdlCollection(String.format("%s\n\n%s", expectedDdl, expectedDdl));

        return businessObjectFormatDdlCollectionResponse;
    }

    /**
     * Returns an expected string representation of the specified business object format key.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the string representation of the specified business object format key
     */
    protected String getExpectedBusinessObjectFormatKeyAsString(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
            "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d", namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage,
            businessObjectFormatFileType, businessObjectFormatVersion);
    }

    /**
     * Returns the business object format not found error message per specified parameters.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the business object format not found error message
     */
    protected String getExpectedBusinessObjectFormatNotFoundErrorMessage(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion)
    {
        return String.format("Business object format with namespace \"%s\", business object definition name \"%s\"," +
            " format usage \"%s\", format file type \"%s\", and format version \"%d\" doesn't exist.", namespaceCode, businessObjectDefinitionName,
            businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion);
    }

    /**
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @return the Hive DDL
     */
    protected String getExpectedDdl()
    {
        return getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, STORAGE_1_AVAILABLE_PARTITION_VALUES, SUBPARTITION_VALUES,
            false, true, true);
    }

    /**
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @return the Hive DDL
     */
    protected String getExpectedDdl(int partitionLevels, String firstColumnName, String firstColumnDataType, String hiveRowFormat, String hiveFileFormat,
        String businessObjectFormatFileType, int partitionColumnPosition, List<String> partitionValues, List<String> subPartitionValues,
        boolean replaceUnderscoresWithHyphens, boolean isDropStatementIncluded, boolean isIfNotExistsOptionIncluded)
    {
        return getExpectedDdl(partitionLevels, firstColumnName, firstColumnDataType, hiveRowFormat, hiveFileFormat, businessObjectFormatFileType,
            partitionColumnPosition, partitionValues, subPartitionValues, replaceUnderscoresWithHyphens, isDropStatementIncluded, isIfNotExistsOptionIncluded,
            NO_INCLUDE_DROP_PARTITIONS);
    }

    /**
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @param partitionLevels the number of partition levels
     * @param firstColumnName the name of the first schema column
     * @param firstColumnDataType the data type of the first schema column
     * @param hiveRowFormat the Hive row format
     * @param hiveFileFormat the Hive file format
     * @param businessObjectFormatFileType the business object format file type
     * @param partitionColumnPosition the position of the partition column
     * @param partitionValues the list of partition values
     * @param subPartitionValues the list of subpartition values
     * @param replaceUnderscoresWithHyphens specifies if we need to replace underscores with hyphens in subpartition key values when building subpartition
     * location path
     * @param isDropStatementIncluded specifies if expected DDL should include a drop table statement
     * @param isDropPartitionsStatementsIncluded specifies if expected DDL should include the relative drop partition statements
     *
     * @return the Hive DDL
     */
    protected String getExpectedDdl(int partitionLevels, String firstColumnName, String firstColumnDataType, String hiveRowFormat, String hiveFileFormat,
        String businessObjectFormatFileType, int partitionColumnPosition, List<String> partitionValues, List<String> subPartitionValues,
        boolean replaceUnderscoresWithHyphens, boolean isDropStatementIncluded, boolean isIfNotExistsOptionIncluded, boolean isDropPartitionsStatementsIncluded)
    {
        StringBuilder sb = new StringBuilder();

        if (isDropStatementIncluded)
        {
            sb.append("DROP TABLE IF EXISTS `[Table Name]`;\n\n");
        }
        sb.append("CREATE EXTERNAL TABLE [If Not Exists]`[Table Name]` (\n");
        sb.append(String.format("    `%s` %s,\n", firstColumnName, firstColumnDataType));
        sb.append("    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. ");
        sb.append("Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n");
        sb.append("    `COLUMN003` INT,\n");
        sb.append("    `COLUMN004` BIGINT,\n");
        sb.append("    `COLUMN005` FLOAT,\n");
        sb.append("    `COLUMN006` DOUBLE,\n");
        sb.append("    `COLUMN007` DECIMAL,\n");
        sb.append("    `COLUMN008` DECIMAL(p,s),\n");
        sb.append("    `COLUMN009` DECIMAL,\n");
        sb.append("    `COLUMN010` DECIMAL(p),\n");
        sb.append("    `COLUMN011` DECIMAL(p,s),\n");
        sb.append("    `COLUMN012` TIMESTAMP,\n");
        sb.append("    `COLUMN013` DATE,\n");
        sb.append("    `COLUMN014` STRING,\n");
        sb.append("    `COLUMN015` VARCHAR(n),\n");
        sb.append("    `COLUMN016` VARCHAR(n),\n");
        sb.append("    `COLUMN017` CHAR(n),\n");
        sb.append("    `COLUMN018` BOOLEAN,\n");
        sb.append("    `COLUMN019` BINARY)\n");

        if (partitionLevels > 0)
        {
            if (partitionLevels > 1)
            {
                // Multiple level partitioning.
                sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE, `PRTN_CLMN002` STRING, `PRTN_CLMN003` INT, `PRTN_CLMN004` DECIMAL, " +
                    "`PRTN_CLMN005` BOOLEAN, `PRTN_CLMN006` DECIMAL, `PRTN_CLMN007` DECIMAL)\n");
            }
            else
            {
                // Single level partitioning.
                sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE)\n");
            }
        }

        sb.append("[Row Format]\n");
        sb.append(String.format("STORED AS [Hive File Format]%s\n", partitionLevels > 0 ? ";" : ""));

        if (partitionLevels > 0)
        {
            // Add partitions if we have a non-empty list of partition values.
            if (!CollectionUtils.isEmpty(partitionValues))
            {
                // Add drop partition statements.
                if (isDropPartitionsStatementsIncluded)
                {
                    sb.append("\n");

                    for (String partitionValue : partitionValues)
                    {
                        sb.append(String
                            .format("ALTER TABLE `[Table Name]` DROP IF EXISTS PARTITION (`PRTN_CLMN00%d`='%s');\n", partitionColumnPosition, partitionValue));
                    }
                }

                sb.append("\n");

                for (String partitionValue : partitionValues)
                {
                    if (partitionLevels > 1)
                    {
                        // Adjust expected partition values based on the partition column position.
                        String testPrimaryPartitionValue =
                            partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION ? partitionValue : PARTITION_VALUE;
                        List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                        if (partitionColumnPosition > BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
                        {
                            testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                        }

                        // Multiple level partitioning.
                        if (partitionLevels == SUBPARTITION_VALUES.size() + 1)
                        {
                            // No auto-discovery.
                            sb.append(String.format("ALTER TABLE `[Table Name]` ADD [If Not Exists]PARTITION (`PRTN_CLMN001`='%s', `PRTN_CLMN002`='%s', " +
                                "`PRTN_CLMN003`='%s', `PRTN_CLMN004`='%s', `PRTN_CLMN005`='%s') " +
                                "LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                                "/[Format File Type]/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]" +
                                "/data-v[Data Version]/prtn-clmn001=%s/prtn-clmn002=%s/prtn-clmn003=%s/prtn-clmn004=%s/prtn-clmn005=%s';\n",
                                testPrimaryPartitionValue, testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2),
                                testSubPartitionValues.get(3), getExpectedS3BucketName(partitionValue), testPrimaryPartitionValue,
                                testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2), testSubPartitionValues.get(3)));
                        }
                        else
                        {
                            // Auto-discovery test template.
                            for (String binaryString : Arrays.asList("00", "01", "10", "11"))
                            {
                                sb.append(String.format("ALTER TABLE `[Table Name]` ADD [If Not Exists]PARTITION (`PRTN_CLMN001`='%s', `PRTN_CLMN002`='%s', " +
                                    "`PRTN_CLMN003`='%s', `PRTN_CLMN004`='%s', `PRTN_CLMN005`='%s', `PRTN_CLMN006`='%s', `PRTN_CLMN007`='%s') " +
                                    "LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                                    "/[Format File Type]/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]" +
                                    "/data-v[Data Version]/prtn-clmn001=%s/prtn-clmn002=%s/prtn-clmn003=%s/prtn-clmn004=%s/prtn-clmn005=%s/" +
                                    (replaceUnderscoresWithHyphens ? "prtn-clmn006" : "prtn_clmn006") + "=%s/" +
                                    (replaceUnderscoresWithHyphens ? "prtn-clmn007" : "prtn_clmn007") + "=%s';\n", testPrimaryPartitionValue,
                                    testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2), testSubPartitionValues.get(3),
                                    binaryString.substring(0, 1), binaryString.substring(1, 2), getExpectedS3BucketName(partitionValue),
                                    testPrimaryPartitionValue, testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2),
                                    testSubPartitionValues.get(3), binaryString.substring(0, 1), binaryString.substring(1, 2)));
                            }
                        }
                    }
                    else
                    {
                        // Single level partitioning.
                        sb.append(String.format("ALTER TABLE `[Table Name]` ADD [If Not Exists]PARTITION (`PRTN_CLMN001`='%s') " +
                            "LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                            "/[Format File Type]/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]" +
                            "/data-v[Data Version]/prtn-clmn001=%s';\n", partitionValue, getExpectedS3BucketName(partitionValue), partitionValue));
                    }
                }
            }
        }
        else if (!CollectionUtils.isEmpty(partitionValues))
        {
            // Add a location statement since the table is not partitioned and we have a non-empty list of partition values.
            sb.append(String.format("LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                "/txt/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]/data-v[Data Version]/partition=none';",
                getExpectedS3BucketName(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE)));
        }
        else
        {
            // Add a location statement for a non-partitioned table for the business object format dll unit tests.
            sb.append("LOCATION '${non-partitioned.table.location}';");
        }

        String ddlTemplate = sb.toString().trim();
        Pattern pattern = Pattern.compile("\\[(.+?)\\]");
        Matcher matcher = pattern.matcher(ddlTemplate);
        HashMap<String, String> replacements = new HashMap<>();

        // Populate the replacements map.
        replacements.put("Table Name", TABLE_NAME);
        replacements.put("Random Suffix", RANDOM_SUFFIX);
        replacements.put("Format Version", String.valueOf(FORMAT_VERSION));
        replacements.put("Data Version", String.valueOf(DATA_VERSION));
        replacements.put("Row Format", hiveRowFormat);
        replacements.put("Hive File Format", hiveFileFormat);
        replacements.put("Format File Type", businessObjectFormatFileType.toLowerCase());
        replacements.put("If Not Exists", isIfNotExistsOptionIncluded ? "IF NOT EXISTS " : "");

        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find())
        {
            String replacement = replacements.get(matcher.group(1));
            builder.append(ddlTemplate.substring(i, matcher.start()));
            if (replacement == null)
            {
                builder.append(matcher.group(0));
            }
            else
            {
                builder.append(replacement);
            }
            i = matcher.end();
        }

        builder.append(ddlTemplate.substring(i, ddlTemplate.length()));

        return builder.toString();
    }

    /**
     * Returns the actual HIVE DDL expected to be generated.
     *
     * @return the actual HIVE DDL expected to be generated
     */
    protected String getExpectedHiveDdl(String partitionValue)
    {
        // Build ddl expected to be generated.
        StringBuilder ddlBuilder = new StringBuilder();
        ddlBuilder.append("DROP TABLE IF EXISTS `" + TABLE_NAME + "`;\n");
        ddlBuilder.append("\n");
        ddlBuilder.append("CREATE EXTERNAL TABLE IF NOT EXISTS `" + TABLE_NAME + "` (\n");
        ddlBuilder.append("    `ORGNL_" + FIRST_PARTITION_COLUMN_NAME + "` DATE,\n");
        ddlBuilder.append("    `" + COLUMN_NAME + "` DECIMAL(" + COLUMN_SIZE + ") COMMENT '" + COLUMN_DESCRIPTION + "')\n");
        ddlBuilder.append("PARTITIONED BY (`" + FIRST_PARTITION_COLUMN_NAME + "` DATE)\n");
        ddlBuilder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'\n");
        ddlBuilder.append("STORED AS TEXTFILE;");

        if (partitionValue != null)
        {
            // Add the alter table drop partition statement.
            ddlBuilder.append("\n\n");
            ddlBuilder.append("ALTER TABLE `" + TABLE_NAME + "` DROP IF EXISTS PARTITION (`" + FIRST_PARTITION_COLUMN_NAME + "`='" + partitionValue + "');");

            // Build an expected S3 key prefix.
            String expectedS3KeyPrefix =
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    FIRST_PARTITION_COLUMN_NAME, partitionValue, null, null, DATA_VERSION);

            // Add the alter table add partition statement.
            ddlBuilder.append("\n\n");
            ddlBuilder.append("ALTER TABLE `" + TABLE_NAME + "` ADD IF NOT EXISTS PARTITION (`" + FIRST_PARTITION_COLUMN_NAME + "`='" + partitionValue +
                "') LOCATION 's3n://" + S3_BUCKET_NAME + "/" + expectedS3KeyPrefix + "';");
        }

        String expectedDdl = ddlBuilder.toString();

        return expectedDdl;
    }

    /**
     * Returns the actual HIVE DDL expected to be generated.
     *
     * @param partitions the list of partitions, where each is represented by a primary value and a sub-partition value
     *
     * @return the actual HIVE DDL expected to be generated
     */
    protected String getExpectedHiveDdlTwoPartitionLevels(List<List<String>> partitions)
    {
        // Build ddl expected to be generated.
        StringBuilder ddlBuilder = new StringBuilder();
        ddlBuilder.append("DROP TABLE IF EXISTS `" + TABLE_NAME + "`;\n");
        ddlBuilder.append("\n");
        ddlBuilder.append("CREATE EXTERNAL TABLE IF NOT EXISTS `" + TABLE_NAME + "` (\n");
        ddlBuilder.append("    `ORGNL_" + FIRST_PARTITION_COLUMN_NAME + "` DATE,\n");
        ddlBuilder.append("    `ORGNL_" + SECOND_PARTITION_COLUMN_NAME + "` STRING,\n");
        ddlBuilder.append("    `" + COLUMN_NAME + "` DECIMAL(" + COLUMN_SIZE + ") COMMENT '" + COLUMN_DESCRIPTION + "')\n");
        ddlBuilder.append("PARTITIONED BY (`" + FIRST_PARTITION_COLUMN_NAME + "` DATE, `" + SECOND_PARTITION_COLUMN_NAME + "` STRING)\n");
        ddlBuilder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'\n");
        ddlBuilder.append("STORED AS TEXTFILE;");

        // Add the alter table drop partition statement.
        ddlBuilder.append("\n\n");
        ddlBuilder
            .append("ALTER TABLE `" + TABLE_NAME + "` DROP IF EXISTS PARTITION (`" + FIRST_PARTITION_COLUMN_NAME + "`='" + partitions.get(0).get(0) + "');");
        ddlBuilder.append("\n");

        for (List<String> partition : partitions)
        {
            // Build an expected S3 key prefix.
            String expectedS3KeyPrefix =
                getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                    FIRST_PARTITION_COLUMN_NAME, partition.get(0), Arrays.asList(
                    new SchemaColumn(SECOND_PARTITION_COLUMN_NAME, "STRING", NO_COLUMN_SIZE, COLUMN_REQUIRED, NO_COLUMN_DEFAULT_VALUE, NO_COLUMN_DESCRIPTION))
                    .toArray(new SchemaColumn[1]), Arrays.asList(partition.get(1)).toArray(new String[1]), DATA_VERSION);

            // Add the alter table add partition statement.
            ddlBuilder.append("\n");
            ddlBuilder.append("ALTER TABLE `" + TABLE_NAME + "` ADD IF NOT EXISTS PARTITION (`" + FIRST_PARTITION_COLUMN_NAME + "`='" + partition.get(0) +
                "', `" + SECOND_PARTITION_COLUMN_NAME + "`='" + partition.get(1) + "') LOCATION 's3n://" + S3_BUCKET_NAME + "/" + expectedS3KeyPrefix + "';");
        }

        String expectedDdl = ddlBuilder.toString();

        return expectedDdl;
    }

    protected String getExpectedS3BucketName(String partitionValue)
    {
        if (STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue) || Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE.equals(partitionValue))
        {
            return S3_BUCKET_NAME;
        }
        else
        {
            return S3_BUCKET_NAME_2;
        }
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
    protected String getExpectedS3KeyPrefix(String namespaceCd, String dataProviderName, String businessObjectDefinitionName, String formatUsage,
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

    /**
     * Returns an expected string representation of the specified storage policy key and version.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     *
     * @return the string representation of the specified storage policy key
     */
    protected String getExpectedStoragePolicyKeyAndVersionAsString(StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
    {
        return String.format("namespace: \"%s\", storagePolicyName: \"%s\", storagePolicyVersion: \"%d\"", storagePolicyKey.getNamespace(),
            storagePolicyKey.getStoragePolicyName(), storagePolicyVersion);
    }

    /**
     * Returns a list of all possible invalid partition filters based on the presence of partition value filter elements. This helper method is for all negative
     * test cases covering partition value filter having none or more than one partition filter option specified.
     */
    protected List<PartitionValueFilter> getInvalidPartitionValueFilters()
    {
        return Arrays.asList(new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(),
                new LatestAfterPartitionValue()),
            new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                NO_LATEST_BEFORE_PARTITION_VALUE, new LatestAfterPartitionValue()),
            new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                new LatestBeforePartitionValue(), NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(PARTITION_KEY, NO_PARTITION_VALUES, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                new LatestBeforePartitionValue(), new LatestAfterPartitionValue()),
            new PartitionValueFilter(PARTITION_KEY, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                new LatestAfterPartitionValue()),
            new PartitionValueFilter(PARTITION_KEY, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(),
                NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(PARTITION_KEY, UNSORTED_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(),
                new LatestAfterPartitionValue()),
            new PartitionValueFilter(PARTITION_KEY, UNSORTED_PARTITION_VALUES, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(PARTITION_KEY, UNSORTED_PARTITION_VALUES, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                NO_LATEST_BEFORE_PARTITION_VALUE, new LatestAfterPartitionValue()),
            new PartitionValueFilter(PARTITION_KEY, UNSORTED_PARTITION_VALUES, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                new LatestBeforePartitionValue(), NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(PARTITION_KEY, UNSORTED_PARTITION_VALUES, new PartitionValueRange(START_PARTITION_VALUE, END_PARTITION_VALUE),
                new LatestBeforePartitionValue(), new LatestAfterPartitionValue()));
    }

    /**
     * Gets a new list of attributes.
     *
     * @return the list of attributes.
     */
    protected List<Attribute> getNewAttributes()
    {
        List<Attribute> attributes = new ArrayList<>();

        attributes.add(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2));
        attributes.add(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3));

        return attributes;
    }

    /**
     * Gets a second set of test attributes.
     *
     * @return the list of attributes
     */
    protected List<Attribute> getNewAttributes2()
    {
        List<Attribute> attributes = new ArrayList<>();

        // Attribute 1 has a new value compared to the first set of test attributes.
        attributes.add(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1_UPDATED));

        // Attribute 2 is missing compared to the first set of the test attributes.

        // Attribute 3 is identical to the one from the first set of the test attributes.
        attributes.add(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3));

        // Attribute 4 is not present in the first set of the test attributes.
        attributes.add(new Attribute(ATTRIBUTE_NAME_4_MIXED_CASE, ATTRIBUTE_VALUE_4));

        return attributes;
    }

    /**
     * Gets a new business object data create request with attributes and attribute definitions.
     *
     * @return the business object create request.
     */
    protected BusinessObjectDataCreateRequest getNewBusinessObjectDataCreateRequest()
    {
        return getNewBusinessObjectDataCreateRequest(true);
    }

    /**
     * Gets a newly created business object data create request.
     *
     * @param includeAttributes If true, attribute definitions and attributes will be included. Otherwise, not.
     *
     * @return the business object create request.
     */
    protected BusinessObjectDataCreateRequest getNewBusinessObjectDataCreateRequest(boolean includeAttributes)
    {
        // Crete a test business object format (and associated data).
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(includeAttributes);

        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();

        // Create a request to create business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = new BusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataCreateRequest.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectDataCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectDataCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectDataCreateRequest.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectDataCreateRequest.setPartitionKey(businessObjectFormatEntity.getPartitionKey());
        businessObjectDataCreateRequest.setPartitionValue(PARTITION_VALUE);
        businessObjectDataCreateRequest.setSubPartitionValues(SUBPARTITION_VALUES);

        List<StorageUnitCreateRequest> storageUnits = new ArrayList<>();
        businessObjectDataCreateRequest.setStorageUnits(storageUnits);

        StorageUnitCreateRequest storageUnit = new StorageUnitCreateRequest();
        storageUnits.add(storageUnit);
        storageUnit.setStorageName(storageEntity.getName());

        StorageDirectory storageDirectory = new StorageDirectory();
        storageUnit.setStorageDirectory(storageDirectory);
        storageDirectory.setDirectoryPath("Folder");

        List<StorageFile> storageFiles = new ArrayList<>();
        storageUnit.setStorageFiles(storageFiles);

        StorageFile storageFile1 = new StorageFile();
        storageFiles.add(storageFile1);
        storageFile1.setFilePath("Folder/file1.gz");
        storageFile1.setFileSizeBytes(0L);
        storageFile1.setRowCount(0L);

        StorageFile storageFile2 = new StorageFile();
        storageFiles.add(storageFile2);
        storageFile2.setFilePath("Folder/file2.gz");
        storageFile2.setFileSizeBytes(2999L);
        storageFile2.setRowCount(1000L);

        StorageFile storageFile3 = new StorageFile();
        storageFiles.add(storageFile3);
        storageFile3.setFilePath("Folder/file3.gz");
        storageFile3.setFileSizeBytes(Long.MAX_VALUE);
        storageFile3.setRowCount(Long.MAX_VALUE);

        if (includeAttributes)
        {
            businessObjectDataCreateRequest.setAttributes(getNewAttributes());
        }

        List<BusinessObjectDataKey> businessObjectDataParents = new ArrayList<>();
        businessObjectDataCreateRequest.setBusinessObjectDataParents(businessObjectDataParents);

        // Create 2 parents.
        for (int i = 0; i < 2; i++)
        {
            BusinessObjectDataEntity parentBusinessObjectDataEntity = businessObjectDataDaoTestHelper.createBusinessObjectDataEntity();
            BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
            businessObjectDataKey.setNamespace(parentBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
            businessObjectDataKey
                .setBusinessObjectDefinitionName(parentBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
            businessObjectDataKey.setBusinessObjectFormatUsage(parentBusinessObjectDataEntity.getBusinessObjectFormat().getUsage());
            businessObjectDataKey.setBusinessObjectFormatFileType(parentBusinessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
            businessObjectDataKey.setBusinessObjectFormatVersion(parentBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
            businessObjectDataKey.setPartitionValue(parentBusinessObjectDataEntity.getPartitionValue());
            businessObjectDataKey.setBusinessObjectDataVersion(parentBusinessObjectDataEntity.getVersion());
            businessObjectDataKey.setSubPartitionValues(businessObjectDataHelper.getSubPartitionValues(parentBusinessObjectDataEntity));

            businessObjectDataParents.add(businessObjectDataKey);
        }

        return businessObjectDataCreateRequest;
    }

    protected BusinessObjectDataStorageFilesCreateRequest getNewBusinessObjectDataStorageFilesCreateRequest()
    {
        // Crete a test business object format (and associated data).
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(false);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION, BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, DATA_VERSION, true, businessObjectDataStatusEntity.getCode());
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create a request to create business object data.
        BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest = new BusinessObjectDataStorageFilesCreateRequest();
        businessObjectDataStorageFilesCreateRequest.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectDataStorageFilesCreateRequest.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());

        businessObjectDataStorageFilesCreateRequest.setStorageName(storageEntity.getName());

        List<StorageFile> storageFiles = new ArrayList<>();
        businessObjectDataStorageFilesCreateRequest.setStorageFiles(storageFiles);

        StorageFile storageFile1 = new StorageFile();
        storageFiles.add(storageFile1);
        storageFile1.setFilePath("Folder/file1.gz");
        storageFile1.setFileSizeBytes(0L);
        storageFile1.setRowCount(0L);

        StorageFile storageFile2 = new StorageFile();
        storageFiles.add(storageFile2);
        storageFile2.setFilePath("Folder/file2.gz");
        storageFile2.setFileSizeBytes(2999L);
        storageFile2.setRowCount(1000L);

        StorageFile storageFile3 = new StorageFile();
        storageFiles.add(storageFile3);
        storageFile3.setFilePath("Folder/file3.gz");
        storageFile3.setFileSizeBytes(Long.MAX_VALUE);
        storageFile3.setRowCount(Long.MAX_VALUE);

        return businessObjectDataStorageFilesCreateRequest;
    }

    /**
     * Returns a list of attribute definitions that use hard coded test values.
     *
     * @return the list of test attribute definitions
     */
    protected List<AttributeDefinition> getTestAttributeDefinitions()
    {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        attributeDefinitions.add(new AttributeDefinition(ATTRIBUTE_NAME_1_MIXED_CASE, NO_PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(ATTRIBUTE_NAME_2_MIXED_CASE, NO_PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(ATTRIBUTE_NAME_3_MIXED_CASE, PUBLISH_ATTRIBUTE));

        return attributeDefinitions;
    }

    /**
     * Creates a check business object data availability collection request using hard coded test values.
     *
     * @return the business object data availability collection request
     */
    protected BusinessObjectDataAvailabilityCollectionRequest getTestBusinessObjectDataAvailabilityCollectionRequest()
    {
        // Create a check business object data availability collection request.
        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest = new BusinessObjectDataAvailabilityCollectionRequest();

        // Create a list of check business object data availability requests.
        List<BusinessObjectDataAvailabilityRequest> businessObjectDataAvailabilityRequests = new ArrayList<>();
        businessObjectDataAvailabilityCollectionRequest.setBusinessObjectDataAvailabilityRequests(businessObjectDataAvailabilityRequests);

        // Create a business object data availability request.
        BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest =
            new BusinessObjectDataAvailabilityRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(PARTITION_KEY, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                    NO_LATEST_AFTER_PARTITION_VALUE)), null, DATA_VERSION, NO_STORAGE_NAMES, STORAGE_NAME, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS);
        businessObjectDataAvailabilityRequests.add(businessObjectDataAvailabilityRequest);

        return businessObjectDataAvailabilityCollectionRequest;
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data availability request
     */
    protected BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(List<String> partitionValues)
    {
        return getTestBusinessObjectDataAvailabilityRequest(FIRST_PARTITION_COLUMN_NAME, null, null, partitionValues);
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param partitionKey the partition key
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data availability request
     */
    protected BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(String partitionKey, List<String> partitionValues)
    {
        return getTestBusinessObjectDataAvailabilityRequest(partitionKey, null, null, partitionValues);
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     *
     * @return the newly created business object data availability request
     */
    protected BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(String startPartitionValue, String endPartitionValue)
    {
        return getTestBusinessObjectDataAvailabilityRequest(FIRST_PARTITION_COLUMN_NAME, startPartitionValue, endPartitionValue, null);
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param partitionKey the partition key
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data availability request
     */
    protected BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(String partitionKey, String startPartitionValue,
        String endPartitionValue, List<String> partitionValues)
    {
        BusinessObjectDataAvailabilityRequest request = new BusinessObjectDataAvailabilityRequest();

        request.setNamespace(NAMESPACE);
        request.setBusinessObjectDefinitionName(BDEF_NAME);
        request.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        request.setBusinessObjectFormatVersion(FORMAT_VERSION);

        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        request.setPartitionValueFilters(Arrays.asList(partitionValueFilter));
        partitionValueFilter.setPartitionKey(partitionKey);

        if (startPartitionValue != null || endPartitionValue != null)
        {
            PartitionValueRange partitionValueRange = new PartitionValueRange();
            partitionValueFilter.setPartitionValueRange(partitionValueRange);
            partitionValueRange.setStartPartitionValue(startPartitionValue);
            partitionValueRange.setEndPartitionValue(endPartitionValue);
        }

        if (partitionValues != null)
        {
            partitionValueFilter.setPartitionValues(new ArrayList<>(partitionValues));
        }

        request.setBusinessObjectDataVersion(DATA_VERSION);
        request.setStorageName(STORAGE_NAME);
        request.setIncludeAllRegisteredSubPartitions(NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS);

        return request;
    }

    /**
     * Creates a generate business object data ddl collection request using hard coded test values.
     *
     * @return the business object data ddl collection request
     */
    protected BusinessObjectDataDdlCollectionRequest getTestBusinessObjectDataDdlCollectionRequest()
    {
        // Create a generate business object data ddl collection request.
        BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest = new BusinessObjectDataDdlCollectionRequest();

        // Create a list of generate business object data ddl requests.
        List<BusinessObjectDataDdlRequest> businessObjectDataDdlRequests = new ArrayList<>();
        businessObjectDataDdlCollectionRequest.setBusinessObjectDataDdlRequests(businessObjectDataDdlRequests);

        // Create a generate business object data ddl request.
        BusinessObjectDataDdlRequest businessObjectDataDdlRequest =
            new BusinessObjectDataDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(FIRST_PARTITION_COLUMN_NAME, Arrays.asList(PARTITION_VALUE), NO_PARTITION_VALUE_RANGE,
                    NO_LATEST_BEFORE_PARTITION_VALUE, NO_LATEST_AFTER_PARTITION_VALUE)), NO_STANDALONE_PARTITION_VALUE_FILTER, DATA_VERSION, NO_STORAGE_NAMES,
                STORAGE_NAME, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT,
                INCLUDE_IF_NOT_EXISTS_OPTION, INCLUDE_DROP_PARTITIONS, NO_ALLOW_MISSING_DATA, NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                NO_INCLUDE_ARCHIVED_BUSINESS_OBJECT_DATA);

        // Add two business object ddl requests to the collection request.
        businessObjectDataDdlRequests.add(businessObjectDataDdlRequest);
        businessObjectDataDdlRequests.add(businessObjectDataDdlRequest);

        return businessObjectDataDdlCollectionRequest;
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data ddl request
     */
    protected BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(List<String> partitionValues)
    {
        return getTestBusinessObjectDataDdlRequest(null, null, partitionValues, NO_CUSTOM_DDL_NAME);
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     *
     * @return the newly created business object data ddl request
     */
    protected BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(String startPartitionValue, String endPartitionValue)
    {
        return getTestBusinessObjectDataDdlRequest(startPartitionValue, endPartitionValue, null, NO_CUSTOM_DDL_NAME);
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param partitionValues the list of partition values
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object data ddl request
     */
    protected BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(List<String> partitionValues, String customDdlName)
    {
        return getTestBusinessObjectDataDdlRequest(null, null, partitionValues, customDdlName);
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range * @param customDdlName the custom DDL name
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object data ddl request
     */
    protected BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(String startPartitionValue, String endPartitionValue, String customDdlName)
    {
        return getTestBusinessObjectDataDdlRequest(startPartitionValue, endPartitionValue, null, customDdlName);
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     * @param partitionValues the list of partition values
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object data ddl request
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(String startPartitionValue, String endPartitionValue,
        List<String> partitionValues, String customDdlName)
    {
        BusinessObjectDataDdlRequest request = new BusinessObjectDataDdlRequest();

        request.setNamespace(NAMESPACE);
        request.setBusinessObjectDefinitionName(BDEF_NAME);
        request.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FileTypeEntity.TXT_FILE_TYPE);
        request.setBusinessObjectFormatVersion(FORMAT_VERSION);

        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        request.setPartitionValueFilters(Arrays.asList(partitionValueFilter));
        partitionValueFilter.setPartitionKey(FIRST_PARTITION_COLUMN_NAME);

        if (startPartitionValue != null || endPartitionValue != null)
        {
            PartitionValueRange partitionValueRange = new PartitionValueRange();
            partitionValueFilter.setPartitionValueRange(partitionValueRange);
            partitionValueRange.setStartPartitionValue(startPartitionValue);
            partitionValueRange.setEndPartitionValue(endPartitionValue);
        }

        if (partitionValues != null)
        {
            partitionValueFilter.setPartitionValues(new ArrayList(partitionValues));
        }

        request.setBusinessObjectDataVersion(DATA_VERSION);
        request.setStorageName(STORAGE_NAME);
        request.setOutputFormat(BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL);
        request.setTableName(TABLE_NAME);
        request.setCustomDdlName(customDdlName);
        request.setIncludeDropTableStatement(true);
        request.setIncludeIfNotExistsOption(true);
        request.setAllowMissingData(true);
        request.setIncludeAllRegisteredSubPartitions(NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS);
        request.setIncludeArchivedBusinessObjectData(NO_INCLUDE_ARCHIVED_BUSINESS_OBJECT_DATA);

        return request;
    }

    /**
     * Creates and returns a list of business object data status elements initialised per provided parameters.
     *
     * @param businessObjectFormatVersion the business object format version
     * @param partitionColumnPosition the position of the partition column (one-based numbering)
     * @param partitionValues the list of partition values
     * @param subPartitionValues the list of subpartition values
     * @param businessObjectDataVersion the business object data version
     * @param reason the reason for the not available business object data
     * @param useSinglePartitionValue specifies if not available statuses should be generated using single partition value logic
     *
     * @return the newly created list of business object data status elements
     */
    protected List<BusinessObjectDataStatus> getTestBusinessObjectDataStatuses(Integer businessObjectFormatVersion, int partitionColumnPosition,
        List<String> partitionValues, List<String> subPartitionValues, Integer businessObjectDataVersion, String reason, boolean useSinglePartitionValue)
    {
        List<BusinessObjectDataStatus> businessObjectDataStatuses = new ArrayList<>();

        if (partitionValues != null)
        {
            for (String partitionValue : partitionValues)
            {
                BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();
                businessObjectDataStatuses.add(businessObjectDataStatus);
                businessObjectDataStatus.setBusinessObjectFormatVersion(businessObjectFormatVersion);
                businessObjectDataStatus.setBusinessObjectDataVersion(businessObjectDataVersion);
                businessObjectDataStatus.setReason(reason);

                if (BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED.equals(reason))
                {
                    // We are generating business object data status for a not registered business object data.
                    if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
                    {
                        // This is a not-available not-registered business object data searched on a primary partition.
                        businessObjectDataStatus.setPartitionValue(partitionValue);
                        businessObjectDataStatus.setSubPartitionValues(useSinglePartitionValue ? null : Arrays.asList("", "", "", ""));
                    }
                    else
                    {
                        // This is a not-available not-registered business object data searched on a sub-partition value.
                        if (useSinglePartitionValue)
                        {
                            businessObjectDataStatus.setPartitionValue(partitionValue);
                        }
                        else
                        {
                            businessObjectDataStatus.setPartitionValue("");
                            businessObjectDataStatus.setSubPartitionValues(Arrays.asList("", "", "", ""));
                            businessObjectDataStatus.getSubPartitionValues().set(partitionColumnPosition - 2, partitionValue);
                        }
                    }
                }
                else if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
                {
                    // This is a found business object data selected on primary partition value.
                    businessObjectDataStatus.setPartitionValue(partitionValue);
                    businessObjectDataStatus.setSubPartitionValues(subPartitionValues);
                }
                else
                {
                    // This is a found business object data selected on a subpartition column.
                    businessObjectDataStatus.setPartitionValue(PARTITION_VALUE);
                    List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                    // Please note that the value of the second partition column is located at index 0.
                    testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                    businessObjectDataStatus.setSubPartitionValues(testSubPartitionValues);
                }
            }
        }

        return businessObjectDataStatuses;
    }

    /**
     * Creates a generate business object format ddl collection request using hard coded test values.
     *
     * @return the business object format ddl collection request
     */
    protected BusinessObjectFormatDdlCollectionRequest getTestBusinessObjectFormatDdlCollectionRequest()
    {
        // Create a generate business object format ddl collection request.
        BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest = new BusinessObjectFormatDdlCollectionRequest();

        // Create a list of generate business object format ddl requests.
        List<BusinessObjectFormatDdlRequest> businessObjectFormatDdlRequests = new ArrayList<>();
        businessObjectFormatDdlCollectionRequest.setBusinessObjectFormatDdlRequests(businessObjectFormatDdlRequests);

        // Create a generate business object format ddl request.
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest =
            new BusinessObjectFormatDdlRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, NO_CUSTOM_DDL_NAME, INCLUDE_DROP_TABLE_STATEMENT, INCLUDE_IF_NOT_EXISTS_OPTION,
                null);

        // Add two business object ddl requests to the collection request.
        businessObjectFormatDdlRequests.add(businessObjectFormatDdlRequest);
        businessObjectFormatDdlRequests.add(businessObjectFormatDdlRequest);

        return businessObjectFormatDdlCollectionRequest;
    }

    /**
     * Creates and returns a business object format ddl request using passed parameters along with some hard-coded test values.
     *
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object format ddl request
     */
    protected BusinessObjectFormatDdlRequest getTestBusinessObjectFormatDdlRequest(String customDdlName)
    {
        BusinessObjectFormatDdlRequest request = new BusinessObjectFormatDdlRequest();

        request.setNamespace(NAMESPACE);
        request.setBusinessObjectDefinitionName(BDEF_NAME);
        request.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FileTypeEntity.TXT_FILE_TYPE);
        request.setBusinessObjectFormatVersion(FORMAT_VERSION);
        request.setOutputFormat(BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL);
        request.setTableName(TABLE_NAME);
        request.setCustomDdlName(customDdlName);
        request.setIncludeDropTableStatement(true);
        request.setIncludeIfNotExistsOption(true);

        return request;
    }

    /**
     * Returns the Hive custom DDL.
     *
     * @param partitioned specifies whether the table the custom DDL is for is partitioned or not
     *
     * @return the custom Hive DDL
     */
    protected String getTestCustomDdl(boolean partitioned)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS `${table.name}` (\n");
        sb.append("    `COLUMN001` TINYINT,\n");
        sb.append("    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. ");
        sb.append("Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n");
        sb.append("    `COLUMN003` INT,\n");
        sb.append("    `COLUMN004` BIGINT,\n");
        sb.append("    `COLUMN005` FLOAT,\n");
        sb.append("    `COLUMN006` DOUBLE,\n");
        sb.append("    `COLUMN007` DECIMAL,\n");
        sb.append("    `COLUMN008` DECIMAL(p,s),\n");
        sb.append("    `COLUMN009` DECIMAL,\n");
        sb.append("    `COLUMN010` DECIMAL(p),\n");
        sb.append("    `COLUMN011` DECIMAL(p,s),\n");
        sb.append("    `COLUMN012` TIMESTAMP,\n");
        sb.append("    `COLUMN013` DATE,\n");
        sb.append("    `COLUMN014` STRING,\n");
        sb.append("    `COLUMN015` VARCHAR(n),\n");
        sb.append("    `COLUMN016` VARCHAR(n),\n");
        sb.append("    `COLUMN017` CHAR(n),\n");
        sb.append("    `COLUMN018` BOOLEAN,\n");
        sb.append("    `COLUMN019` BINARY)\n");

        if (partitioned)
        {
            sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE, `PRTN_CLMN002` STRING, `PRTN_CLMN003` INT, `PRTN_CLMN004` DECIMAL, " +
                "`PRTN_CLMN005` BOOLEAN, `PRTN_CLMN006` DECIMAL, `PRTN_CLMN007` DECIMAL)\n");
        }

        sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'\n");

        if (partitioned)
        {
            sb.append("STORED AS TEXTFILE;");
        }
        else
        {
            sb.append("STORED AS TEXTFILE\n");
            sb.append("LOCATION '${non-partitioned.table.location}';");
        }

        return sb.toString();
    }

    /**
     * Returns a business object format schema that uses hard coded test values.
     *
     * @return the test business object format schema
     */
    protected Schema getTestSchema()
    {
        Schema schema = new Schema();

        schema.setNullValue(SCHEMA_NULL_VALUE_BACKSLASH_N);
        schema.setDelimiter(SCHEMA_DELIMITER_PIPE);
        schema.setEscapeCharacter(SCHEMA_ESCAPE_CHARACTER_BACKSLASH);
        schema.setPartitionKeyGroup(PARTITION_KEY_GROUP);
        schema.setColumns(schemaColumnDaoTestHelper.getTestSchemaColumns(RANDOM_SUFFIX));
        schema.setPartitions(schemaColumnDaoTestHelper.getTestPartitionColumns(RANDOM_SUFFIX));

        return schema;
    }

    /**
     * Returns a business object format schema that uses hard coded test values.
     *
     * @return the test business object format schema
     */
    protected Schema getTestSchema2()
    {
        Schema schema = new Schema();

        schema.setNullValue(SCHEMA_NULL_VALUE_NULL_WORD);
        schema.setDelimiter(SCHEMA_DELIMITER_COMMA);
        schema.setEscapeCharacter(SCHEMA_ESCAPE_CHARACTER_TILDE);
        schema.setPartitionKeyGroup(PARTITION_KEY_GROUP_2);
        schema.setColumns(schemaColumnDaoTestHelper.getTestSchemaColumns(RANDOM_SUFFIX_2));
        schema.setPartitions(schemaColumnDaoTestHelper.getTestPartitionColumns(RANDOM_SUFFIX_2));

        return schema;
    }

    /**
     * Builds and returns a list of test storage file object instances.
     *
     * @param s3KeyPrefix the S3 key prefix
     * @param relativeFilePaths the list of relative file paths that might include sub-directories
     *
     * @return the newly created list of storage files
     */
    protected List<StorageFile> getTestStorageFiles(String s3KeyPrefix, List<String> relativeFilePaths)
    {
        return getTestStorageFiles(s3KeyPrefix, relativeFilePaths, true);
    }

    /**
     * Builds and returns a list of test storage file object instances.
     *
     * @param s3KeyPrefix the S3 key prefix
     * @param relativeFilePaths the list of relative file paths that might include sub-directories,
     * @param setRowCount specifies if some storage files should get row count attribute set to a hard coded test value
     *
     * @return the newly created list of storage files
     */
    protected List<StorageFile> getTestStorageFiles(String s3KeyPrefix, List<String> relativeFilePaths, boolean setRowCount)
    {
        // Build a list of storage files.
        List<StorageFile> storageFiles = new ArrayList<>();

        for (String file : relativeFilePaths)
        {
            StorageFile storageFile = new StorageFile();
            storageFiles.add(storageFile);
            storageFile.setFilePath(s3KeyPrefix + "/" + file.replaceAll("\\\\", "/"));
            storageFile.setFileSizeBytes(FILE_SIZE_1_KB);

            if (setRowCount)
            {
                // Row count is an optional field, so let's not set it for one of the storage files - this is required for code coverage.
                storageFile.setRowCount(file.equals(LOCAL_FILES.get(0)) ? null : ROW_COUNT_1000);
            }
        }

        return storageFiles;
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
     * Creates specified list of files in the local temporary directory and uploads them to the test S3 bucket.
     *
     * @param s3keyPrefix the destination S3 key prefix
     * @param localFilePaths the list of local files that might include sub-directories
     *
     * @throws Exception
     */
    protected void prepareTestS3Files(String s3keyPrefix, List<String> localFilePaths) throws Exception
    {
        prepareTestS3Files(s3keyPrefix, localFilePaths, new ArrayList<String>());
    }

    /**
     * Creates specified list of files in the local temporary directory and uploads them to the test S3 bucket. This method also creates 0 byte S3 directory
     * markers relative to the s3 key prefix.
     *
     * @param s3KeyPrefix the destination S3 key prefix
     * @param localFilePaths the list of local files that might include sub-directories
     * @param directoryPaths the list of directory paths to be created in S3 relative to the S3 key prefix
     *
     * @throws Exception
     */
    protected void prepareTestS3Files(String s3KeyPrefix, List<String> localFilePaths, List<String> directoryPaths) throws Exception
    {
        prepareTestS3Files(null, s3KeyPrefix, localFilePaths, directoryPaths);
    }

    /**
     * Creates specified list of files in the local temporary directory and uploads them to the test S3 bucket. This method also creates 0 byte S3 directory
     * markers relative to the s3 key prefix.
     *
     * @param bucketName the bucket name in S3 to place the files.
     * @param s3KeyPrefix the destination S3 key prefix
     * @param localFilePaths the list of local files that might include sub-directories
     * @param directoryPaths the list of directory paths to be created in S3 relative to the S3 key prefix
     *
     * @throws Exception
     */
    protected void prepareTestS3Files(String bucketName, String s3KeyPrefix, List<String> localFilePaths, List<String> directoryPaths) throws Exception
    {
        // Create local test files.
        for (String file : localFilePaths)
        {
            createLocalFile(localTempPath.toString(), file, FILE_SIZE_1_KB);
        }

        // Upload test file to S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        if (bucketName != null)
        {
            s3FileTransferRequestParamsDto.setS3BucketName(bucketName);
        }
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Service.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate the transfer result.
        assertEquals(Long.valueOf(localFilePaths.size()), results.getTotalFilesTransferred());

        // Create 0 byte S3 directory markers.
        for (String directoryPath : directoryPaths)
        {
            // Create 0 byte directory marker.
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix + "/" + directoryPath);
            s3Service.createDirectory(s3FileTransferRequestParamsDto);
        }

        // Validate the uploaded S3 files and created directory markers, if any.
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
        assertEquals(localFilePaths.size() + directoryPaths.size(), s3Service.listDirectory(s3FileTransferRequestParamsDto).size());
    }

    /**
     * Sets specified namespace authorizations for the current user by updating the security context.
     *
     * @param namespace the namespace
     * @param namespacePermissions the list of namespace permissions
     */
    protected void setCurrentUserNamespaceAuthorizations(String namespace, List<NamespacePermissionEnum> namespacePermissions)
    {
        String username = USER_ID;
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();
        namespaceAuthorizations.add(new NamespaceAuthorization(namespace, namespacePermissions));
        applicationUser.setNamespaceAuthorizations(namespaceAuthorizations);
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));
    }

    /**
     * Validates a list of Attributes against the expected values.
     *
     * @param expectedAttributes the list of expected Attributes
     * @param actualAttributes the list of actual Attributes to be validated
     */
    protected void validateAttributes(List<Attribute> expectedAttributes, List<Attribute> actualAttributes)
    {
        assertEquals(expectedAttributes.size(), actualAttributes.size());
        for (int i = 0; i < expectedAttributes.size(); i++)
        {
            Attribute expectedAttribute = expectedAttributes.get(i);
            Attribute actualAttribute = actualAttributes.get(i);
            assertEquals(expectedAttribute.getName(), actualAttribute.getName());
            assertEquals(expectedAttribute.getValue(), actualAttribute.getValue());
        }
    }

    /**
     * Validates business object data against specified arguments and expected (hard coded) test values.
     *
     * @param request the business object data create request
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectData(BusinessObjectDataCreateRequest request, Integer expectedBusinessObjectDataVersion, Boolean expectedLatestVersion,
        BusinessObjectData actualBusinessObjectData)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(org.apache.commons.lang3.StringUtils.isNotBlank(request.getNamespace()) ? request.getNamespace() : NAMESPACE,
                request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
                request.getBusinessObjectFormatVersion()));

        List<String> expectedSubPartitionValues =
            CollectionUtils.isEmpty(request.getSubPartitionValues()) ? new ArrayList<String>() : request.getSubPartitionValues();

        String expectedStatusCode =
            org.apache.commons.lang3.StringUtils.isNotBlank(request.getStatus()) ? request.getStatus() : BusinessObjectDataStatusEntity.VALID;

        StorageUnitCreateRequest storageUnitCreateRequest = request.getStorageUnits().get(0);

        StorageEntity storageEntity = storageDao.getStorageByName(storageUnitCreateRequest.getStorageName());

        String expectedStorageDirectoryPath =
            storageUnitCreateRequest.getStorageDirectory() != null ? storageUnitCreateRequest.getStorageDirectory().getDirectoryPath() : null;

        List<StorageFile> expectedStorageFiles =
            CollectionUtils.isEmpty(storageUnitCreateRequest.getStorageFiles()) ? null : storageUnitCreateRequest.getStorageFiles();

        List<Attribute> expectedAttributes = CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<Attribute>() : request.getAttributes();

        validateBusinessObjectData(businessObjectFormatEntity, request.getPartitionValue(), expectedSubPartitionValues, expectedBusinessObjectDataVersion,
            expectedLatestVersion, expectedStatusCode, storageEntity.getName(), expectedStorageDirectoryPath, expectedStorageFiles, expectedAttributes,
            actualBusinessObjectData);
    }

    /**
     * Validates business object data against specified arguments and expected (hard coded) test values.
     *
     * @param businessObjectFormatEntity the business object format entity that this business object data belongs to
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param expectedStorageName the expected storage name
     * @param expectedStorageDirectoryPath the expected storage directory path
     * @param expectedStorageFiles the expected storage files
     * @param expectedAttributes the expected attributes
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectData(BusinessObjectFormatEntity businessObjectFormatEntity, String expectedBusinessObjectDataPartitionValue,
        List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion, Boolean expectedLatestVersion,
        String expectedStatusCode, String expectedStorageName, String expectedStorageDirectoryPath, List<StorageFile> expectedStorageFiles,
        List<Attribute> expectedAttributes, BusinessObjectData actualBusinessObjectData)
    {
        validateBusinessObjectData(null, businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(),
            expectedBusinessObjectDataPartitionValue, expectedBusinessObjectDataSubPartitionValues, expectedBusinessObjectDataVersion, expectedLatestVersion,
            expectedStatusCode, expectedStorageName, expectedStorageDirectoryPath, expectedStorageFiles, expectedAttributes, actualBusinessObjectData);
    }

    /**
     * Validates business object data against specified arguments and expected (hard coded) test values.
     *
     * @param expectedBusinessObjectDataId the expected business object data ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param expectedStorageName the expected storage name
     * @param expectedStorageDirectoryPath the expected storage directory path
     * @param expectedStorageFiles the expected storage files
     * @param expectedAttributes the expected attributes
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectData(Integer expectedBusinessObjectDataId, String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion,
        Boolean expectedLatestVersion, String expectedStatusCode, String expectedStorageName, String expectedStorageDirectoryPath,
        List<StorageFile> expectedStorageFiles, List<Attribute> expectedAttributes, BusinessObjectData actualBusinessObjectData)
    {
        validateBusinessObjectData(expectedBusinessObjectDataId, expectedNamespace, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage,
            expectedBusinessObjectFormatFileType, expectedBusinessObjectFormatVersion, expectedBusinessObjectDataPartitionValue,
            expectedBusinessObjectDataSubPartitionValues, expectedBusinessObjectDataVersion, expectedLatestVersion, expectedStatusCode,
            actualBusinessObjectData);

        // We expected test business object data to contain a single storage unit.
        assertEquals(1, actualBusinessObjectData.getStorageUnits().size());
        StorageUnit actualStorageUnit = actualBusinessObjectData.getStorageUnits().get(0);

        assertEquals(expectedStorageName, actualStorageUnit.getStorage().getName());
        assertEquals(expectedStorageDirectoryPath,
            actualStorageUnit.getStorageDirectory() != null ? actualStorageUnit.getStorageDirectory().getDirectoryPath() : null);
        assertEqualsIgnoreOrder("storage files", expectedStorageFiles, actualStorageUnit.getStorageFiles());

        assertEquals(expectedAttributes, actualBusinessObjectData.getAttributes());
    }

    /**
     * Validates business object data against specified arguments.
     *
     * @param expectedBusinessObjectDataId the expected business object data ID
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectData(Integer expectedBusinessObjectDataId, BusinessObjectDataKey expectedBusinessObjectDataKey,
        Boolean expectedLatestVersion, String expectedStatusCode, BusinessObjectData actualBusinessObjectData)
    {
        validateBusinessObjectData(expectedBusinessObjectDataId, expectedBusinessObjectDataKey.getNamespace(),
            expectedBusinessObjectDataKey.getBusinessObjectDefinitionName(), expectedBusinessObjectDataKey.getBusinessObjectFormatUsage(),
            expectedBusinessObjectDataKey.getBusinessObjectFormatFileType(), expectedBusinessObjectDataKey.getBusinessObjectFormatVersion(),
            expectedBusinessObjectDataKey.getPartitionValue(), expectedBusinessObjectDataKey.getSubPartitionValues(),
            expectedBusinessObjectDataKey.getBusinessObjectDataVersion(), expectedLatestVersion, expectedStatusCode, actualBusinessObjectData);
    }

    /**
     * Validates business object data against specified arguments.
     *
     * @param expectedBusinessObjectDataId the expected business object data ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectData(Integer expectedBusinessObjectDataId, String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion,
        Boolean expectedLatestVersion, String expectedStatusCode, BusinessObjectData actualBusinessObjectData)
    {
        assertNotNull(actualBusinessObjectData);

        if (expectedBusinessObjectDataId != null)
        {
            assertEquals(expectedBusinessObjectDataId, Integer.valueOf(actualBusinessObjectData.getId()));
        }

        assertEquals(expectedNamespace, actualBusinessObjectData.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectData.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectData.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectData.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, Integer.valueOf(actualBusinessObjectData.getBusinessObjectFormatVersion()));
        assertEquals(expectedBusinessObjectDataPartitionValue, actualBusinessObjectData.getPartitionValue());
        assertEquals(expectedBusinessObjectDataSubPartitionValues, actualBusinessObjectData.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, Integer.valueOf(actualBusinessObjectData.getVersion()));
        assertEquals(expectedLatestVersion, actualBusinessObjectData.isLatestVersion());
        assertEquals(expectedStatusCode, actualBusinessObjectData.getStatus());
    }

    protected void validateBusinessObjectData(String expectedNamespaceCode, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataStatus, List<Attribute> expectedAttributes, String expectedStorageName, String expectedFileName,
        Long expectedFileSizeBytes, BusinessObjectData businessObjectData)
    {
        assertNotNull(businessObjectData);

        // Validate business object data alternate key values.
        assertEquals(expectedNamespaceCode, businessObjectData.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, businessObjectData.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, businessObjectData.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, businessObjectData.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, Integer.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        // The business object data partition value must contain an UUID value.
        assertNotNull(businessObjectData.getPartitionValue());
        assertEquals(EXPECTED_UUID_SIZE, businessObjectData.getPartitionValue().length());
        assertEquals(NO_SUBPARTITION_VALUES, businessObjectData.getSubPartitionValues());
        assertEquals(INITIAL_DATA_VERSION, Integer.valueOf(businessObjectData.getVersion()));

        // Validate business object data status.
        assertTrue(businessObjectData.isLatestVersion());
        assertEquals(expectedBusinessObjectDataStatus, businessObjectData.getStatus());

        // Validate business object data attributes.
        validateAttributes(expectedAttributes, businessObjectData.getAttributes());

        // Validate storage unit contents.
        assertEquals(1, businessObjectData.getStorageUnits().size());
        StorageUnit storageUnit = businessObjectData.getStorageUnits().get(0);
        assertEquals(expectedStorageName, storageUnit.getStorage().getName());
        String expectedStorageDirectoryPath = String
            .format("%s/%s/%s", ENVIRONMENT_NAME.trim().toLowerCase().replace('_', '-'), expectedNamespaceCode.trim().toLowerCase().replace('_', '-'),
                businessObjectData.getPartitionValue());
        assertEquals(expectedStorageDirectoryPath, storageUnit.getStorageDirectory().getDirectoryPath());
        assertEquals(1, storageUnit.getStorageFiles().size());
        StorageFile storageFile = storageUnit.getStorageFiles().get(0);
        String expectedStorageFilePath = String.format("%s/%s", expectedStorageDirectoryPath, expectedFileName);
        assertEquals(expectedStorageFilePath, storageFile.getFilePath());
        assertEquals(expectedFileSizeBytes, storageFile.getFileSizeBytes());
        assertEquals(null, storageFile.getRowCount());
    }

    /**
     * Validates business object data attribute contents against specified arguments.
     *
     * @param businessObjectDataAttributeId the expected business object data attribute ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedBusinessObjectDataAttributeName the expected business object data attribute name
     * @param expectedBusinessObjectDataAttributeValue the expected business object data attribute value
     * @param actualBusinessObjectDataAttribute the business object data attribute object instance to be validated
     */
    protected void validateBusinessObjectDataAttribute(Integer businessObjectDataAttributeId, String expectedNamespace,
        String expectedBusinessObjectDefinitionName, String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType,
        Integer expectedBusinessObjectFormatVersion, String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues,
        Integer expectedBusinessObjectDataVersion, String expectedBusinessObjectDataAttributeName, String expectedBusinessObjectDataAttributeValue,
        BusinessObjectDataAttribute actualBusinessObjectDataAttribute)
    {
        assertNotNull(actualBusinessObjectDataAttribute);
        if (businessObjectDataAttributeId != null)
        {
            assertEquals(businessObjectDataAttributeId, Integer.valueOf(actualBusinessObjectDataAttribute.getId()));
        }
        validateBusinessObjectDataAttributeKey(expectedNamespace, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage,
            expectedBusinessObjectFormatFileType, expectedBusinessObjectFormatVersion, expectedBusinessObjectDataPartitionValue,
            expectedBusinessObjectDataSubPartitionValues, expectedBusinessObjectDataVersion, expectedBusinessObjectDataAttributeName,
            actualBusinessObjectDataAttribute.getBusinessObjectDataAttributeKey());
        assertEquals(expectedBusinessObjectDataAttributeValue, actualBusinessObjectDataAttribute.getBusinessObjectDataAttributeValue());
    }

    /**
     * Validates business object data attribute key against specified arguments.
     *
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedBusinessObjectDataAttributeName the expected business object data attribute name
     * @param actualBusinessObjectDataAttributeKey the business object data attribute key object instance to be validated
     */
    protected void validateBusinessObjectDataAttributeKey(String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion,
        String expectedBusinessObjectDataAttributeName, BusinessObjectDataAttributeKey actualBusinessObjectDataAttributeKey)
    {
        assertNotNull(actualBusinessObjectDataAttributeKey);

        assertEquals(expectedNamespace, actualBusinessObjectDataAttributeKey.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectDataAttributeKey.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectDataAttributeKey.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectDataAttributeKey.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualBusinessObjectDataAttributeKey.getBusinessObjectFormatVersion());
        assertEquals(expectedBusinessObjectDataPartitionValue, actualBusinessObjectDataAttributeKey.getPartitionValue());
        assertEquals(expectedBusinessObjectDataSubPartitionValues, actualBusinessObjectDataAttributeKey.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, actualBusinessObjectDataAttributeKey.getBusinessObjectDataVersion());
        assertEquals(expectedBusinessObjectDataAttributeName, actualBusinessObjectDataAttributeKey.getBusinessObjectDataAttributeName());
    }

    /**
     * Validates business object data availability against specified arguments and expected (hard coded) test values.
     *
     * @param request the business object data availability request
     * @param actualBusinessObjectDataAvailability the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest request,
        List<BusinessObjectDataStatus> expectedAvailableStatuses, List<BusinessObjectDataStatus> expectedNotAvailableStatuses,
        BusinessObjectDataAvailability actualBusinessObjectDataAvailability)
    {
        assertNotNull(actualBusinessObjectDataAvailability);
        assertEquals(request.getNamespace(), actualBusinessObjectDataAvailability.getNamespace());
        assertEquals(request.getBusinessObjectDefinitionName(), actualBusinessObjectDataAvailability.getBusinessObjectDefinitionName());
        assertEquals(request.getBusinessObjectFormatUsage(), actualBusinessObjectDataAvailability.getBusinessObjectFormatUsage());
        assertEquals(request.getBusinessObjectFormatFileType(), actualBusinessObjectDataAvailability.getBusinessObjectFormatFileType());
        assertEquals(request.getBusinessObjectFormatVersion(), actualBusinessObjectDataAvailability.getBusinessObjectFormatVersion());
        assertEquals(request.getPartitionValueFilter(), actualBusinessObjectDataAvailability.getPartitionValueFilter());
        assertEquals(request.getBusinessObjectDataVersion(), actualBusinessObjectDataAvailability.getBusinessObjectDataVersion());
        assertEquals(request.getStorageName(), actualBusinessObjectDataAvailability.getStorageName());
        assertEquals(expectedAvailableStatuses, actualBusinessObjectDataAvailability.getAvailableStatuses());
        assertEquals(expectedNotAvailableStatuses, actualBusinessObjectDataAvailability.getNotAvailableStatuses());
    }

    /**
     * Validates business object data ddl object instance against specified arguments and expected (hard coded) test values.
     *
     * @param request the business object ddl request
     * @param actualBusinessObjectDataDdl the business object data ddl object instance to be validated
     */
    protected void validateBusinessObjectDataDdl(BusinessObjectDataDdlRequest request, String expectedDdl, BusinessObjectDataDdl actualBusinessObjectDataDdl)
    {
        assertNotNull(actualBusinessObjectDataDdl);
        assertEquals(request.getNamespace(), actualBusinessObjectDataDdl.getNamespace());
        assertEquals(request.getBusinessObjectDefinitionName(), actualBusinessObjectDataDdl.getBusinessObjectDefinitionName());
        assertEquals(request.getBusinessObjectFormatUsage(), actualBusinessObjectDataDdl.getBusinessObjectFormatUsage());
        assertEquals(request.getBusinessObjectFormatFileType(), actualBusinessObjectDataDdl.getBusinessObjectFormatFileType());
        assertEquals(request.getBusinessObjectFormatVersion(), actualBusinessObjectDataDdl.getBusinessObjectFormatVersion());
        assertEquals(request.getPartitionValueFilter(), actualBusinessObjectDataDdl.getPartitionValueFilter());
        assertEquals(request.getBusinessObjectDataVersion(), actualBusinessObjectDataDdl.getBusinessObjectDataVersion());
        assertEquals(request.getStorageName(), actualBusinessObjectDataDdl.getStorageName());
        assertEquals(request.getOutputFormat(), actualBusinessObjectDataDdl.getOutputFormat());
        assertEquals(request.getTableName(), actualBusinessObjectDataDdl.getTableName());
        assertEquals(expectedDdl, actualBusinessObjectDataDdl.getDdl());
    }

    /**
     * Validates business object data key against specified arguments.
     *
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param actualBusinessObjectDataKey the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectDataKey(String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion,
        BusinessObjectDataKey actualBusinessObjectDataKey)
    {
        assertNotNull(actualBusinessObjectDataKey);
        assertEquals(expectedNamespace, actualBusinessObjectDataKey.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectDataKey.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectDataKey.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectDataKey.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualBusinessObjectDataKey.getBusinessObjectFormatVersion());
        assertEquals(expectedBusinessObjectDataPartitionValue, actualBusinessObjectDataKey.getPartitionValue());
        assertEquals(expectedBusinessObjectDataSubPartitionValues, actualBusinessObjectDataKey.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, actualBusinessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Validates business object data notification registration key against specified arguments.
     *
     * @param expectedNamespaceCode the expected namespace code
     * @param expectedNotificationName the expected notification name
     * @param actualBusinessObjectDataNotificationRegistrationKey the business object data notification registration key object instance to be validated
     */
    protected void validateBusinessObjectDataNotificationRegistrationKey(String expectedNamespaceCode, String expectedNotificationName,
        NotificationRegistrationKey actualBusinessObjectDataNotificationRegistrationKey)
    {
        assertNotNull(actualBusinessObjectDataNotificationRegistrationKey);
        assertEquals(expectedNamespaceCode, actualBusinessObjectDataNotificationRegistrationKey.getNamespace());
        assertEquals(expectedNotificationName, actualBusinessObjectDataNotificationRegistrationKey.getNotificationName());
    }

    /**
     * Validates a business object data restore DTO against the specified arguments.
     *
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedOriginStorageName the expected origin storage name
     * @param expectedOriginBucketName the expected origin S3 bucket name
     * @param expectedOriginS3KeyPrefix the expected origin S3 key prefix
     * @param expectedOriginStorageFiles the expected list of origin storage files
     * @param expectedGlacierStorageName the expected Glacier storage name
     * @param expectedGlacierBucketName the expected Glacier S3 bucket name
     * @param expectedGlacierS3KeyBasePrefix the expected Glacier S3 key base prefix
     * @param expectedGlacierS3KeyPrefix the expected Glacier S3 key prefix
     * @param expectedException the expected exception
     * @param actualBusinessObjectDataRestoreDto the business object data restore DTO to be validated
     */
    protected void validateBusinessObjectDataRestoreDto(BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedOriginStorageName,
        String expectedOriginBucketName, String expectedOriginS3KeyPrefix, List<StorageFile> expectedOriginStorageFiles, String expectedGlacierStorageName,
        String expectedGlacierBucketName, String expectedGlacierS3KeyBasePrefix, String expectedGlacierS3KeyPrefix, Exception expectedException,
        BusinessObjectDataRestoreDto actualBusinessObjectDataRestoreDto)
    {
        assertNotNull(actualBusinessObjectDataRestoreDto);
        assertEquals(expectedBusinessObjectDataKey, actualBusinessObjectDataRestoreDto.getBusinessObjectDataKey());
        assertEquals(expectedOriginStorageName, actualBusinessObjectDataRestoreDto.getOriginStorageName());
        assertEquals(expectedOriginBucketName, actualBusinessObjectDataRestoreDto.getOriginBucketName());
        assertEquals(expectedOriginS3KeyPrefix, actualBusinessObjectDataRestoreDto.getOriginS3KeyPrefix());
        assertEquals(expectedOriginStorageFiles, actualBusinessObjectDataRestoreDto.getOriginStorageFiles());
        assertEquals(expectedGlacierStorageName, actualBusinessObjectDataRestoreDto.getGlacierStorageName());
        assertEquals(expectedGlacierBucketName, actualBusinessObjectDataRestoreDto.getGlacierBucketName());
        assertEquals(expectedGlacierS3KeyBasePrefix, actualBusinessObjectDataRestoreDto.getGlacierS3KeyBasePrefix());
        assertEquals(expectedGlacierS3KeyPrefix, actualBusinessObjectDataRestoreDto.getGlacierS3KeyPrefix());
        assertEquals(expectedException, actualBusinessObjectDataRestoreDto.getException());
    }

    /**
     * Validates that the business object data status change message is valid.
     *
     * @param message the message to be validated.
     * @param businessObjectDataKey the business object data key.
     * @param businessObjectDataId the business object data Id.
     * @param username the username.
     * @param newBusinessObjectDataStatus the new business object data status.
     * @param oldBusinessObjectDataStatus the old business object data status.
     * @param businessObjectDataAttributes the list of business object data attributes.
     */
    protected void validateBusinessObjectDataStatusChangeMessage(String message, BusinessObjectDataKey businessObjectDataKey, Integer businessObjectDataId,
        String username, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus, List<Attribute> businessObjectDataAttributes)
    {
        validateXmlFieldPresent(message, "correlation-id", "BusinessObjectData_" + businessObjectDataId);
        validateXmlFieldPresent(message, "triggered-by-username", username);
        validateXmlFieldPresent(message, "context-message-type", "testDomain/testApplication/BusinessObjectDataStatusChanged");
        validateXmlFieldPresent(message, "newBusinessObjectDataStatus", newBusinessObjectDataStatus);

        if (oldBusinessObjectDataStatus == null)
        {
            validateXmlFieldNotPresent(message, "oldBusinessObjectDataStatus");
        }
        else
        {
            validateXmlFieldPresent(message, "oldBusinessObjectDataStatus", oldBusinessObjectDataStatus);
        }

        validateXmlFieldPresent(message, "namespace", businessObjectDataKey.getNamespace());
        validateXmlFieldPresent(message, "businessObjectDefinitionName", businessObjectDataKey.getBusinessObjectDefinitionName());
        validateXmlFieldPresent(message, "businessObjectFormatUsage", businessObjectDataKey.getBusinessObjectFormatUsage());
        validateXmlFieldPresent(message, "businessObjectFormatFileType", businessObjectDataKey.getBusinessObjectFormatFileType());
        validateXmlFieldPresent(message, "businessObjectFormatVersion", businessObjectDataKey.getBusinessObjectFormatVersion());
        validateXmlFieldPresent(message, "partitionValue", businessObjectDataKey.getPartitionValue());

        if (CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()))
        {
            validateXmlFieldNotPresent(message, "subPartitionValues");
        }
        else
        {
            validateXmlFieldPresent(message, "subPartitionValues");
        }

        for (String subPartitionValue : businessObjectDataKey.getSubPartitionValues())
        {
            validateXmlFieldPresent(message, "partitionValue", subPartitionValue);
        }

        if (CollectionUtils.isEmpty(businessObjectDataAttributes))
        {
            validateXmlFieldNotPresent(message, "attributes");
        }
        else
        {
            validateXmlFieldPresent(message, "attributes");
        }

        for (Attribute attribute : businessObjectDataAttributes)
        {
            // Validate each expected "<attribute>" XML tag. Please note that null attribute value is expected to be published as an empty string.
            validateXmlFieldPresent(message, "attribute", "name", attribute.getName(), attribute.getValue() == null ? EMPTY_STRING : attribute.getValue());
        }

        validateXmlFieldPresent(message, "businessObjectDataVersion", businessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Validates the contents of a business object data status information against the specified parameters.
     *
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedBusinessObjectDataStatus the expected business object data status
     * @param businessObjectDataStatusInformation the actual business object data status information
     */
    protected void validateBusinessObjectDataStatusInformation(BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedBusinessObjectDataStatus,
        BusinessObjectDataStatusInformation businessObjectDataStatusInformation)
    {
        assertNotNull(businessObjectDataStatusInformation);
        assertEquals(expectedBusinessObjectDataKey, businessObjectDataStatusInformation.getBusinessObjectDataKey());
        assertEquals(expectedBusinessObjectDataStatus, businessObjectDataStatusInformation.getStatus());
    }

    /**
     * Validates the contents of a business object data status update response against the specified parameters.
     *
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedBusinessObjectDataStatus the expected business object data status
     * @param expectedPreviousBusinessObjectDataStatus the expected previous business object data status
     * @param actualResponse the actual business object data status update response
     */
    protected void validateBusinessObjectDataStatusUpdateResponse(BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedBusinessObjectDataStatus,
        String expectedPreviousBusinessObjectDataStatus, BusinessObjectDataStatusUpdateResponse actualResponse)
    {
        assertNotNull(actualResponse);
        assertEquals(expectedBusinessObjectDataKey, actualResponse.getBusinessObjectDataKey());
        assertEquals(expectedBusinessObjectDataStatus, actualResponse.getStatus());
        assertEquals(expectedPreviousBusinessObjectDataStatus, actualResponse.getPreviousStatus());
    }

    /**
     * Validates business object data storage files create response contents against specified parameters.
     *
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedPartitionValue the expected partition value
     * @param expectedSubPartitionValues the expected subpartition values
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedStorageName the expected storage name
     * @param expectedStorageFiles the list of expected storage files
     * @param actualResponse the business object data storage files create response to be validated
     */
    protected void validateBusinessObjectDataStorageFilesCreateResponse(String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedPartitionValue, List<String> expectedSubPartitionValues, Integer expectedBusinessObjectDataVersion, String expectedStorageName,
        List<StorageFile> expectedStorageFiles, BusinessObjectDataStorageFilesCreateResponse actualResponse)
    {
        assertNotNull(actualResponse);
        assertEquals(expectedNamespace, actualResponse.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualResponse.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualResponse.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualResponse.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualResponse.getBusinessObjectFormatVersion());
        assertEquals(expectedPartitionValue, actualResponse.getPartitionValue());
        assertEquals(expectedSubPartitionValues, actualResponse.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, actualResponse.getBusinessObjectDataVersion());
        assertEquals(expectedStorageName, actualResponse.getStorageName());
        assertEquals(expectedStorageFiles, actualResponse.getStorageFiles());
    }

    /**
     * Validates business object definition contents against specified arguments.
     *
     * @param expectedBusinessObjectDefinitionId the expected business object definition ID
     * @param expectedNamespaceCode the expected namespace code
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedDataProviderName the expected data provider name
     * @param expectedBusinessObjectDefinitionDescription the expected business object definition description
     * @param expectedAttributes the expected list of attributes
     * @param actualBusinessObjectDefinition the business object definition object instance to be validated
     */
    protected void validateBusinessObjectDefinition(Integer expectedBusinessObjectDefinitionId, String expectedNamespaceCode,
        String expectedBusinessObjectDefinitionName, String expectedDataProviderName, String expectedBusinessObjectDefinitionDescription,
        List<Attribute> expectedAttributes, BusinessObjectDefinition actualBusinessObjectDefinition)
    {
        assertNotNull(actualBusinessObjectDefinition);
        if (expectedBusinessObjectDefinitionId != null)
        {
            assertEquals(expectedBusinessObjectDefinitionId, Integer.valueOf(actualBusinessObjectDefinition.getId()));
        }
        assertEquals(expectedNamespaceCode, actualBusinessObjectDefinition.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectDefinition.getBusinessObjectDefinitionName());
        assertEquals(expectedDataProviderName, actualBusinessObjectDefinition.getDataProviderName());
        assertEquals(expectedBusinessObjectDefinitionDescription, actualBusinessObjectDefinition.getDescription());
        assertEquals(expectedAttributes, actualBusinessObjectDefinition.getAttributes());
    }

    /**
     * Validates business object format contents against specified arguments and expected (hard coded) test values.
     *
     * @param expectedBusinessObjectFormatId the expected business object format ID
     * @param expectedNamespaceCode the expected namespace code
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedIsLatestVersion the expected business object format version
     * @param expectedPartitionKey the expected business object format partition key
     * @param expectedDescription the expected business object format description
     * @param expectedAttributes the expected attributes
     * @param expectedAttributeDefinitions the list of expected attribute definitions
     * @param expectedSchema the expected business object format schema
     * @param actualBusinessObjectFormat the BusinessObjectFormat object instance to be validated
     */
    protected void validateBusinessObjectFormat(Integer expectedBusinessObjectFormatId, String expectedNamespaceCode,
        String expectedBusinessObjectDefinitionName, String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType,
        Integer expectedBusinessObjectFormatVersion, Boolean expectedIsLatestVersion, String expectedPartitionKey, String expectedDescription,
        List<Attribute> expectedAttributes, List<AttributeDefinition> expectedAttributeDefinitions, Schema expectedSchema,
        BusinessObjectFormat actualBusinessObjectFormat)
    {
        assertNotNull(actualBusinessObjectFormat);

        if (expectedBusinessObjectFormatId != null)
        {
            assertEquals(expectedBusinessObjectFormatId, Integer.valueOf(actualBusinessObjectFormat.getId()));
        }

        assertEquals(expectedNamespaceCode, actualBusinessObjectFormat.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectFormat.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectFormat.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectFormat.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, Integer.valueOf(actualBusinessObjectFormat.getBusinessObjectFormatVersion()));
        assertEquals(expectedIsLatestVersion, actualBusinessObjectFormat.isLatestVersion());
        assertEquals(expectedPartitionKey, actualBusinessObjectFormat.getPartitionKey());
        assertEqualsIgnoreNullOrEmpty("description", expectedDescription, actualBusinessObjectFormat.getDescription());

        // Ignoring the order, check if the actual list of attributes matches the expected list.
        if (!CollectionUtils.isEmpty(expectedAttributes))
        {
            assertEquals(expectedAttributes, actualBusinessObjectFormat.getAttributes());
        }
        else
        {
            assertEquals(0, actualBusinessObjectFormat.getAttributes().size());
        }

        // Ignoring the order, check if the actual list of attribute definitions matches the expected list.
        if (!CollectionUtils.isEmpty(expectedAttributeDefinitions))
        {
            assertEquals(expectedAttributeDefinitions, actualBusinessObjectFormat.getAttributeDefinitions());
        }
        else
        {
            assertEquals(0, actualBusinessObjectFormat.getAttributeDefinitions().size());
        }

        // Validate the schema.
        if (expectedSchema != null)
        {
            assertNotNull(actualBusinessObjectFormat.getSchema());
            assertEqualsIgnoreNullOrEmpty("null value", expectedSchema.getNullValue(), actualBusinessObjectFormat.getSchema().getNullValue());
            assertEqualsIgnoreNullOrEmpty("delimiter", expectedSchema.getDelimiter(), actualBusinessObjectFormat.getSchema().getDelimiter());
            assertEqualsIgnoreNullOrEmpty("escape character", expectedSchema.getEscapeCharacter(), actualBusinessObjectFormat.getSchema().getEscapeCharacter());
            assertEquals(expectedSchema.getPartitionKeyGroup(), actualBusinessObjectFormat.getSchema().getPartitionKeyGroup());
            assertEquals(expectedSchema.getColumns().size(), actualBusinessObjectFormat.getSchema().getColumns().size());

            for (int i = 0; i < expectedSchema.getColumns().size(); i++)
            {
                SchemaColumn expectedSchemaColumn = expectedSchema.getColumns().get(i);
                SchemaColumn actualSchemaColumn = actualBusinessObjectFormat.getSchema().getColumns().get(i);
                assertEquals(expectedSchemaColumn.getName(), actualSchemaColumn.getName());
                assertEquals(expectedSchemaColumn.getType(), actualSchemaColumn.getType());
                assertEquals(expectedSchemaColumn.getSize(), actualSchemaColumn.getSize());
                assertEquals(expectedSchemaColumn.isRequired(), actualSchemaColumn.isRequired());
                assertEquals(expectedSchemaColumn.getDefaultValue(), actualSchemaColumn.getDefaultValue());
                assertEquals(expectedSchemaColumn.getDescription(), actualSchemaColumn.getDescription());
            }

            if (CollectionUtils.isEmpty(expectedSchema.getPartitions()))
            {
                assertTrue(CollectionUtils.isEmpty(actualBusinessObjectFormat.getSchema().getPartitions()));
            }
            else
            {
                for (int i = 0; i < expectedSchema.getPartitions().size(); i++)
                {
                    SchemaColumn expectedPartitionColumn = expectedSchema.getPartitions().get(i);
                    SchemaColumn actualPartitionColumn = actualBusinessObjectFormat.getSchema().getPartitions().get(i);
                    assertEquals(expectedPartitionColumn.getName(), actualPartitionColumn.getName());
                    assertEquals(expectedPartitionColumn.getType(), actualPartitionColumn.getType());
                    assertEquals(expectedPartitionColumn.getSize(), actualPartitionColumn.getSize());
                    assertEquals(expectedPartitionColumn.isRequired(), actualPartitionColumn.isRequired());
                    assertEquals(expectedPartitionColumn.getDefaultValue(), actualPartitionColumn.getDefaultValue());
                    assertEquals(expectedPartitionColumn.getDescription(), actualPartitionColumn.getDescription());
                }
            }
        }
        else
        {
            assertNull(actualBusinessObjectFormat.getSchema());
        }
    }

    /**
     * Validates business object format ddl object instance against hard coded test values.
     *
     * @param expectedCustomDdlName the expected custom ddl name
     * @param expectedDdl the expected DDL
     * @param actualBusinessObjectFormatDdl the business object format ddl object instance to be validated
     */
    protected void validateBusinessObjectFormatDdl(String expectedCustomDdlName, String expectedDdl, BusinessObjectFormatDdl actualBusinessObjectFormatDdl)
    {
        validateBusinessObjectFormatDdl(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, FORMAT_VERSION,
            BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, TABLE_NAME, expectedCustomDdlName, expectedDdl, actualBusinessObjectFormatDdl);
    }

    /**
     * Validates business object format ddl object instance against specified parameters.
     *
     * @param expectedNamespaceCode the expected namespace code
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedOutputFormat the expected output format
     * @param expectedTableName the expected table name
     * @param expectedCustomDdlName the expected custom ddl name
     * @param expectedDdl the expected DDL
     * @param actualBusinessObjectFormatDdl the business object format ddl object instance to be validated
     */
    protected void validateBusinessObjectFormatDdl(String expectedNamespaceCode, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        BusinessObjectDataDdlOutputFormatEnum expectedOutputFormat, String expectedTableName, String expectedCustomDdlName, String expectedDdl,
        BusinessObjectFormatDdl actualBusinessObjectFormatDdl)
    {
        assertNotNull(actualBusinessObjectFormatDdl);
        assertEquals(expectedNamespaceCode, actualBusinessObjectFormatDdl.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectFormatDdl.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectFormatDdl.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectFormatDdl.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualBusinessObjectFormatDdl.getBusinessObjectFormatVersion());
        assertEquals(expectedOutputFormat, actualBusinessObjectFormatDdl.getOutputFormat());
        assertEquals(expectedTableName, actualBusinessObjectFormatDdl.getTableName());
        assertEquals(expectedCustomDdlName, actualBusinessObjectFormatDdl.getCustomDdlName());
        assertEquals(expectedDdl, actualBusinessObjectFormatDdl.getDdl());
    }

    /**
     * Validates custom DDL contents against specified parameters.
     *
     * @param customDdlId the expected custom DDL ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedCustomDdlName the expected custom DDL name
     * @param expectedDdl the expected DDL
     * @param actualCustomDdl the custom DDL object instance to be validated
     */
    protected void validateCustomDdl(Integer customDdlId, String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedCustomDdlName, String expectedDdl, CustomDdl actualCustomDdl)
    {
        assertNotNull(actualCustomDdl);
        if (customDdlId != null)
        {
            assertEquals(customDdlId, Integer.valueOf(actualCustomDdl.getId()));
        }
        assertEquals(
            new CustomDdlKey(expectedNamespace, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage, expectedBusinessObjectFormatFileType,
                expectedBusinessObjectFormatVersion, expectedCustomDdlName), actualCustomDdl.getCustomDdlKey());
        assertEquals(expectedDdl, actualCustomDdl.getDdl());
    }

    /**
     * Validates a download single initiation response as compared to the upload initiation response.
     *
     * @param uploadSingleInitiationResponse the upload single initiation response.
     * @param downloadSingleInitiationResponse the download single initiation response.
     */
    protected void validateDownloadSingleInitiationResponse(UploadSingleInitiationResponse uploadSingleInitiationResponse,
        DownloadSingleInitiationResponse downloadSingleInitiationResponse)
    {
        BusinessObjectData targetBusinessObjectData = uploadSingleInitiationResponse.getTargetBusinessObjectData();

        validateDownloadSingleInitiationResponse(targetBusinessObjectData.getNamespace(), targetBusinessObjectData.getBusinessObjectDefinitionName(),
            targetBusinessObjectData.getBusinessObjectFormatUsage(), targetBusinessObjectData.getBusinessObjectFormatFileType(),
            targetBusinessObjectData.getBusinessObjectFormatVersion(), targetBusinessObjectData.getAttributes(),
            targetBusinessObjectData.getStorageUnits().get(0).getStorageFiles().get(0).getFileSizeBytes(), downloadSingleInitiationResponse);
    }

    protected void validateDownloadSingleInitiationResponse(String expectedNamespaceCode, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        List<Attribute> expectedAttributes, Long expectedFileSizeBytes, DownloadSingleInitiationResponse actualDownloadSingleInitiationResponse)
    {
        assertNotNull(actualDownloadSingleInitiationResponse);

        validateBusinessObjectData(expectedNamespaceCode, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage,
            expectedBusinessObjectFormatFileType, expectedBusinessObjectFormatVersion, BusinessObjectDataStatusEntity.VALID, expectedAttributes,
            StorageEntity.MANAGED_EXTERNAL_STORAGE, FILE_NAME, expectedFileSizeBytes, actualDownloadSingleInitiationResponse.getBusinessObjectData());

        assertNotNull("aws access key", actualDownloadSingleInitiationResponse.getAwsAccessKey());
        assertNotNull("aws secret key", actualDownloadSingleInitiationResponse.getAwsSecretKey());
        assertNotNull("aws session token", actualDownloadSingleInitiationResponse.getAwsSessionToken());
        assertNotNull("pre-signed URL", actualDownloadSingleInitiationResponse.getPreSignedUrl());
    }

    /**
     * Validates expected partition value information contents against specified arguments.
     *
     * @param expectedPartitionKeyGroupName the expected partition key group name
     * @param expectedExpectedPartitionValue the expected value of the expected partition value
     * @param actualExpectedPartitionValueInformation the expected partition value information to be validated
     */
    protected void validateExpectedPartitionValueInformation(String expectedPartitionKeyGroupName, String expectedExpectedPartitionValue,
        ExpectedPartitionValueInformation actualExpectedPartitionValueInformation)
    {
        assertNotNull(actualExpectedPartitionValueInformation);
        assertEquals(expectedPartitionKeyGroupName, actualExpectedPartitionValueInformation.getExpectedPartitionValueKey().getPartitionKeyGroupName());
        assertEquals(expectedExpectedPartitionValue, actualExpectedPartitionValueInformation.getExpectedPartitionValueKey().getExpectedPartitionValue());
    }

    /**
     * Validates expected partition values information contents against specified arguments.
     *
     * @param expectedPartitionKeyGroupName the expected partition key group name
     * @param expectedExpectedPartitionValues the expected list of expected partition values
     * @param actualExpectedPartitionValuesInformation the expected partition values information to be validated
     */
    protected void validateExpectedPartitionValuesInformation(String expectedPartitionKeyGroupName, List<String> expectedExpectedPartitionValues,
        ExpectedPartitionValuesInformation actualExpectedPartitionValuesInformation)
    {
        assertNotNull(actualExpectedPartitionValuesInformation);
        assertEquals(expectedPartitionKeyGroupName, actualExpectedPartitionValuesInformation.getPartitionKeyGroupKey().getPartitionKeyGroupName());
        assertEquals(expectedExpectedPartitionValues, actualExpectedPartitionValuesInformation.getExpectedPartitionValues());
    }

    /**
     * Validates namespace contents against specified parameters.
     *
     * @param expectedNamespaceCode the expected namespace code
     * @param actualNamespace the namespace object instance to be validated
     */
    protected void validateNamespace(String expectedNamespaceCode, Namespace actualNamespace)
    {
        assertNotNull(actualNamespace);
        assertEquals(expectedNamespaceCode, actualNamespace.getNamespaceCode());
    }

    /**
     * Validates partition key group contents against specified arguments.
     *
     * @param expectedPartitionKeyGroupName the expected partition key group name
     * @param actualPartitionKeyGroup the partition key group object instance to be validated
     */
    protected void validatePartitionKeyGroup(String expectedPartitionKeyGroupName, PartitionKeyGroup actualPartitionKeyGroup)
    {
        assertNotNull(actualPartitionKeyGroup);
        assertEquals(expectedPartitionKeyGroupName, actualPartitionKeyGroup.getPartitionKeyGroupKey().getPartitionKeyGroupName());
    }

    /**
     * Validates a list of StorageFiles against the expected values.
     *
     * @param expectedStorageFiles the list of expected StorageFiles
     * @param actualStorageFiles the list of actual StorageFiles to be validated
     */
    protected void validateStorageFiles(List<StorageFile> expectedStorageFiles, List<StorageFile> actualStorageFiles)
    {
        assertEquals(expectedStorageFiles.size(), actualStorageFiles.size());
        for (int i = 0; i < expectedStorageFiles.size(); i++)
        {
            StorageFile expectedStorageFile = expectedStorageFiles.get(i);
            StorageFile actualStorageFile = actualStorageFiles.get(i);
            assertEquals(expectedStorageFile.getFilePath(), actualStorageFile.getFilePath());
            assertEquals(expectedStorageFile.getFileSizeBytes(), actualStorageFile.getFileSizeBytes());
            assertEquals(expectedStorageFile.getRowCount(), actualStorageFile.getRowCount());
        }
    }

    /**
     * Validates a storage unit key against the specified arguments.
     *
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedStorageName the expected storage name
     * @param actualStorageUnitKey the storage unit key to be validated
     */
    protected void validateStorageUnitKey(BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedStorageName,
        StorageUnitAlternateKeyDto actualStorageUnitKey)
    {
        assertNotNull(actualStorageUnitKey);
        assertEquals(expectedBusinessObjectDataKey.getNamespace(), actualStorageUnitKey.getNamespace());
        assertEquals(expectedBusinessObjectDataKey.getBusinessObjectDefinitionName(), actualStorageUnitKey.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectDataKey.getBusinessObjectFormatUsage(), actualStorageUnitKey.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectDataKey.getBusinessObjectFormatFileType(), actualStorageUnitKey.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectDataKey.getBusinessObjectFormatVersion(), actualStorageUnitKey.getBusinessObjectFormatVersion());
        assertEquals(expectedBusinessObjectDataKey.getPartitionValue(), actualStorageUnitKey.getPartitionValue());
        assertEquals(expectedBusinessObjectDataKey.getSubPartitionValues(), actualStorageUnitKey.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataKey.getBusinessObjectDataVersion(), actualStorageUnitKey.getBusinessObjectDataVersion());
        assertEquals(expectedStorageName, actualStorageUnitKey.getStorageName());
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

    /**
     * Validates upload single initiation response contents against specified parameters.
     *
     * @param expectedSourceNamespaceCode the expected source namespace code
     * @param expectedSourceBusinessObjectDefinitionName the expected source business object definition name
     * @param expectedSourceBusinessObjectFormatUsage the expected source business object format usage
     * @param expectedSourceBusinessObjectFormatFileType the expected source business object format file type
     * @param expectedSourceBusinessObjectFormatVersion the expected source business object format version
     * @param expectedTargetNamespaceCode the expected target namespace code
     * @param expectedTargetBusinessObjectDefinitionName the expected target business object definition name
     * @param expectedTargetBusinessObjectFormatUsage the expected target business object format usage
     * @param expectedTargetBusinessObjectFormatFileType the expected target business object format file type
     * @param expectedTargetBusinessObjectFormatVersion the expected target business object format version
     * @param expectedAttributes the expected business object data attributes
     * @param expectedFileName the expected file name
     * @param expectedFileSizeBytes the expected file size in bytes
     * @param expectedTargetStorageName The expected target storage name. Optional. Defaults to configured {@link
     * ConfigurationValue#S3_EXTERNAL_STORAGE_NAME_DEFAULT}
     * @param actualUploadSingleInitiationResponse the upload single initiation response to be validated
     */
    protected void validateUploadSingleInitiationResponse(String expectedSourceNamespaceCode, String expectedSourceBusinessObjectDefinitionName,
        String expectedSourceBusinessObjectFormatUsage, String expectedSourceBusinessObjectFormatFileType, Integer expectedSourceBusinessObjectFormatVersion,
        String expectedTargetNamespaceCode, String expectedTargetBusinessObjectDefinitionName, String expectedTargetBusinessObjectFormatUsage,
        String expectedTargetBusinessObjectFormatFileType, Integer expectedTargetBusinessObjectFormatVersion, List<Attribute> expectedAttributes,
        String expectedFileName, Long expectedFileSizeBytes, String expectedTargetStorageName,
        UploadSingleInitiationResponse actualUploadSingleInitiationResponse)
    {
        if (expectedTargetStorageName == null)
        {
            expectedTargetStorageName = configurationHelper.getProperty(ConfigurationValue.S3_EXTERNAL_STORAGE_NAME_DEFAULT);
        }

        assertNotNull(actualUploadSingleInitiationResponse);

        // Validate source business object data.
        validateBusinessObjectData(expectedSourceNamespaceCode, expectedSourceBusinessObjectDefinitionName, expectedSourceBusinessObjectFormatUsage,
            expectedSourceBusinessObjectFormatFileType, expectedSourceBusinessObjectFormatVersion, BusinessObjectDataStatusEntity.UPLOADING, expectedAttributes,
            StorageEntity.MANAGED_LOADING_DOCK_STORAGE, expectedFileName, expectedFileSizeBytes,
            actualUploadSingleInitiationResponse.getSourceBusinessObjectData());

        // Validate target business object data.
        validateBusinessObjectData(expectedTargetNamespaceCode, expectedTargetBusinessObjectDefinitionName, expectedTargetBusinessObjectFormatUsage,
            expectedTargetBusinessObjectFormatFileType, expectedTargetBusinessObjectFormatVersion, BusinessObjectDataStatusEntity.UPLOADING, expectedAttributes,
            expectedTargetStorageName, expectedFileName, expectedFileSizeBytes, actualUploadSingleInitiationResponse.getTargetBusinessObjectData());

        // Validate the file element.
        assertNotNull(actualUploadSingleInitiationResponse.getFile());
        assertEquals(expectedFileName, actualUploadSingleInitiationResponse.getFile().getFileName());
        assertEquals(expectedFileSizeBytes, actualUploadSingleInitiationResponse.getFile().getFileSizeBytes());

        // Validate the source uuid element.
        assertEquals(actualUploadSingleInitiationResponse.getSourceBusinessObjectData().getPartitionValue(), actualUploadSingleInitiationResponse.getUuid());

        // Validate the target uuid element.
        assertEquals(actualUploadSingleInitiationResponse.getTargetBusinessObjectData().getPartitionValue(), actualUploadSingleInitiationResponse.getUuid());

        // Validate temporary security credentials.
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY, actualUploadSingleInitiationResponse.getAwsAccessKey());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY, actualUploadSingleInitiationResponse.getAwsSecretKey());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN, actualUploadSingleInitiationResponse.getAwsSessionToken());

        assertEquals(expectedTargetStorageName, actualUploadSingleInitiationResponse.getTargetStorageName());

        // Validate KMS Key ID.
        assertEquals(storageHelper.getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID),
            storageDaoHelper.getStorageEntity(expectedTargetStorageName), true), actualUploadSingleInitiationResponse.getAwsKmsKeyId());
    }

    /**
     * Gets the {@link BusinessObjectDataKey} from the given request.
     *
     * @param request {@link BusinessObjectDataInvalidateUnregisteredRequest}
     *
     * @return {@link BusinessObjectDataKey} minus the version
     */
    private BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataInvalidateUnregisteredRequest request)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(request.getNamespace());
        businessObjectDataKey.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataKey.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataKey.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(request.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(request.getSubPartitionValues());
        return businessObjectDataKey;
    }

    /**
     * Validates that a specified XML opening and closing set of tags are not present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     */
    private void validateXmlFieldNotPresent(String message, String xmlTagName)
    {
        for (String xmlTag : Arrays.asList(String.format("<%s>", xmlTagName), String.format("</%s>", xmlTagName)))
        {
            assertTrue(String.format("%s tag not expected, but found.", xmlTag), !message.contains(xmlTag));
        }
    }

    /**
     * Validates that a specified XML opening and closing set of tags are present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     */
    private void validateXmlFieldPresent(String message, String xmlTagName)
    {
        for (String xmlTag : Arrays.asList(String.format("<%s>", xmlTagName), String.format("</%s>", xmlTagName)))
        {
            assertTrue(String.format("%s expected, but not found.", xmlTag), message.contains(xmlTag));
        }
    }

    /**
     * Validates that a specified XML tag and value are present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     * @param value the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, Object value)
    {
        assertTrue(xmlTagName + " \"" + value + "\" expected, but not found.",
            message.contains("<" + xmlTagName + ">" + (value == null ? null : value.toString()) + "</" + xmlTagName + ">"));
    }

    /**
     * Validates that the specified XML tag with the specified tag attribute and tag value is present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     * @param xmlTagAttributeName the tag attribute name.
     * @param xmlTagAttributeValue the tag attribute value.
     * @param xmlTagValue the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, String xmlTagAttributeName, String xmlTagAttributeValue, Object xmlTagValue)
    {
        assertTrue(String.format("<%s> is expected, but not found or does not match expected attribute and/or value.", xmlTagName), message.contains(String
            .format("<%s %s=\"%s\">%s</%s>", xmlTagName, xmlTagAttributeName, xmlTagAttributeValue, xmlTagValue == null ? null : xmlTagValue.toString(),
                xmlTagName)));
    }
}
