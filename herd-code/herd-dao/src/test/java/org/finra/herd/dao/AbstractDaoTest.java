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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.config.DaoTestSpringModuleConfig;
import org.finra.herd.dao.helper.HerdCollectionHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity;
import org.finra.herd.model.jpa.EmrClusterCreationLogEntity_;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.JmsMessageEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceEntity_;
import org.finra.herd.model.jpa.NotificationActionEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.OnDemandPriceEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This is an abstract base class that provides useful methods for DAO test drivers.
 */
@ContextConfiguration(classes = DaoTestSpringModuleConfig.class, inheritLocations = false)
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public abstract class AbstractDaoTest extends AbstractCoreTest
{
    public static final String ENVIRONMENT_NAME = "TEST";
    public static final String STORAGE_PLATFORM_CODE = "UT_StoragePlatform" + RANDOM_SUFFIX;

    public static final String STORAGE_NAME = "UT_Storage_1_" + RANDOM_SUFFIX;
    public static final String STORAGE_NAME_2 = "UT_Storage_2_" + RANDOM_SUFFIX;
    public static final String STORAGE_NAME_3 = "UT_Storage_3_" + RANDOM_SUFFIX;
    public static final String NO_STORAGE_NAME = null;

    public static final List<String> STORAGE_NAMES = Arrays.asList(STORAGE_NAME, STORAGE_NAME_2);
    public static final List<String> NO_STORAGE_NAMES = null;

    public static final Integer STORAGE_UNIT_ID = (int) (Math.random() * Integer.MAX_VALUE);

    public static final String NAMESPACE_CD = "UT_Namespace" + RANDOM_SUFFIX;
    public static final String NAMESPACE_CD_2 = "UT_Namespace_2" + RANDOM_SUFFIX;
    public static final String NO_NAMESPACE_CD = null;
    public static final String NOTIFICATION_NAME = "UT_Ntfcn_Name" + RANDOM_SUFFIX;
    public static final String NOTIFICATION_NAME_2 = "UT_Ntfcn_Name_2" + RANDOM_SUFFIX;
    public static final String NOTIFICATION_EVENT_TYPE = "UT_Ntfcn_Event" + RANDOM_SUFFIX;
    public static final String NOTIFICATION_EVENT_TYPE_2 = "UT_Ntfcn_Event_2" + RANDOM_SUFFIX;
    public static final String CORRELATION_DATA = "UT_Correlation_Data" + RANDOM_SUFFIX;
    public static final String CORRELATION_DATA_2 = "UT_Correlation_Data_2" + RANDOM_SUFFIX;
    public static final String CORRELATION_DATA_3 = "UT_Correlation_Data_3" + RANDOM_SUFFIX;
    public static final String STORAGE_POLICY_NAMESPACE_CD = "UT_Storage_Policy_Namespace_1_" + RANDOM_SUFFIX;
    public static final String STORAGE_POLICY_NAMESPACE_CD_2 = "UT_Storage_Policy_Namespace_2_" + RANDOM_SUFFIX;
    public static final String STORAGE_POLICY_NAME = "UT_Storage_Policy_Name_1_" + RANDOM_SUFFIX;
    public static final String STORAGE_POLICY_NAME_2 = "UT_Storage_Policy_Name_2_" + RANDOM_SUFFIX;
    public static final String STORAGE_POLICY_NAME_3 = "UT_Storage_Policy_Name_3_" + RANDOM_SUFFIX;
    public static final String STORAGE_POLICY_RULE_TYPE = "UT_Storage_Policy_Rule_Type_" + RANDOM_SUFFIX;
    public static final Integer STORAGE_POLICY_RULE_VALUE = (int) (Math.random() * Integer.MAX_VALUE);
    public static final String DESCRIPTION = "UT_Description_1_" + RANDOM_SUFFIX;
    public static final String DESCRIPTION_2 = "UT_Description_2_" + RANDOM_SUFFIX;
    public static final String REASON = "UT_Reason_1_" + RANDOM_SUFFIX;
    public static final String JOB_NAMESPACE = "UT_Job_Namespace" + RANDOM_SUFFIX;
    public static final String JOB_NAMESPACE_2 = "UT_Job_Namespace_2" + RANDOM_SUFFIX;
    public static final String JOB_NAMESPACE_3 = "UT_Job_Namespace_3" + RANDOM_SUFFIX;
    public static final String JOB_NAME = "UT_Job" + RANDOM_SUFFIX;
    public static final String JOB_NAME_2 = "UT_Job_2" + RANDOM_SUFFIX;
    public static final String JOB_NAME_3 = "UT_Job_3" + RANDOM_SUFFIX;
    public static final String JOB_DESCRIPTION = "UT_JobDescription" + RANDOM_SUFFIX;
    public static final String ACTIVITI_ID = "UT_Activiti_ID" + RANDOM_SUFFIX;
    public static final String EMR_CLUSTER_DEFINITION_NAME = "UT_EMR_CLUSTER_DFN" + RANDOM_SUFFIX;
    public static final String EMR_CLUSTER_DEFINITION_NAME_2 = "UT_EMR_CLUSTER_DFN_2" + RANDOM_SUFFIX;
    public static final String EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH = "classpath:testEmrClusterDefinition.xml";
    public static final String EMR_CLUSTER_DEFINITION_XML_FILE_MINIMAL_CLASSPATH = "classpath:testEmrClusterDefinitionMinimal.xml";
    public static final String EMR_CLUSTER_NAME = "UT_EMR_CLUSTER" + RANDOM_SUFFIX;
    public static final String EMR_STEP_NAME = "UT_EmrStepName" + RANDOM_SUFFIX;

    public static final String DATA_PROVIDER_NAME = "UT_DataProvider_1_" + RANDOM_SUFFIX;
    public static final String DATA_PROVIDER_NAME_2 = "UT_DataProvider_2_" + RANDOM_SUFFIX;
    protected final List<DataProviderKey> DATA_PROVIDER_KEYS =
        Collections.unmodifiableList(Arrays.asList(new DataProviderKey(DATA_PROVIDER_NAME), new DataProviderKey(DATA_PROVIDER_NAME_2)));

    public static final String BOD_NAMESPACE = "UT_Bodef_Namespace" + RANDOM_SUFFIX;
    public static final String BOD_NAMESPACE_2 = "UT_Bodef_Namespace_2" + RANDOM_SUFFIX;
    public static final String NO_BOD_NAMESPACE = null;
    public static final String BOD_NAME = "UT_Bodef_1_" + RANDOM_SUFFIX;
    public static final String BOD_NAME_2 = "UT_Bodef_2_" + RANDOM_SUFFIX;
    public static final String NO_BOD_NAME = null;
    public static final String BOD_DESCRIPTION = "UT_BusinessObjectDefinitionDescription" + RANDOM_SUFFIX;
    public static final String BOD_DESCRIPTION_2 = "UT_BusinessObjectDefinitionDescription" + RANDOM_SUFFIX_2;
    public static final String FORMAT_USAGE_CODE = "UT_Usage" + RANDOM_SUFFIX;
    public static final String FORMAT_USAGE_CODE_2 = "UT_Usage_2" + RANDOM_SUFFIX;
    public static final String NO_FORMAT_USAGE_CODE = null;
    public static final String FORMAT_FILE_TYPE_CODE = "UT_FileType" + RANDOM_SUFFIX;
    public static final String FORMAT_FILE_TYPE_CODE_2 = "UT_FileType_2" + RANDOM_SUFFIX;
    public static final String FORMAT_FILE_TYPE_DESCRIPTION = "UT_Description of " + FORMAT_FILE_TYPE_CODE;
    public static final String NO_FORMAT_FILE_TYPE_CODE = null;
    public static final String PARTITION_KEY = "UT_PartitionKey" + RANDOM_SUFFIX;
    public static final String FORMAT_DESCRIPTION = "UT_Format" + RANDOM_SUFFIX;
    public static final String FORMAT_DESCRIPTION_2 = "UT_Format" + RANDOM_SUFFIX_2;
    public static final String NO_FORMAT_DESCRIPTION = null;
    public static final String SCHEMA_NULL_VALUE_BACKSLASH_N = "\\N";
    public static final String SCHEMA_NULL_VALUE_NULL_WORD = "NULL";
    public static final String SCHEMA_DELIMITER_PIPE = "|";
    public static final String SCHEMA_DELIMITER_COMMA = ",";
    public static final String SCHEMA_ESCAPE_CHARACTER_BACKSLASH = "\\";
    public static final String SCHEMA_ESCAPE_CHARACTER_TILDE = "~";
    public static final String SINGLE_QUOTE = "'";
    public static final String BACKSLASH = "\\";
    public static final String PARTITION_VALUE = "UT_2014-12-31" + RANDOM_SUFFIX;
    public static final String PARTITION_VALUE_2 = "UT_2015-01-13" + RANDOM_SUFFIX;
    public static final String PARTITION_VALUE_3 = "UT_2015-08-20" + RANDOM_SUFFIX;
    public static final List<String> SUBPARTITION_VALUES =
        Arrays.asList("Aa" + RANDOM_SUFFIX, "Bb" + RANDOM_SUFFIX, "Cc" + RANDOM_SUFFIX, "Dd" + RANDOM_SUFFIX);
    public static final List<String> SUBPARTITION_VALUES_2 =
        Arrays.asList("Ee" + RANDOM_SUFFIX, "Ff" + RANDOM_SUFFIX, "Gg" + RANDOM_SUFFIX, "Hh" + RANDOM_SUFFIX);
    public static final List<String> NO_SUBPARTITION_VALUES = new ArrayList<>();
    public static final String BDATA_STATUS = "UT_Status" + RANDOM_SUFFIX;
    public static final String BDATA_STATUS_2 = "UT_Status" + RANDOM_SUFFIX_2;
    public static final String BDATA_STATUS_3 = "UT_Status_3" + RANDOM_SUFFIX;
    public static final String BDATA_STATUS_4 = "UT_Status_4" + RANDOM_SUFFIX;
    public static final Integer INITIAL_FORMAT_VERSION = 0;
    public static final Integer SECOND_FORMAT_VERSION = 1;
    public static final Integer THIRD_FORMAT_VERSION = 2;
    public static final Integer INITIAL_DATA_VERSION = 0;
    public static final Integer SECOND_DATA_VERSION = 1;
    public static final Integer THIRD_DATA_VERSION = 2;
    public static final Integer FORMAT_VERSION = (int) (Math.random() * Integer.MAX_VALUE);
    public static final Integer FORMAT_VERSION_2 = (int) (Math.random() * Integer.MAX_VALUE);
    public static final Integer NO_DATA_VERSION = null;
    public static final Integer DATA_VERSION = (int) (Math.random() * Integer.MAX_VALUE);
    public static final Integer INVALID_FORMAT_VERSION = -1 * FORMAT_VERSION;
    public static final Integer INVALID_DATA_VERSION = -1 * DATA_VERSION;

    protected static final Boolean LATEST_VERSION_FLAG_SET = true;
    protected static final Boolean NO_LATEST_VERSION_FLAG_SET = false;

    public static final String STORAGE_UNIT_STATUS = "UT_SU_Status_1_" + RANDOM_SUFFIX;
    public static final String STORAGE_UNIT_STATUS_2 = "UT_SU_Status_2_" + RANDOM_SUFFIX;

    protected static final Boolean STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET = true;
    protected static final Boolean NO_STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET = false;

    protected static final Boolean SELECT_ONLY_AVAILABLE_STORAGE_UNITS = true;
    protected static final Boolean NO_SELECT_ONLY_AVAILABLE_STORAGE_UNITS = false;

    public static final String CUSTOM_DDL_NAME = "UT_CustomDdl" + RANDOM_SUFFIX;
    public static final String CUSTOM_DDL_NAME_2 = "UT_CustomDdl_2" + RANDOM_SUFFIX;
    public static final String NO_CUSTOM_DDL_NAME = null;

    public static final String ATTRIBUTE_NAME_1_MIXED_CASE = "Attribute Name 1";
    public static final String ATTRIBUTE_VALUE_1 = "Attribute Value 1";
    public static final String ATTRIBUTE_VALUE_1_UPDATED = "Attribute Value 1 Updated";
    public static final String ATTRIBUTE_NAME_2_MIXED_CASE = "Attribute Name 2";
    public static final String ATTRIBUTE_VALUE_2 = "   Attribute Value 2  ";
    public static final String ATTRIBUTE_NAME_3_MIXED_CASE = "Attribute Name 3";
    public static final String ATTRIBUTE_VALUE_3 = "Attribute Value 3";
    public static final String ATTRIBUTE_NAME_4_MIXED_CASE = "Attribute Name 4";
    public static final String ATTRIBUTE_VALUE_4 = "Attribute Value 4";

    protected static final List<Attribute> NO_ATTRIBUTES = new ArrayList<>();
    protected static final List<AttributeDefinition> NO_ATTRIBUTE_DEFINITIONS = new ArrayList<>();
    protected static final Schema NO_SCHEMA = null;

    public static final String PARTITION_KEY_GROUP = "UT_Calendar_A" + RANDOM_SUFFIX;
    public static final String PARTITION_KEY_GROUP_2 = "UT_Calendar_B" + RANDOM_SUFFIX;
    public static final String NO_PARTITION_KEY_GROUP = null;
    public static final String TABLE_NAME = "Test_Table" + RANDOM_SUFFIX;
    public static final String JMS_QUEUE_NAME = "UT_JmsQueueName" + RANDOM_SUFFIX;
    public static final String JMS_QUEUE_NAME_2 = "UT_JmsQueueName_2" + RANDOM_SUFFIX;
    public static final String MESSAGE_TEXT = "UT_Message_Text" + RANDOM_SUFFIX;
    public static final String MESSAGE_TEXT_2 = "UT_Message_Text_2" + RANDOM_SUFFIX;
    public static final String OOZIE_WORKFLOW_LOCATION = "UT_Oozie_workflow_2" + RANDOM_SUFFIX;

    public static final String AWS_REGION = "UT_Region" + RANDOM_SUFFIX;
    public static final String EC2_INSTANCE_TYPE = "UT_Ec2InstanceType" + RANDOM_SUFFIX;
    public static final String EC2_INSTANCE_ID = "UT_Ec2InstanceId" + RANDOM_SUFFIX;
    public static final String EC2_SECURITY_GROUP_1 = "UT_Ec2SecurityGroup1" + RANDOM_SUFFIX;
    public static final String EC2_SECURITY_GROUP_2 = "UT_Ec2SecurityGroup2" + RANDOM_SUFFIX;

    public static final Integer BDATA_AGE_IN_DAYS = 1000;
    public static final Integer MAX_RESULT = 10;

    protected final String TEST_DDL = "CREATE EXTERNAL TABLE `ITEMS` (\n" +
        "    `ORGNL_TRANSFORM` INT,\n" +
        "    `DATA` DOUBLE)\n" +
        "PARTITIONED BY (`TRANSFORM` INT)\n" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\\\\' NULL DEFINED AS '\\001'\n" +
        "STORED AS TEXTFILE;";

    protected final String TEST_DDL_2 = "DROP TABLE `Test`;\n" + "CREATE EXTERNAL TABLE `TEST`;";

    protected final String S3_BUCKET_NAME = "UT_S3_Bucket_Name" + RANDOM_SUFFIX;
    protected final String S3_BUCKET_NAME_2 = "UT_S3_Bucket_Name2" + RANDOM_SUFFIX;

    protected static final String TEST_S3_KEY_PREFIX = "herd-dao-test-key-prefix" + RANDOM_SUFFIX;

    protected static final String STORAGE_DIRECTORY_PATH = "UT_Storage_Directory/Some_Path/" + RANDOM_SUFFIX;
    protected static final String NO_STORAGE_DIRECTORY_PATH = null;

    protected final String GLACIER_VAULT_NAME = "UT_Glacier_Vault_Name_1_" + RANDOM_SUFFIX;

    protected static final String LOCAL_FILE = "foo.dat";
    protected static final String TARGET_S3_KEY = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

    protected static final List<String> LOCAL_FILES =
        Arrays.asList("foo1.dat", "Foo2.dat", "FOO3.DAT", "folder/foo3.dat", "folder/foo2.dat", "folder/foo1.dat");
    protected static final List<String> S3_DIRECTORY_MARKERS = Arrays.asList("", "folder");
    protected static final List<String> SORTED_LOCAL_FILES =
        Arrays.asList("FOO3.DAT", "Foo2.dat", "folder/foo1.dat", "folder/foo2.dat", "folder/foo3.dat", "foo1.dat");
    protected static final List<String> LOCAL_FILES_SUBSET = Arrays.asList("Foo2.dat", "FOO3.DAT", "folder/foo2.dat");

    public static final String STRING_VALUE = "UT_SomeText" + RANDOM_SUFFIX;
    public static final Integer INTEGER_VALUE = (int) (Math.random() * Integer.MAX_VALUE);
    public static final Long LONG_VALUE = (long) (Math.random() * Long.MAX_VALUE);

    public static final String HTTP_PROXY_HOST = "UT_ProxyHost" + RANDOM_SUFFIX;
    public static final Integer HTTP_PROXY_PORT = (int) (Math.random() * Integer.MAX_VALUE);
    public static final String SESSION_NAME = "UT_SessionName" + RANDOM_SUFFIX;
    public static final String AWS_ROLE_ARN = "UT_AwsRoleArn" + RANDOM_SUFFIX;

    protected final List<String> STORAGE_1_AVAILABLE_PARTITION_VALUES =
        Collections.unmodifiableList(Arrays.asList("2014-04-02", "2014-04-02A", "2014-04-03", "2014-04-05", "2014-04-08"));

    protected final List<String> STORAGE_2_AVAILABLE_PARTITION_VALUES = Collections.unmodifiableList(Arrays.asList("2014-04-06", "2014-04-08"));

    protected final List<String> MULTI_STORAGE_AVAILABLE_PARTITION_VALUES_UNION =
        Collections.unmodifiableList(Arrays.asList("2014-04-02", "2014-04-02A", "2014-04-03", "2014-04-05", "2014-04-06", "2014-04-08"));

    protected final List<String> MULTI_STORAGE_AVAILABLE_PARTITION_VALUES_INTERSECTION = Collections.unmodifiableList(Arrays.asList("2014-04-08"));

    protected final List<String> STORAGE_1_NOT_AVAILABLE_PARTITION_VALUES =
        Collections.unmodifiableList(Arrays.asList("2014-04-04", "2014-04-06", "2014-04-07"));

    protected final List<String> MULTI_STORAGE_NOT_AVAILABLE_PARTITION_VALUES = Collections.unmodifiableList(Arrays.asList("2014-04-04", "2014-04-07"));

    protected static final List<String> SORTED_PARTITION_VALUES =
        Arrays.asList("2014-04-02", "2014-04-02A", "2014-04-03", "2014-04-04", "2014-04-05", "2014-04-06", "2014-04-07", "2014-04-08");

    protected static final List<String> UNSORTED_PARTITION_VALUES =
        Arrays.asList("2014-04-02", "2014-04-04", "2014-04-03", "2014-04-02A", "2014-04-08", "2014-04-07", "2014-04-05", "2014-04-06");

    protected final String STORAGE_1_GREATEST_PARTITION_VALUE = STORAGE_1_AVAILABLE_PARTITION_VALUES.get(STORAGE_1_AVAILABLE_PARTITION_VALUES.size() - 1);
    protected final String STORAGE_1_LEAST_PARTITION_VALUE = STORAGE_1_AVAILABLE_PARTITION_VALUES.get(0);

    protected static final String BLANK_TEXT = "   \n   \t\t ";

    protected static final Boolean ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA = true;
    protected static final Boolean NO_ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA = false;

    private static final String OVERRIDE_PROPERTY_SOURCE_MAP_NAME = "overrideMapPropertySource";

    // Provide easy access to the herd DAO for all test methods.
    @Autowired
    protected HerdDao herdDao;

    // Provide easy access to the S3 DAO for all test methods.
    @Autowired
    protected S3Dao s3Dao;

    // Provide easy access to the Glacier DAO for all test methods.
    @Autowired
    protected GlacierDao glacierDao;

    @PersistenceContext
    protected EntityManager entityManager;

    // A holding location for a property source.
    // When we remove the property source from the environment, we will place it here as a holding area. Then when we want to add it back into the
    // environment, we will take it from this holding area and put it back in the environment. When the property source is in the environment, we
    // set this holder to null.
    protected ReloadablePropertySource propertySourceHoldingLocation;

    @Autowired
    protected OozieDao oozieDao;

    @Autowired
    protected StsDao stsDao;

    @Autowired
    protected Ec2Dao ec2Dao;

    @Autowired
    protected EmrDao emrDao;

    @Autowired
    protected SqsDao sqsDao;

    @Autowired
    protected KmsDao kmsDao;

    @Autowired
    protected HerdCollectionHelper herdCollectionHelper;

    @Autowired
    protected S3Operations s3Operations;

    @Autowired
    protected JavaPropertiesHelper javaPropertiesHelper;

    @Autowired
    protected JdbcDao jdbcDao;

    @Autowired
    protected BusinessObjectDataNotificationRegistrationDao businessObjectDataNotificationRegistrationDao;

    @Autowired
    protected NotificationRegistrationDao notificationRegistrationDao;

    @Autowired
    protected NotificationRegistrationStatusDao notificationRegistrationStatusDao;

    /**
     * Returns an S3 file transfer request parameters DTO instance initialized using hard coded test values. This DTO is required for testing and clean up
     * activities.
     *
     * @return the newly created S3 file transfer request parameters DTO
     */
    protected S3FileTransferRequestParamsDto getTestS3FileTransferRequestParamsDto()
    {
        String s3BucketName = getS3ManagedBucketName();

        return S3FileTransferRequestParamsDto.builder().s3BucketName(s3BucketName).s3KeyPrefix(TEST_S3_KEY_PREFIX).build();
    }

    /**
     * Gets the bucket name for the S3 managed storage.
     *
     * @return the bucket name.
     */
    protected String getS3ManagedBucketName()
    {
        return getBucketNameFromStorage(StorageEntity.MANAGED_STORAGE);
    }

    /**
     * Gets the bucket name for the S3 loading dock storage.
     *
     * @return the bucket name.
     */
    protected String getS3LoadingDockBucketName()
    {
        return getBucketNameFromStorage(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);
    }

    /**
     * Gets the bucket name for the S3 external storage.
     *
     * @return the bucket name.
     */
    protected String getS3ExternalBucketName()
    {
        return getBucketNameFromStorage(StorageEntity.MANAGED_EXTERNAL_STORAGE);
    }

    /**
     * Returns the bucket name of the specified storage name.
     * <p/>
     * Gets the storage with specified name and finds and returns the value of the attribute for the bucket name.
     *
     * @param storageName the name of the storage to get the bucket name for.
     *
     * @return S3 bucket name
     * @throws IllegalStateException when either the storage or attribute is not found.
     */
    private String getBucketNameFromStorage(String storageName)
    {
        String s3BucketName = null;
        StorageEntity storageEntity = herdDao.getStorageByName(storageName);

        if (storageEntity == null)
        {
            throw new IllegalStateException("storageEntity \"" + storageName + "\" not found");
        }

        for (StorageAttributeEntity storageAttributeEntity : storageEntity.getAttributes())
        {
            if (configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME).equals(storageAttributeEntity.getName()))
            {
                s3BucketName = storageAttributeEntity.getValue();
                break;
            }
        }

        if (s3BucketName == null)
        {
            throw new IllegalStateException(
                "storageAttributeEntity with name " + configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME) +
                    " not found for storage \"" + storageName + "\".");
        }

        return s3BucketName;
    }

    /**
     * Removes the re-loadable properties source from the environment. It must not have been removed already. It can be added back using the
     * addReloadablePropertySourceToEnvironment method.
     *
     * @throws Exception if the property source couldn't be removed.
     */
    protected void removeReloadablePropertySourceFromEnvironment() throws Exception
    {
        // If the property source is in the holding location, then it has already been removed from the environment so throw an exception since it
        // shouldn't be removed again (i.e. it should be re-added first and then possibly removed again if needed).
        if (propertySourceHoldingLocation != null)
        {
            throw new Exception("Reloadable property source has already been removed.");
        }

        MutablePropertySources mutablePropertySources = getMutablePropertySources();
        propertySourceHoldingLocation = (ReloadablePropertySource) mutablePropertySources.remove(ReloadablePropertySource.class.getName());

        // Verify that the property source was removed and returned.
        if (propertySourceHoldingLocation == null)
        {
            throw new Exception("Property source with name \"" + ReloadablePropertySource.class.getName() +
                "\" is not configured and couldn't be removed from the environment.");
        }
    }

    /**
     * Modifies the re-loadable property source. Copies all the existing properties and overrides with the properties passed in the map.
     *
     * @param overrideMap a map containing the properties.
     *
     * @throws Exception if the property source couldn't be modified.
     */
    protected void modifyPropertySourceInEnvironment(Map<String, Object> overrideMap) throws Exception
    {
        removeReloadablePropertySourceFromEnvironment();

        Map<String, Object> updatedPropertiesMap = new HashMap<>();
        updatedPropertiesMap.putAll(propertySourceHoldingLocation.getSource());
        updatedPropertiesMap.putAll(overrideMap);

        // Re-add in the property source we previously removed.
        getMutablePropertySources().addLast(new MapPropertySource(OVERRIDE_PROPERTY_SOURCE_MAP_NAME, updatedPropertiesMap));
    }

    /**
     * Restores the re-loadable property source back into the environment. It must have first been removed using the modifyPropertySourceInEnvironment method.
     *
     * @throws Exception if the property source wasn't previously removed or couldn't be re-added.
     */
    protected void restorePropertySourceInEnvironment() throws Exception
    {
        // If the property source isn't in the holding area, then it hasn't yet been removed from the environment so throw an exception informing the
        // caller that it first needs to be removed before it can be added back in.
        if (propertySourceHoldingLocation == null)
        {
            throw new Exception("Reloadable property source hasn't yet been removed so it can not be re-added.");
        }

        // Remove the modified map
        MutablePropertySources mutablePropertySources = getMutablePropertySources();
        mutablePropertySources.remove(OVERRIDE_PROPERTY_SOURCE_MAP_NAME);

        // Re-add in the property source we previously removed.
        getMutablePropertySources().addLast(propertySourceHoldingLocation);

        // Remove the property source so we know it was re-added.
        propertySourceHoldingLocation = null;
    }

    /**
     * Creates and persists a new data provider entity.
     *
     * @param dataProviderName the data provider name
     *
     * @return the newly created data provider entity.
     */
    protected DataProviderEntity createDataProviderEntity(String dataProviderName)
    {
        DataProviderEntity dataProviderEntity = new DataProviderEntity();
        dataProviderEntity.setName(dataProviderName);
        return herdDao.saveAndRefresh(dataProviderEntity);
    }

    /**
     * Creates and persists a new data provider entity.
     *
     * @return the newly created data provider entity.
     */
    protected DataProviderEntity createDataProviderEntity()
    {
        return createDataProviderEntity("DataProviderTest" + getRandomSuffix());
    }

    /**
     * Creates and persists a new namespace entity.
     *
     * @param namespaceCd the namespace code
     *
     * @return the newly created namespace entity.
     */
    protected NamespaceEntity createNamespaceEntity(String namespaceCd)
    {
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespaceCd);
        return herdDao.saveAndRefresh(namespaceEntity);
    }

    /**
     * Creates and persists a new namespace entity.
     *
     * @return the newly created namespace entity.
     */
    protected NamespaceEntity createNamespaceEntity()
    {
        return createNamespaceEntity("NamespaceTest" + getRandomSuffix());
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    protected BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription)
    {
        return createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, dataProviderName, businessObjectDefinitionDescription, null);
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    protected BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, List<Attribute> attributes)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = herdDao.getNamespaceByCd(namespaceCode);
        if (namespaceEntity == null)
        {
            namespaceEntity = createNamespaceEntity(namespaceCode);
        }

        // Create a data provider entity if needed.
        DataProviderEntity dataProviderEntity = herdDao.getDataProviderByName(dataProviderName);
        if (dataProviderEntity == null)
        {
            dataProviderEntity = createDataProviderEntity(dataProviderName);
        }

        return createBusinessObjectDefinitionEntity(namespaceEntity, businessObjectDefinitionName, dataProviderEntity, businessObjectDefinitionDescription,
            attributes);
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    protected BusinessObjectDefinitionEntity createBusinessObjectDefinitionEntity(NamespaceEntity namespaceEntity, String businessObjectDefinitionName,
        DataProviderEntity dataProviderEntity, String businessObjectDefinitionDescription, List<Attribute> attributes)
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setDataProvider(dataProviderEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionName);
        businessObjectDefinitionEntity.setDescription(businessObjectDefinitionDescription);

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            List<BusinessObjectDefinitionAttributeEntity> attributeEntities = new ArrayList<>();
            businessObjectDefinitionEntity.setAttributes(attributeEntities);
            for (Attribute attribute : attributes)
            {
                BusinessObjectDefinitionAttributeEntity attributeEntity = new BusinessObjectDefinitionAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        return herdDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Creates and persists a new business object definition.
     *
     * @return the newly created business object definition.
     */
    protected BusinessObjectDefinitionEntity createBusinessObjectDefinition()
    {
        String businessObjectDefinitionName = "BusObjDefTest" + getRandomSuffix();
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(createNamespaceEntity());
        businessObjectDefinitionEntity.setDataProvider(createDataProviderEntity());
        businessObjectDefinitionEntity.setName(businessObjectDefinitionName);
        businessObjectDefinitionEntity.setDescription("test");
        return herdDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Validates a business object definition entity against specified parameters.
     *
     * @param expectedBusinessObjectDefinitionId the expected business object definition ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedDataProvider the expected data provider
     * @param expectedDescription the expected description
     * @param actualBusinessObjectDefinitionEntity the business object data availability object instance to be validated
     */
    protected void validateBusinessObjectDefinitionEntity(Integer expectedBusinessObjectDefinitionId, String expectedNamespace,
        String expectedBusinessObjectDefinitionName, String expectedDataProvider, String expectedDescription,
        BusinessObjectDefinitionEntity actualBusinessObjectDefinitionEntity)
    {
        assertNotNull(actualBusinessObjectDefinitionEntity);
        assertEquals(expectedBusinessObjectDefinitionId, actualBusinessObjectDefinitionEntity.getId());
        assertEquals(expectedNamespace, actualBusinessObjectDefinitionEntity.getNamespace().getCode());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectDefinitionEntity.getName());
        assertEquals(expectedDataProvider, actualBusinessObjectDefinitionEntity.getDataProvider().getName());
        assertEquals(expectedDescription, actualBusinessObjectDefinitionEntity.getDescription());
    }

    /**
     * Creates and persists a new file type entity.
     *
     * @param fileTypeCode the file type code value
     * @param fileTypeDescription the description of this file type
     *
     * @return the newly created file type entity.
     */
    protected FileTypeEntity createFileTypeEntity(String fileTypeCode, String fileTypeDescription)
    {
        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(fileTypeCode);
        fileTypeEntity.setDescription(fileTypeDescription);
        return herdDao.saveAndRefresh(fileTypeEntity);
    }

    /**
     * Creates and persists a new file type entity.
     *
     * @param fileTypeCode the file type code value
     *
     * @return the newly created file type entity.
     */
    protected FileTypeEntity createFileTypeEntity(String fileTypeCode)
    {
        return createFileTypeEntity(fileTypeCode, String.format("Description of \"%s\" file type.", fileTypeCode));
    }

    /**
     * Creates and persists a new file type entity.
     *
     * @return the newly created file type entity.
     */
    protected FileTypeEntity createFileTypeEntity()
    {
        String randomNumber = getRandomSuffix();
        return createFileTypeEntity("FileType" + randomNumber, "File Type " + randomNumber);
    }

    /**
     * Creates and persists a new partition key group entity.
     *
     * @param partitionKeyGroupName the name of the partition key group
     *
     * @return the newly created partition key group entity.
     */
    protected PartitionKeyGroupEntity createPartitionKeyGroupEntity(String partitionKeyGroupName)
    {
        PartitionKeyGroupEntity partitionKeyGroupEntity = new PartitionKeyGroupEntity();
        partitionKeyGroupEntity.setPartitionKeyGroupName(partitionKeyGroupName);
        return herdDao.saveAndRefresh(partitionKeyGroupEntity);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormatEntity(BusinessObjectFormatKey businessObjectFormatKey,
        String businessObjectFormatDescription, Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey)
    {
        return createBusinessObjectFormatEntity(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
            businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
            businessObjectFormatKey.getBusinessObjectFormatVersion(), businessObjectFormatDescription, businessObjectFormatLatestVersion,
            businessObjectFormatPartitionKey);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey)
    {
        return createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey, null);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey, String partitionKeyGroupName)
    {
        return createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey, partitionKeyGroupName, NO_ATTRIBUTES);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey, String partitionKeyGroupName, List<Attribute> attributes)
    {
        return createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey, partitionKeyGroupName, attributes, null, null,
            null, null, null);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey, String partitionKeyGroupName, List<Attribute> attributes,
        String schemaDelimiterCharacter, String schemaEscapeCharacter, String schemaNullValue, List<SchemaColumn> schemaColumns,
        List<SchemaColumn> partitionColumns)
    {
        // Create a business object definition entity if it does not exist.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            herdDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(namespaceCode, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, DATA_PROVIDER_NAME, null);
        }

        // Create a business object format file type entity if it does not exist.
        FileTypeEntity fileTypeEntity = herdDao.getFileTypeByCode(fileType);
        if (fileTypeEntity == null)
        {
            fileTypeEntity = createFileTypeEntity(fileType, null);
        }

        // If partition key group was specified, check if we need to create an entity for it first.
        PartitionKeyGroupEntity partitionKeyGroupEntity = null;
        if (StringUtils.isNotBlank(partitionKeyGroupName))
        {
            partitionKeyGroupEntity = herdDao.getPartitionKeyGroupByName(partitionKeyGroupName);
            if (partitionKeyGroupEntity == null)
            {
                partitionKeyGroupEntity = createPartitionKeyGroupEntity(partitionKeyGroupName);
            }
        }

        return createBusinessObjectFormatEntity(businessObjectDefinitionEntity, businessObjectFormatUsage, fileTypeEntity, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey, partitionKeyGroupEntity, attributes,
            schemaDelimiterCharacter, schemaEscapeCharacter, schemaNullValue, schemaColumns, partitionColumns);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormatEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        String businessObjectFormatUsage, FileTypeEntity fileTypeEntity, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey, PartitionKeyGroupEntity partitionKeyGroupEntity,
        List<Attribute> attributes, String schemaDelimiterCharacter, String schemaEscapeCharacter, String schemaNullValue, List<SchemaColumn> schemaColumns,
        List<SchemaColumn> partitionColumns)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setDescription(businessObjectFormatDescription);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectFormatEntity.setLatestVersion(businessObjectFormatLatestVersion);
        businessObjectFormatEntity.setUsage(businessObjectFormatUsage);
        businessObjectFormatEntity.setPartitionKey(businessObjectFormatPartitionKey);
        businessObjectFormatEntity.setPartitionKeyGroup(partitionKeyGroupEntity);

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            List<BusinessObjectFormatAttributeEntity> attributeEntities = new ArrayList<>();
            businessObjectFormatEntity.setAttributes(attributeEntities);
            for (Attribute attribute : attributes)
            {
                BusinessObjectFormatAttributeEntity attributeEntity = new BusinessObjectFormatAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        if (schemaColumns != null && !schemaColumns.isEmpty())
        {
            businessObjectFormatEntity.setDelimiter(schemaDelimiterCharacter);
            businessObjectFormatEntity.setEscapeCharacter(schemaEscapeCharacter);
            businessObjectFormatEntity.setNullValue(schemaNullValue);

            List<SchemaColumnEntity> schemaColumnEntities = new ArrayList<>();
            businessObjectFormatEntity.setSchemaColumns(schemaColumnEntities);

            int columnPosition = 1;
            for (SchemaColumn schemaColumn : schemaColumns)
            {
                SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
                schemaColumnEntities.add(schemaColumnEntity);
                schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                schemaColumnEntity.setPosition(columnPosition);
                schemaColumnEntity.setPartitionLevel(null);
                schemaColumnEntity.setName(schemaColumn.getName());
                schemaColumnEntity.setType(schemaColumn.getType());
                schemaColumnEntity.setSize(schemaColumn.getSize());
                schemaColumnEntity.setDescription(schemaColumn.getDescription());
                schemaColumnEntity.setRequired(schemaColumn.isRequired());
                schemaColumnEntity.setDefaultValue(schemaColumn.getDefaultValue());
                columnPosition++;
            }

            if (partitionColumns != null && !partitionColumns.isEmpty())
            {
                int partitionLevel = 1;
                for (SchemaColumn schemaColumn : partitionColumns)
                {
                    // Check if this partition column belongs to the list of regular schema columns.
                    int schemaColumnIndex = schemaColumns.indexOf(schemaColumn);
                    if (schemaColumnIndex >= 0)
                    {
                        // Retrieve the relative column entity and set its partition level.
                        schemaColumnEntities.get(schemaColumnIndex).setPartitionLevel(partitionLevel);
                    }
                    else
                    {
                        // Add this partition column as a new schema column entity.
                        SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
                        schemaColumnEntities.add(schemaColumnEntity);
                        schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                        schemaColumnEntity.setPosition(null);
                        schemaColumnEntity.setPartitionLevel(partitionLevel);
                        schemaColumnEntity.setName(schemaColumn.getName());
                        schemaColumnEntity.setType(schemaColumn.getType());
                        schemaColumnEntity.setSize(schemaColumn.getSize());
                        schemaColumnEntity.setDescription(schemaColumn.getDescription());
                        schemaColumnEntity.setRequired(schemaColumn.isRequired());
                        schemaColumnEntity.setDefaultValue(schemaColumn.getDefaultValue());
                    }
                    partitionLevel++;
                }
            }
        }

        return herdDao.saveAndRefresh(businessObjectFormatEntity);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    protected BusinessObjectFormatEntity createBusinessObjectFormatEntity(boolean includeAttributeDefinition)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(createBusinessObjectDefinition());
        businessObjectFormatEntity.setDescription("test");
        businessObjectFormatEntity.setFileType(createFileTypeEntity());
        businessObjectFormatEntity.setBusinessObjectFormatVersion(0);
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setUsage("PRC");
        businessObjectFormatEntity.setPartitionKey("testPartitionKey");

        if (includeAttributeDefinition)
        {
            List<BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntities = new ArrayList<>();
            businessObjectFormatEntity.setAttributeDefinitions(attributeDefinitionEntities);
            BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
            attributeDefinitionEntities.add(attributeDefinitionEntity);
            attributeDefinitionEntity.setBusinessObjectFormat(businessObjectFormatEntity);
            attributeDefinitionEntity.setName(ATTRIBUTE_NAME_1_MIXED_CASE);
        }

        return herdDao.saveAndRefresh(businessObjectFormatEntity);
    }

    /**
     * Creates and persists a new business object data attribute definition entity.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the newly created business object data attribute definition entity.
     */
    protected BusinessObjectDataAttributeDefinitionEntity createBusinessObjectDataAttributeDefinitionEntity(String namespaceCode,
        String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion,
        String businessObjectDataAttributeName)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity =
                createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                    businessObjectFormatVersion, FORMAT_DESCRIPTION, true, PARTITION_KEY);
        }

        // Create a new business object data attribute definition entity.
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataAttributeDefinitionEntity.setName(businessObjectDataAttributeName);

        // Update the parent entity.
        businessObjectFormatEntity.getAttributeDefinitions().add(businessObjectDataAttributeDefinitionEntity);
        herdDao.saveAndRefresh(businessObjectFormatEntity);

        return businessObjectDataAttributeDefinitionEntity;
    }

    /**
     * Creates and persists a new custom DDL entity.
     *
     * @return the newly created custom DDL entity
     */
    protected CustomDdlEntity createCustomDdlEntity(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String customDdlName, String ddl)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity =
                createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                    businessObjectFormatVersion, FORMAT_DESCRIPTION, true, PARTITION_KEY);
        }

        return createCustomDdlEntity(businessObjectFormatEntity, customDdlName, ddl);
    }

    /**
     * Creates and persists a new custom DDL entity.
     *
     * @return the newly created custom DDL entity
     */
    protected CustomDdlEntity createCustomDdlEntity(BusinessObjectFormatEntity businessObjectFormatEntity, String customDdlName, String ddl)
    {
        CustomDdlEntity customDdlEntity = new CustomDdlEntity();

        customDdlEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        customDdlEntity.setCustomDdlName(customDdlName);
        customDdlEntity.setDdl(ddl);

        return herdDao.saveAndRefresh(customDdlEntity);
    }

    /**
     * Creates and persists a new business object data status entity.
     *
     * @param statusCode the code of the business object data status
     *
     * @return the newly created business object data status entity
     */
    protected BusinessObjectDataStatusEntity createBusinessObjectDataStatusEntity(String statusCode)
    {
        return createBusinessObjectDataStatusEntity(statusCode, DESCRIPTION);
    }

    /**
     * Creates and persists a new business object data status entity.
     *
     * @param statusCode the code of the business object data status
     * @param description the description of the business object data status
     *
     * @return the newly created business object data status entity
     */
    protected BusinessObjectDataStatusEntity createBusinessObjectDataStatusEntity(String statusCode, String description)
    {
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(statusCode);
        businessObjectDataStatusEntity.setDescription(description);
        return herdDao.saveAndRefresh(businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode)
    {
        return createBusinessObjectDataEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
            businessObjectFormatVersion, businessObjectDataPartitionValue, NO_SUBPARTITION_VALUES, businessObjectDataVersion, businessObjectDataLatestVersion,
            businessObjectDataStatusCode);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectDataKey businessObjectDataKey, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode)
    {
        return createBusinessObjectDataEntity(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion(), businessObjectDataLatestVersion, businessObjectDataStatusCode);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity =
                createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                    businessObjectFormatVersion, FORMAT_DESCRIPTION, true, PARTITION_KEY);
        }

        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = herdDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode)
    {
        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = herdDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, NO_SUBPARTITION_VALUES, businessObjectDataVersion,
            businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion,
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, NO_SUBPARTITION_VALUES, businessObjectDataVersion,
            businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion,
        Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode)
    {
        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = herdDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion,
        Boolean businessObjectDataLatestVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        businessObjectDataEntity.setPartitionValue(businessObjectDataPartitionValue);
        if (businessObjectDataSubPartitionValues != null)
        {
            businessObjectDataEntity.setPartitionValue2(businessObjectDataSubPartitionValues.size() > 0 ? businessObjectDataSubPartitionValues.get(0) : null);
            businessObjectDataEntity.setPartitionValue3(businessObjectDataSubPartitionValues.size() > 1 ? businessObjectDataSubPartitionValues.get(1) : null);
            businessObjectDataEntity.setPartitionValue4(businessObjectDataSubPartitionValues.size() > 2 ? businessObjectDataSubPartitionValues.get(2) : null);
            businessObjectDataEntity.setPartitionValue5(businessObjectDataSubPartitionValues.size() > 3 ? businessObjectDataSubPartitionValues.get(3) : null);
        }
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setLatestVersion(businessObjectDataLatestVersion);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        return herdDao.saveAndRefresh(businessObjectDataEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    protected BusinessObjectDataEntity createBusinessObjectDataEntity()
    {
        return createBusinessObjectDataEntity(createBusinessObjectFormatEntity(false),
            new SimpleDateFormat(HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK).format(System.currentTimeMillis()), SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true,
            BusinessObjectDataStatusEntity.VALID);
    }

    /**
     * Resets business object data entity "created on" field value back by the specified number of days.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param offsetInDays the number of days to reset the business object data "created on" field value
     */
    public void ageBusinessObjectData(BusinessObjectDataEntity businessObjectDataEntity, long offsetInDays)
    {
        // Apply the offset in days to business object data "created on" value.
        businessObjectDataEntity
            .setCreatedOn(new Timestamp(businessObjectDataEntity.getCreatedOn().getTime() - offsetInDays * 86400000L));    // 24L * 60L * 60L * 1000L
        herdDao.saveAndRefresh(businessObjectDataEntity);
    }

    /**
     * Creates and persists a new business object data attribute entity.
     *
     * @return the newly created business object data attribute entity.
     */
    protected BusinessObjectDataAttributeEntity createBusinessObjectDataAttributeEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion, String businessObjectDataAttributeName,
        String businessObjectDataAttributeValue)
    {
        // Create a business object data entity if it does not exist.
        BusinessObjectDataEntity businessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues, businessObjectDataVersion));
        if (businessObjectDataEntity == null)
        {
            businessObjectDataEntity =
                createBusinessObjectDataEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                    businessObjectFormatVersion, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues, businessObjectDataVersion, true,
                    BDATA_STATUS);
        }

        // Create a new entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();

        businessObjectDataAttributeEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataAttributeEntity.setName(businessObjectDataAttributeName);
        businessObjectDataAttributeEntity.setValue(businessObjectDataAttributeValue);

        // Update the parent entity.
        businessObjectDataEntity.getAttributes().add(businessObjectDataAttributeEntity);
        herdDao.saveAndRefresh(businessObjectDataEntity);

        return businessObjectDataAttributeEntity;
    }

    /**
     * Creates and persists a new storage platform entity.
     *
     * @param storagePlatformCode the storage platform code
     *
     * @return the newly created storage platform entity
     */
    protected StoragePlatformEntity createStoragePlatformEntity(String storagePlatformCode)
    {
        StoragePlatformEntity storagePlatformEntity = new StoragePlatformEntity();
        storagePlatformEntity.setName(storagePlatformCode);
        return herdDao.saveAndRefresh(storagePlatformEntity);
    }

    /**
     * Creates and persists a new storage entity with a random name and no attributes.
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity()
    {
        return createStorageEntity("StorageTest" + getRandomSuffix());
    }

    /**
     * Creates and persists a new storage entity with a random name with the specified attributes.
     *
     * @param attributeName the attribute name.
     * @param attributeValue the attribute value.
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntityWithAttributes(String attributeName, String attributeValue)
    {
        return createStorageEntity("StorageTest" + getRandomSuffix(), StoragePlatformEntity.S3, attributeName, attributeValue);
    }

    /**
     * Creates and persists a new storage entity of S3 storage platform with no attributes.
     *
     * @param storageName the storage name
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity(String storageName)
    {
        return createStorageEntity(storageName, StoragePlatformEntity.S3);
    }

    /**
     * Creates and persists a new storage entity of S3 storage platform with the specified attributes.
     *
     * @param storageName the storage name
     * @param attributes the storage attributes.
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity(String storageName, List<Attribute> attributes)
    {
        return createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);
    }

    /**
     * Creates and persists a new storage entity with no attributes.
     *
     * @param storageName the storage name
     * @param storagePlatformCode the storage platform code
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity(String storageName, String storagePlatformCode)
    {
        return createStorageEntity(storageName, storagePlatformCode, null);
    }

    /**
     * Creates and persists a new storage entity with an attribute.
     *
     * @param storageName the storage name
     * @param storagePlatformCode the storage platform code
     * @param attributeName the attribute name
     * @param attributeValue the attribute value
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity(String storageName, String storagePlatformCode, String attributeName, String attributeValue)
    {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(attributeName, attributeValue));
        return createStorageEntity(storageName, storagePlatformCode, attributes);
    }

    /**
     * Creates and persists a new storage entity with the specified attributes.
     *
     * @param storageName the storage name
     * @param storagePlatformCode the storage platform code
     * @param attributes the attributes.
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity(String storageName, String storagePlatformCode, List<Attribute> attributes)
    {
        // Create storage platform entity if it does not exist.
        StoragePlatformEntity storagePlatformEntity = herdDao.getStoragePlatformByName(storagePlatformCode);
        if (storagePlatformEntity == null)
        {
            storagePlatformEntity = createStoragePlatformEntity(storagePlatformCode);
        }
        return createStorageEntity(storageName, storagePlatformEntity, attributes);
    }

    /**
     * Creates and persists a new storage entity based on the specified storage name and storage platform entity and no attributes.
     *
     * @param storageName the storage name
     * @param storagePlatformEntity the storage platform entity
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity(String storageName, StoragePlatformEntity storagePlatformEntity)
    {
        return createStorageEntity(storageName, storagePlatformEntity, null);
    }

    /**
     * Creates and persists a new storage entity.
     *
     * @param storageName the storage name
     * @param storagePlatformEntity the storage platform entity
     *
     * @return the newly created storage entity.
     */
    protected StorageEntity createStorageEntity(String storageName, StoragePlatformEntity storagePlatformEntity, List<Attribute> attributes)
    {
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(storageName);
        storageEntity.setStoragePlatform(storagePlatformEntity);

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            List<StorageAttributeEntity> attributeEntities = new ArrayList<>();
            storageEntity.setAttributes(attributeEntities);
            for (Attribute attribute : attributes)
            {
                StorageAttributeEntity attributeEntity = new StorageAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setStorage(storageEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        return herdDao.saveAndRefresh(storageEntity);
    }

    /**
     * Creates and persists a new storage attribute entity.
     *
     * @param storageEntity the storage entity to add the attribute to
     * @param attributeName the attribute name
     * @param attributeValue the attribute value
     *
     * @return the newly created storage attribute entity.
     */
    protected StorageAttributeEntity createStorageAttributeEntity(StorageEntity storageEntity, String attributeName, String attributeValue)
    {
        StorageAttributeEntity storageAttributeEntity = new StorageAttributeEntity();
        storageAttributeEntity.setStorage(storageEntity);
        storageAttributeEntity.setName(attributeName);
        storageAttributeEntity.setValue(attributeValue);
        return herdDao.saveAndRefresh(storageAttributeEntity);
    }

    /**
     * Creates and persists a new storage unit status entity.
     *
     * @param statusCode the code of the storage unit status
     *
     * @return the newly created storage unit status entity
     */
    protected StorageUnitStatusEntity createStorageUnitStatusEntity(String statusCode)
    {
        return createStorageUnitStatusEntity(statusCode, DESCRIPTION, STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);
    }

    /**
     * Creates and persists a new storage unit status entity.
     *
     * @param statusCode the code of the storage unit status
     * @param description the description of the status code
     * @param available specifies if the business object data stored in the relative storage unit is available or not for consumption
     *
     * @return the newly created storage unit status entity
     */
    protected StorageUnitStatusEntity createStorageUnitStatusEntity(String statusCode, String description, Boolean available)
    {
        StorageUnitStatusEntity storageUnitStatusEntity = new StorageUnitStatusEntity();
        storageUnitStatusEntity.setCode(statusCode);
        storageUnitStatusEntity.setDescription(description);
        storageUnitStatusEntity.setAvailable(available);
        return herdDao.saveAndRefresh(storageUnitStatusEntity);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageName the storage name
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataLatestVersion specifies if the business object data is flagged as latest version or not
     * @param businessObjectDataStatusCode the business object data status
     * @param storageUnitStatus the storage unit status
     * @param storageDirectoryPath the storage directory path
     *
     * @return the newly created storage unit entity
     */
    protected StorageUnitEntity createStorageUnitEntity(String storageName, String namespace, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue,
        List<String> subPartitionValues, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode,
        String storageUnitStatus, String storageDirectoryPath)
    {
        return createStorageUnitEntity(storageName,
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, subPartitionValues, businessObjectDataVersion), businessObjectDataLatestVersion,
            businessObjectDataStatusCode, storageUnitStatus, storageDirectoryPath);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataLatestVersion specifies if the business object data is flagged as latest version or not
     * @param businessObjectDataStatusCode the business object data status
     * @param storageUnitStatus the storage unit status
     * @param storageDirectoryPath the storage directory path
     *
     * @return the newly created storage unit entity
     */
    protected StorageUnitEntity createStorageUnitEntity(String storageName, BusinessObjectDataKey businessObjectDataKey,
        Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode, String storageUnitStatus, String storageDirectoryPath)
    {
        // Create a storage entity, if not exists.
        StorageEntity storageEntity = herdDao.getStorageByName(storageName);
        if (storageEntity == null)
        {
            storageEntity = createStorageEntity(storageName);
        }

        // Create a business object data entity, if not exists.
        BusinessObjectDataEntity businessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        if (businessObjectDataEntity == null)
        {
            businessObjectDataEntity = createBusinessObjectDataEntity(businessObjectDataKey, businessObjectDataLatestVersion, businessObjectDataStatusCode);
        }

        // Create and return a storage unit entity.
        return createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatus, storageDirectoryPath);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntity the business object data entity
     * @param storageUnitStatus the storage unit status
     * @param directoryPath the storage directory path
     *
     * @return the newly created storage unit entity.
     */
    protected StorageUnitEntity createStorageUnitEntity(StorageEntity storageEntity, BusinessObjectDataEntity businessObjectDataEntity,
        String storageUnitStatus, String directoryPath)
    {
        // Create a storage entity, if not exists.
        StorageUnitStatusEntity storageUnitStatusEntity = herdDao.getStorageUnitStatusByCode(storageUnitStatus);
        if (storageUnitStatusEntity == null)
        {
            storageUnitStatusEntity = createStorageUnitStatusEntity(storageUnitStatus);
        }

        return createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, directoryPath);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntity the business object data entity
     * @param storageUnitStatusEntity the storage unit status entity
     * @param directoryPath the storage directory path
     *
     * @return the newly created storage unit entity.
     */
    protected StorageUnitEntity createStorageUnitEntity(StorageEntity storageEntity, BusinessObjectDataEntity businessObjectDataEntity,
        StorageUnitStatusEntity storageUnitStatusEntity, String directoryPath)
    {
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setDirectoryPath(directoryPath);
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        return herdDao.saveAndRefresh(storageUnitEntity);
    }

    /**
     * Creates and persists a new storage file entity.
     *
     * @return the newly created storage file entity.
     */
    protected StorageFileEntity createStorageFileEntity(StorageUnitEntity storageUnitEntity, String filePath, Long fileSizeInBytes, Long rowCount)
    {
        StorageFileEntity storageFileEntity = new StorageFileEntity();
        storageFileEntity.setStorageUnit(storageUnitEntity);
        storageFileEntity.setPath(filePath);
        storageFileEntity.setFileSizeBytes(fileSizeInBytes);
        storageFileEntity.setRowCount(rowCount);
        return herdDao.saveAndRefresh(storageFileEntity);
    }

    /**
     * Creates and persists a new notification event type entity.
     *
     * @param code the notification event type code
     *
     * @return the newly created notification event type entity
     */
    protected NotificationEventTypeEntity createNotificationEventTypeEntity(String code)
    {
        NotificationEventTypeEntity notificationEventTypeEntity = new NotificationEventTypeEntity();
        notificationEventTypeEntity.setCode(code);
        notificationEventTypeEntity.setDescription(String.format("Description of \"%s\".", code));
        return herdDao.saveAndRefresh(notificationEventTypeEntity);
    }

    /**
     * Returns a list of test job actions.
     *
     * @return the list of test job actions
     */
    protected List<JobAction> getTestJobActions()
    {
        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(JOB_NAMESPACE, JOB_NAME, CORRELATION_DATA));
        jobActions.add(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, CORRELATION_DATA_2));
        return jobActions;
    }

    /**
     * Returns a list of test job actions.
     *
     * @return the list of test job actions
     */
    protected List<JobAction> getTestJobActions2()
    {
        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(JOB_NAMESPACE_3, JOB_NAME_3, CORRELATION_DATA_3));
        return jobActions;
    }

    /**
     * Creates and persists a business object data notification registration entity. Defaults the status to ENABLED.
     *
     * @param businessObjectDataNotificationRegistrationKey the business object data notification registration key
     * @param notificationEventTypeCode the notification event type
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param storageName the storage name
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     * @param jobActions the list of job actions
     *
     * @return the newly created business object data notification registration entity
     */
    protected BusinessObjectDataNotificationRegistrationEntity createBusinessObjectDataNotificationRegistrationEntity(
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey, String notificationEventTypeCode, String businessObjectDefinitionNamespace,
        String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion,
        String storageName, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus, List<JobAction> jobActions)
    {
        return createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, notificationEventTypeCode,
            businessObjectDefinitionNamespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
            businessObjectFormatVersion, storageName, newBusinessObjectDataStatus, oldBusinessObjectDataStatus, jobActions, "ENABLED");
    }

    /**
     * Creates and persists a business object data notification registration entity.
     *
     * @param businessObjectDataNotificationRegistrationKey the business object data notification registration key
     * @param notificationEventTypeCode the notification event type
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param storageName the storage name
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     * @param jobActions the list of job actions
     * @param notificationRegistrationStatus The notification registration status
     *
     * @return the newly created business object data notification registration entity
     */
    protected BusinessObjectDataNotificationRegistrationEntity createBusinessObjectDataNotificationRegistrationEntity(
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey, String notificationEventTypeCode, String businessObjectDefinitionNamespace,
        String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion,
        String storageName, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus, List<JobAction> jobActions,
        String notificationRegistrationStatus)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = herdDao.getNamespaceByCd(businessObjectDataNotificationRegistrationKey.getNamespace());
        if (namespaceEntity == null)
        {
            namespaceEntity = createNamespaceEntity(businessObjectDataNotificationRegistrationKey.getNamespace());
        }

        // Create a notification event type entity if needed.
        NotificationEventTypeEntity notificationEventTypeEntity = herdDao.getNotificationEventTypeByCode(notificationEventTypeCode);
        if (notificationEventTypeEntity == null)
        {
            notificationEventTypeEntity = createNotificationEventTypeEntity(notificationEventTypeCode);
        }

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            herdDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity =
                createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, DATA_PROVIDER_NAME, BOD_DESCRIPTION);
        }

        // Create a business object format file type entity if needed.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectFormatFileType))
        {
            fileTypeEntity = herdDao.getFileTypeByCode(businessObjectFormatFileType);
            if (fileTypeEntity == null)
            {
                fileTypeEntity = createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create a storage entity if needed.
        StorageEntity storageEntity = null;
        if (StringUtils.isNotBlank(storageName))
        {
            storageEntity = herdDao.getStorageByName(storageName);
            if (storageEntity == null)
            {
                storageEntity = createStorageEntity(storageName, StoragePlatformEntity.S3);
            }
        }

        // Create a business object status entity for new status if needed.
        BusinessObjectDataStatusEntity newBusinessObjectDataStatusEntity = null;
        if (StringUtils.isNotBlank(newBusinessObjectDataStatus))
        {
            newBusinessObjectDataStatusEntity = herdDao.getBusinessObjectDataStatusByCode(newBusinessObjectDataStatus);
            if (newBusinessObjectDataStatusEntity == null)
            {
                newBusinessObjectDataStatusEntity = createBusinessObjectDataStatusEntity(newBusinessObjectDataStatus);
            }
        }

        // Create a business object status entity for new status if needed.
        BusinessObjectDataStatusEntity oldBusinessObjectDataStatusEntity = null;
        if (StringUtils.isNotBlank(oldBusinessObjectDataStatus))
        {
            oldBusinessObjectDataStatusEntity = herdDao.getBusinessObjectDataStatusByCode(oldBusinessObjectDataStatus);
            if (oldBusinessObjectDataStatusEntity == null)
            {
                oldBusinessObjectDataStatusEntity = createBusinessObjectDataStatusEntity(oldBusinessObjectDataStatus);
            }
        }

        // Create a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            new BusinessObjectDataNotificationRegistrationEntity();

        businessObjectDataNotificationRegistrationEntity.setNamespace(namespaceEntity);
        businessObjectDataNotificationRegistrationEntity.setName(businessObjectDataNotificationRegistrationKey.getNotificationName());
        businessObjectDataNotificationRegistrationEntity.setNotificationEventType(notificationEventTypeEntity);
        businessObjectDataNotificationRegistrationEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDataNotificationRegistrationEntity.setUsage(businessObjectFormatUsage);
        businessObjectDataNotificationRegistrationEntity.setFileType(fileTypeEntity);
        businessObjectDataNotificationRegistrationEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataNotificationRegistrationEntity.setStorage(storageEntity);
        businessObjectDataNotificationRegistrationEntity.setNewBusinessObjectDataStatus(newBusinessObjectDataStatusEntity);
        businessObjectDataNotificationRegistrationEntity.setOldBusinessObjectDataStatus(oldBusinessObjectDataStatusEntity);
        businessObjectDataNotificationRegistrationEntity
            .setNotificationRegistrationStatus(notificationRegistrationStatusDao.getNotificationRegistrationStatus(notificationRegistrationStatus));

        if (!CollectionUtils.isEmpty(jobActions))
        {
            List<NotificationActionEntity> notificationActionEntities = new ArrayList<>();
            businessObjectDataNotificationRegistrationEntity.setNotificationActions(notificationActionEntities);

            for (JobAction jobAction : jobActions)
            {
                // Create a job definition entity if needed.
                JobDefinitionEntity jobDefinitionEntity = herdDao.getJobDefinitionByAltKey(jobAction.getNamespace(), jobAction.getJobName());
                if (jobDefinitionEntity == null)
                {
                    jobDefinitionEntity = createJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName(),
                        String.format("Description of \"%s.%s\" job definition.", jobAction.getNamespace(), jobAction.getJobName()),
                        String.format("%s.%s.%s", jobAction.getNamespace(), jobAction.getJobName(), ACTIVITI_ID));
                }

                NotificationJobActionEntity notificationJobActionEntity = new NotificationJobActionEntity();
                notificationActionEntities.add(notificationJobActionEntity);
                notificationJobActionEntity.setNotificationRegistration(businessObjectDataNotificationRegistrationEntity);
                notificationJobActionEntity.setJobDefinition(jobDefinitionEntity);
                notificationJobActionEntity.setCorrelationData(jobAction.getCorrelationData());
            }
        }

        return herdDao.saveAndRefresh(businessObjectDataNotificationRegistrationEntity);
    }

    /**
     * Creates and persists a new job definition entity.
     *
     * @param namespaceCode the namespace code
     * @param jobName the job name
     * @param description the job definition description
     * @param activitiId the job definition Activiti ID
     *
     * @return the newly created job definition entity
     */
    protected JobDefinitionEntity createJobDefinitionEntity(String namespaceCode, String jobName, String description, String activitiId)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = herdDao.getNamespaceByCd(namespaceCode);
        if (namespaceEntity == null)
        {
            namespaceEntity = createNamespaceEntity(namespaceCode);
        }

        return createJobDefinitionEntity(namespaceEntity, jobName, description, activitiId);
    }

    /**
     * Creates and persists a new job definition entity.
     *
     * @param namespaceEntity the namespace entity
     * @param jobName the job name
     * @param description the job definition description
     * @param activitiId the job definition Activiti ID
     *
     * @return the newly created job definition entity
     */
    protected JobDefinitionEntity createJobDefinitionEntity(NamespaceEntity namespaceEntity, String jobName, String description, String activitiId)
    {
        JobDefinitionEntity jobDefinitionEntity = new JobDefinitionEntity();
        jobDefinitionEntity.setNamespace(namespaceEntity);
        jobDefinitionEntity.setName(jobName);
        jobDefinitionEntity.setDescription(description);
        jobDefinitionEntity.setActivitiId(activitiId);
        return herdDao.saveAndRefresh(jobDefinitionEntity);
    }

    /**
     * Creates and persists a new EMR cluster definition entity.
     *
     * @param namespaceEntity the namespace entity
     * @param definitionName the cluster definition name
     * @param configurationXml the cluster configuration XML
     *
     * @return the newly created job definition entity
     */
    protected EmrClusterDefinitionEntity createEmrClusterDefinitionEntity(NamespaceEntity namespaceEntity, String definitionName, String configurationXml)
    {
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = new EmrClusterDefinitionEntity();
        emrClusterDefinitionEntity.setNamespace(namespaceEntity);
        emrClusterDefinitionEntity.setName(definitionName);
        emrClusterDefinitionEntity.setConfiguration(configurationXml);
        return herdDao.saveAndRefresh(emrClusterDefinitionEntity);
    }

    /**
     * Creates a list of expected partition value process dates for the month of April, 2014, excluding weekends.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the list of expected partition value process dates
     */
    protected List<ExpectedPartitionValueEntity> createExpectedPartitionValueProcessDatesForApril2014(String partitionKeyGroupName)
    {
        return createExpectedPartitionValueProcessDates(partitionKeyGroupName, new GregorianCalendar(2014, 3, 1), new GregorianCalendar(2014, 3, 30));
    }

    /**
     * Creates a list of expected partition value process dates for a specified range. Weekends are excluded.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param startDate the start date of the range
     * @param endDate the end date of the range
     *
     * @return the list of expected partition value process dates
     */
    protected List<ExpectedPartitionValueEntity> createExpectedPartitionValueProcessDates(String partitionKeyGroupName, Calendar startDate, Calendar endDate)
    {
        // Initialize the list of expected partition values.
        List<String> expectedPartitionValues = new ArrayList<>();

        // Keep incrementing the start date until it is greater than the end date, or until we have 1000 dates to protect against having too many dates or an
        // infinite loop in case the end date is before the start date.
        for (int i = 0; i < 1000 && startDate.compareTo(endDate) <= 0; i++)
        {
            // Create and persist a new entity for the date if it does not fall on the weekend.
            if ((startDate.get(Calendar.DAY_OF_WEEK) != Calendar.SATURDAY) && (startDate.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY))
            {
                expectedPartitionValues.add(new SimpleDateFormat(HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK).format(startDate.getTime()));
            }

            // Add one day to the calendar.
            startDate.add(Calendar.DAY_OF_MONTH, 1);
        }

        // Return the list of entities.
        return createExpectedPartitionValueEntities(partitionKeyGroupName, expectedPartitionValues);
    }

    /**
     * Creates and persists specified partition value entities.  This method also creates and persists a partition key group entity, if it does not exist.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param expectedPartitionValues the list of expected partition values
     *
     * @return the list of expected partition value entities
     */
    protected List<ExpectedPartitionValueEntity> createExpectedPartitionValueEntities(String partitionKeyGroupName, List<String> expectedPartitionValues)
    {
        // Create partition key group if it does not exist.
        PartitionKeyGroupEntity partitionKeyGroupEntity = herdDao.getPartitionKeyGroupByName(partitionKeyGroupName);
        if (partitionKeyGroupEntity == null)
        {
            partitionKeyGroupEntity = createPartitionKeyGroupEntity(partitionKeyGroupName);
        }

        // Initialize the return list.
        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = new ArrayList<>();

        // Keep incrementing the start date until it is greater than the end date, or until we have 1000 dates to protect against having too many dates or an
        // infinite loop in case the end date is before the start date.
        for (String expectedPartitionValue : expectedPartitionValues)
        {
            ExpectedPartitionValueEntity expectedPartitionValueEntity = new ExpectedPartitionValueEntity();
            expectedPartitionValueEntity.setPartitionKeyGroup(partitionKeyGroupEntity);
            expectedPartitionValueEntity.setPartitionValue(expectedPartitionValue);
            expectedPartitionValueEntities.add(herdDao.saveAndRefresh(expectedPartitionValueEntity));
        }

        // Return the list of entities.
        return expectedPartitionValueEntities;
    }

    /**
     * Creates test files of the specified size relative to the base directory.
     *
     * @param baseDirectory the local parent directory path, relative to which we want our file to be created
     * @param size the file size in bytes
     */
    protected void createLocalFiles(String baseDirectory, long size) throws IOException
    {
        // Create local test files.
        for (String file : LOCAL_FILES)
        {
            createLocalFile(baseDirectory, file, size);
        }
    }

    /**
     * Returns a list of {@link EmrClusterCreationLogEntity} objects for the given cluster namespace, cluster definition name, and EMR cluster name. All the
     * given parameters are case insensitive. The returned list's order is not guaranteed.
     *
     * @param namespace - EMR cluster namespace
     * @param definitionName - EMR cluster definition name
     * @param clusterName - EMR cluster name
     *
     * @return list of EMR cluster creation logs
     */
    public List<EmrClusterCreationLogEntity> getEmrClusterCreationLogEntities(String namespace, String definitionName, String clusterName)
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<EmrClusterCreationLogEntity> query = builder.createQuery(EmrClusterCreationLogEntity.class);
        Root<EmrClusterCreationLogEntity> emrClusterCreationLogEntity = query.from(EmrClusterCreationLogEntity.class);
        Join<?, NamespaceEntity> namespaceEntity = emrClusterCreationLogEntity.join(EmrClusterCreationLogEntity_.namespace);
        Predicate namespacePredicate = builder.equal(builder.upper(namespaceEntity.get(NamespaceEntity_.code)), namespace.toUpperCase());
        Predicate definitionNamePredicate =
            builder.equal(builder.upper(emrClusterCreationLogEntity.get(EmrClusterCreationLogEntity_.emrClusterDefinitionName)), definitionName.toUpperCase());
        Predicate clusterNamePredicate =
            builder.equal(builder.upper(emrClusterCreationLogEntity.get(EmrClusterCreationLogEntity_.emrClusterName)), clusterName.toUpperCase());
        query.select(emrClusterCreationLogEntity).where(builder.and(namespacePredicate, definitionNamePredicate, clusterNamePredicate));
        return entityManager.createQuery(query).getResultList();
    }

    /**
     * Creates and persists a new JMS message entity.
     *
     * @param jmsQueueName the JMS queue name
     * @param messageText the message text
     *
     * @return the newly created JMS message entity
     */
    protected JmsMessageEntity createJmsMessageEntity(String jmsQueueName, String messageText)
    {
        JmsMessageEntity jmsMessageEntity = new JmsMessageEntity();
        jmsMessageEntity.setJmsQueueName(jmsQueueName);
        jmsMessageEntity.setMessageText(messageText);
        return herdDao.saveAndRefresh(jmsMessageEntity);
    }

    /**
     * Returns a list of test storage keys.
     *
     * @return the list of test storage keys
     */
    protected List<StorageKey> getTestStorageKeys()
    {
        // Get a list of test storage keys.
        return Arrays.asList(new StorageKey(STORAGE_NAME), new StorageKey(STORAGE_NAME_2));
    }

    /**
     * Returns a list of test namespace keys.
     *
     * @return the list of test namespace keys
     */
    protected List<NamespaceKey> getTestNamespaceKeys()
    {
        return Arrays.asList(new NamespaceKey(NAMESPACE_CD), new NamespaceKey(NAMESPACE_CD_2));
    }

    /**
     * Returns a list of test business object definition keys.
     *
     * @return the list of test business object definition keys
     */
    protected List<BusinessObjectDefinitionKey> getTestBusinessObjectDefinitionKeys()
    {
        List<BusinessObjectDefinitionKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME_2));
        keys.add(new BusinessObjectDefinitionKey(NAMESPACE_CD_2, BOD_NAME_2));
        keys.add(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME));
        keys.add(new BusinessObjectDefinitionKey(NAMESPACE_CD_2, BOD_NAME));

        return keys;
    }

    /**
     * Returns a list of test business object definition keys expected to be returned by getBusinessObjectDefinitions() method.
     *
     * @return the list of expected business object definition keys
     */
    protected List<BusinessObjectDefinitionKey> getExpectedBusinessObjectDefinitionKeys()
    {
        List<BusinessObjectDefinitionKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME));
        keys.add(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME_2));

        return keys;
    }

    /**
     * Returns a list of test file type keys.
     *
     * @return the list of test file type keys
     */
    protected List<FileTypeKey> getTestFileTypeKeys()
    {
        // Get a list of test file type keys.
        return Arrays.asList(new FileTypeKey(FORMAT_FILE_TYPE_CODE), new FileTypeKey(FORMAT_FILE_TYPE_CODE_2));
    }

    /**
     * Returns a list of test partition key group keys.
     *
     * @return the list of test partition key group keys
     */
    protected List<PartitionKeyGroupKey> getTestPartitionKeyGroupKeys()
    {
        // Get a list of test file type keys.
        return Arrays.asList(new PartitionKeyGroupKey(PARTITION_KEY_GROUP), new PartitionKeyGroupKey(PARTITION_KEY_GROUP_2));
    }

    /**
     * Returns a list of test business object format keys.
     *
     * @return the list of test business object format keys
     */
    protected List<BusinessObjectFormatKey> getTestBusinessObjectFormatKeys()
    {
        List<BusinessObjectFormatKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME_2, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD_2, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD_2, BOD_NAME_2, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        return keys;
    }

    /**
     * Returns a list of test business object format keys expected to be returned by getBusinessObjectDefinitions() method.
     *
     * @return the list of expected business object format keys
     */
    protected List<BusinessObjectFormatKey> getExpectedBusinessObjectFormatKeys()
    {
        List<BusinessObjectFormatKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));

        return keys;
    }

    /**
     * Returns a list of test business object format keys expected to be returned by getBusinessObjectDefinitions() method with the
     * latestBusinessObjectFormatVersion flag set to "true".
     *
     * @return the list of expected business object format keys
     */
    protected List<BusinessObjectFormatKey> getExpectedBusinessObjectFormatLatestVersionKeys()
    {
        List<BusinessObjectFormatKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION));

        return keys;
    }

    /**
     * Validates custom DDL key against specified parameters.
     *
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedCustomDdlName the expected custom DDL name
     * @param actualCustomDdlKey the custom DDL key object instance to be validated
     */
    protected void validateCustomDdlKey(String expectedNamespace, String expectedBusinessObjectDefinitionName, String expectedBusinessObjectFormatUsage,
        String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion, String expectedCustomDdlName, CustomDdlKey actualCustomDdlKey)
    {
        assertNotNull(actualCustomDdlKey);
        assertEquals(expectedNamespace, actualCustomDdlKey.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualCustomDdlKey.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualCustomDdlKey.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualCustomDdlKey.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualCustomDdlKey.getBusinessObjectFormatVersion());
        assertEquals(expectedCustomDdlName, actualCustomDdlKey.getCustomDdlName());
    }

    /**
     * Returns a list of test business object data notification registration keys.
     *
     * @return the list of test business object data notification registration keys
     */
    protected List<NotificationRegistrationKey> getTestBusinessObjectDataNotificationRegistrationKeys()
    {
        List<NotificationRegistrationKey> keys = new ArrayList<>();

        keys.add(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME));
        keys.add(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME_2));
        keys.add(new NotificationRegistrationKey(NAMESPACE_CD_2, NOTIFICATION_NAME));
        keys.add(new NotificationRegistrationKey(NAMESPACE_CD_2, NOTIFICATION_NAME_2));

        return keys;
    }

    /**
     * Returns a list of test business object data notification registration keys expected to be returned by getBusinessObjectDataNotificationRegistrationKeys()
     * method.
     *
     * @return the list of expected business object data notification registration keys
     */
    protected List<NotificationRegistrationKey> getExpectedBusinessObjectDataNotificationRegistrationKeys()
    {
        List<NotificationRegistrationKey> keys = new ArrayList<>();

        keys.add(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME));
        keys.add(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME_2));

        return keys;
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param region the AWS region
     * @param instanceType the EC2 instance type
     *
     * @return the newly created storage unit entity.
     */
    protected OnDemandPriceEntity createOnDemandPriceEntity(String region, String instanceType)
    {
        OnDemandPriceEntity onDemandPriceEntity = new OnDemandPriceEntity();
        onDemandPriceEntity.setOnDemandPriceId(LONG_VALUE);
        onDemandPriceEntity.setRegion(region);
        onDemandPriceEntity.setInstanceType(instanceType);
        onDemandPriceEntity.setValue(new BigDecimal(INTEGER_VALUE));
        return herdDao.saveAndRefresh(onDemandPriceEntity);
    }

    /**
     * Returns an unsorted list of test expected partition values.
     *
     * @return the unsorted list of expected partition values
     */
    protected List<String> getTestUnsortedExpectedPartitionValues()
    {
        return Arrays.asList("2014-04-02", "2014-04-04", "2014-04-03", "2014-04-08", "2014-04-07", "2014-04-05", "2014-04-06");
    }

    /**
     * Returns a sorted list of test expected partition values.
     *
     * @return the list of expected partition values in ascending order
     */
    protected List<String> getTestSortedExpectedPartitionValues()
    {
        List<String> expectedPartitionValues = getTestUnsortedExpectedPartitionValues();
        Collections.sort(expectedPartitionValues);
        return expectedPartitionValues;
    }

    /**
     * Creates and persists expected partition value entities.
     *
     * @param partitionKeyGroupEntity the partition key group entity
     * @param expectedPartitionValues the list of expected partition value entities
     */
    protected void createExpectedPartitionValueEntities(PartitionKeyGroupEntity partitionKeyGroupEntity, List<String> expectedPartitionValues)
    {
        for (String expectedPartitionValue : expectedPartitionValues)
        {
            ExpectedPartitionValueEntity expectedPartitionValueEntity = new ExpectedPartitionValueEntity();
            expectedPartitionValueEntity.setPartitionKeyGroup(partitionKeyGroupEntity);
            expectedPartitionValueEntity.setPartitionValue(expectedPartitionValue);
            herdDao.saveAndRefresh(expectedPartitionValueEntity);
        }
        herdDao.saveAndRefresh(partitionKeyGroupEntity);
        assertEquals(expectedPartitionValues.size(), partitionKeyGroupEntity.getExpectedPartitionValues().size());
    }

    /**
     * Creates relative database entities required for the business object data availability service unit tests.
     */
    protected void createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(String partitionKeyGroupName, List<SchemaColumn> columns,
        List<SchemaColumn> partitionColumns, int partitionColumnPosition, List<String> subPartitionValues, boolean allowDuplicateBusinessObjectData)
    {
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(partitionKeyGroupName, columns, partitionColumns, partitionColumnPosition,
            subPartitionValues, allowDuplicateBusinessObjectData, Arrays.asList(STORAGE_NAME));
    }

    /**
     * Creates relative database entities required for the business object data availability service unit tests.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param columns the list of schema columns
     * @param partitionColumns the list of schema partition columns
     * @param partitionColumnPosition the position of the partition column (1-based numbering) that will be changing
     * @param subPartitionValues the list of sub-partition values to be used in test business object data generation
     * @param allowDuplicateBusinessObjectData specifies if business object data is allowed to be registered in multiple storages
     * @param expectedRequestStorageNames the list of storage names expected to be listed in the relative unit test availability requests when queering business
     * object data availability. This list will be used to produce the ordered list of expected available storage units.
     *
     * @return the ordered list of storage unit entities expected to be available across the specified list of storages
     */
    protected List<StorageUnitEntity> createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(String partitionKeyGroupName, List<SchemaColumn> columns,
        List<SchemaColumn> partitionColumns, int partitionColumnPosition, List<String> subPartitionValues, boolean allowDuplicateBusinessObjectData,
        List<String> expectedRequestStorageNames)
    {
        List<StorageUnitEntity> availableStorageUnits = new ArrayList<>();

        // Create relative database entities.
        String partitionKey = partitionColumns.isEmpty() ? PARTITION_KEY : partitionColumns.get(0).getName();

        // Create a business object format entity if it does not exist.
        if (herdDao
            .getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)) ==
            null)
        {
            createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, partitionKey, partitionKeyGroupName, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                SCHEMA_NULL_VALUE_BACKSLASH_N, columns, partitionColumns);
        }

        // Create storage entities if they do not exist.
        StorageEntity storageEntity1 = herdDao.getStorageByName(STORAGE_NAME);
        if (storageEntity1 == null)
        {
            storageEntity1 = createStorageEntity(STORAGE_NAME);
        }
        StorageEntity storageEntity2 = herdDao.getStorageByName(STORAGE_NAME_2);
        if (storageEntity2 == null)
        {
            storageEntity2 = createStorageEntity(STORAGE_NAME_2);
        }

        // Get storage status unit status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = herdDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ENABLED);

        // Create business object data instances and relative storage units.
        for (String partitionValue : SORTED_PARTITION_VALUES)
        {
            BusinessObjectDataEntity businessObjectDataEntity;

            // Create a business object data instance for the specified partition value.
            if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
            {
                businessObjectDataEntity =
                    createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionValue,
                        subPartitionValues, DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);
            }
            else
            {
                List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                // Please note that the second partition column is located at index 0.
                testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                businessObjectDataEntity =
                    createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);
            }

            // Check if we need to create the relative storage units.
            if (STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue))
            {
                StorageUnitEntity storageUnitEntity =
                    createStorageUnitEntity(storageEntity1, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);

                if (expectedRequestStorageNames.contains(STORAGE_NAME))
                {
                    availableStorageUnits.add(storageUnitEntity);
                }
            }

            if (STORAGE_2_AVAILABLE_PARTITION_VALUES.contains(partitionValue) &&
                (allowDuplicateBusinessObjectData || !STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue)))
            {
                StorageUnitEntity storageUnitEntity =
                    createStorageUnitEntity(storageEntity2, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);

                if (expectedRequestStorageNames.contains(STORAGE_NAME_2))
                {
                    availableStorageUnits.add(storageUnitEntity);
                }
            }
        }

        return availableStorageUnits;
    }

    protected void validateS3FileUpload(S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, List<String> expectedS3Keys)
    {
        // Validate the upload.
        List<StorageFile> actualStorageFiles = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        assertTrue(actualStorageFiles.size() == expectedS3Keys.size());

        // Build a list of the actual S3 keys.
        List<String> actualS3Keys = new ArrayList<>();
        for (StorageFile storageFile : actualStorageFiles)
        {
            actualS3Keys.add(storageFile.getFilePath());
        }

        // Check that all local test files got uploaded.
        assertTrue(expectedS3Keys.containsAll(actualS3Keys));
        assertTrue(actualS3Keys.containsAll(expectedS3Keys));
    }

    /**
     * Creates and persists a new storage policy rule type entity.
     *
     * @param code the storage policy rule type code
     *
     * @return the newly created storage policy rule type entity
     */
    protected StoragePolicyRuleTypeEntity createStoragePolicyRuleTypeEntity(String code, String description)
    {
        StoragePolicyRuleTypeEntity storagePolicyRuleTypeEntity = new StoragePolicyRuleTypeEntity();
        storagePolicyRuleTypeEntity.setCode(code);
        storagePolicyRuleTypeEntity.setDescription(description);
        return herdDao.saveAndRefresh(storagePolicyRuleTypeEntity);
    }

    /**
     * Creates and persists a storage policy entity.
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
     *
     * @return the newly created storage policy entity
     */
    protected StoragePolicyEntity createStoragePolicyEntity(StoragePolicyKey storagePolicyKey, String storagePolicyRuleType, Integer storagePolicyRuleValue,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType,
        String storageName, String destinationStorageName)
    {
        // Create a storage policy namespace entity if needed.
        NamespaceEntity storagePolicyNamespaceEntity = herdDao.getNamespaceByCd(storagePolicyKey.getNamespace());
        if (storagePolicyNamespaceEntity == null)
        {
            storagePolicyNamespaceEntity = createNamespaceEntity(storagePolicyKey.getNamespace());
        }

        // Create a storage policy rule type type entity if needed.
        StoragePolicyRuleTypeEntity storagePolicyRuleTypeEntity = herdDao.getStoragePolicyRuleTypeByCode(storagePolicyRuleType);
        if (storagePolicyRuleTypeEntity == null)
        {
            storagePolicyRuleTypeEntity = createStoragePolicyRuleTypeEntity(storagePolicyRuleType, DESCRIPTION);
        }

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = null;
        if (StringUtils.isNotBlank(businessObjectDefinitionName))
        {
            businessObjectDefinitionEntity =
                herdDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
            if (businessObjectDefinitionEntity == null)
            {
                // Create a business object definition.
                businessObjectDefinitionEntity =
                    createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, DATA_PROVIDER_NAME, BOD_DESCRIPTION);
            }
        }

        // Create a business object format file type entity if needed.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectFormatFileType))
        {
            fileTypeEntity = herdDao.getFileTypeByCode(businessObjectFormatFileType);
            if (fileTypeEntity == null)
            {
                fileTypeEntity = createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create a storage entity of S3 storage platform type if needed.
        StorageEntity storageEntity = herdDao.getStorageByName(storageName);
        if (storageEntity == null)
        {
            storageEntity = createStorageEntity(storageName, StoragePlatformEntity.S3);
        }

        // Create a destination storage entity of GLACIER storage platform type if needed.
        StorageEntity destinationStorageEntity = herdDao.getStorageByName(destinationStorageName);
        if (destinationStorageEntity == null)
        {
            destinationStorageEntity = createStorageEntity(destinationStorageName, StoragePlatformEntity.GLACIER);
        }

        // Create a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = new StoragePolicyEntity();

        storagePolicyEntity.setNamespace(storagePolicyNamespaceEntity);
        storagePolicyEntity.setName(storagePolicyKey.getStoragePolicyName());
        storagePolicyEntity.setStoragePolicyRuleType(storagePolicyRuleTypeEntity);
        storagePolicyEntity.setStoragePolicyRuleValue(storagePolicyRuleValue);
        storagePolicyEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        storagePolicyEntity.setUsage(businessObjectFormatUsage);
        storagePolicyEntity.setFileType(fileTypeEntity);
        storagePolicyEntity.setStorage(storageEntity);
        storagePolicyEntity.setDestinationStorage(destinationStorageEntity);

        return herdDao.saveAndRefresh(storagePolicyEntity);
    }
}
