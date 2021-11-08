package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests the createBusinessObjectData functionality within the business object data dao helper.
 */
public class BusinessObjectDataDaoHelperTest extends AbstractServiceTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDataDaoHelperTest.class);

    private static final String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, INITIAL_DATA_VERSION);

    private Path localTempPath;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create a local temp directory.
        localTempPath = Files.createTempDirectory(null);
    }

    /**
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv()
    {
        try
        {
            // Clean up the local directory.
            FileUtils.deleteDirectory(localTempPath.toFile());

            // Clean up the destination S3 folder.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3KeyPrefix(testS3KeyPrefix);
            s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
        }
        catch (final Exception exception)
        {
            // If an exception is thrown by one of the @Test methods, some cleanup operations could also fail. This is why we are just logging a warning here.
            LOGGER.warn("Unable to cleanup environment.", exception);
        }
    }

    @Test
    public void testCreateBusinessObjectData()
    {
        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(businessObjectDataCreateRequest, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataWithFullFilePath()
    {
        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData =
            businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(businessObjectDataCreateRequest, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataWithMinimizedFilePath()
    {
        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData =
            businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(businessObjectDataCreateRequest, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoStorageDirectoryWithFullFilePath()
    {
        // Create an initial version of the business object data without specifying a storage directory.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        request.getStorageUnits().get(0).setStorageDirectory(null);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoStorageDirectoryWithMinimizedFilePath()
    {
        // Create an initial version of the business object data without specifying a storage directory.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        request.getStorageUnits().get(0).setStorageDirectory(null);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoStorageFilesWithFullFilePath()
    {
        // Create the initial version of the business object data without specifying storage files.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        request.getStorageUnits().get(0).setStorageFiles(null);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoStorageFilesWithMinimizedFilePath()
    {
        // Create the initial version of the business object data without specifying storage files.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        request.getStorageUnits().get(0).setStorageFiles(null);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataNoAttributesWithFullFilePath()
    {
        // Create business object data with no attribute definitions and no attributes which is valid.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(false);

        // Call the method under test to create a business object data object.
        businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);
    }

    @Test
    public void testCreateBusinessObjectDataNoAttributesWithMinimizedFilePath()
    {
        // Create business object data with no attribute definitions and no attributes which is valid.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest(false);

        // Call the method under test to create a business object data object.
        businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketWithFullFilePath() throws Exception
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                StorageEntity.MANAGED_STORAGE, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketWithMinimizedFilePath() throws Exception
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                StorageEntity.MANAGED_STORAGE, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketExtraFilesInS3WithFullFilePath() throws Exception
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files including extra
        // files not to be listed in the create business object data create request.
        List<String> localFiles = new ArrayList<>(LOCAL_FILES);
        localFiles.add(FILE_NAME);
        localFiles.add(FILE_NAME_2);
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, localFiles);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                StorageEntity.MANAGED_STORAGE, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataS3ManagedBucketExtraFilesInS3WithMinimizedFilePath() throws Exception
    {
        // Create relative database entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, false, PARTITION_KEY);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files including extra
        // files not to be listed in the create business object data create request.
        List<String> localFiles = new ArrayList<>(LOCAL_FILES);
        localFiles.add(FILE_NAME);
        localFiles.add(FILE_NAME_2);
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, localFiles);

        // Build a new business object data create request.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper
            .createBusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, BDATA_STATUS, StorageEntity.MANAGED_STORAGE, testS3KeyPrefix,
                businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, LOCAL_FILES));

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectFormatEntity, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS,
                StorageEntity.MANAGED_STORAGE, testS3KeyPrefix, businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES),
                CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<>() : request.getAttributes(), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesWithFullFilePath() throws Exception
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Build a new business object data create request with enabled discovery of storage files.
        BusinessObjectDataCreateRequest request =
            new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS, Lists.newArrayList(
                new StorageUnitCreateRequest(StorageEntity.MANAGED_STORAGE, new StorageDirectory(testS3KeyPrefix), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        // Verify the results.
        assertEquals(
            new BusinessObjectData(resultBusinessObjectData.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, Lists.newArrayList(
                new StorageUnit(new Storage(StorageEntity.MANAGED_STORAGE, StoragePlatformEntity.S3, Lists.newArrayList(
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                        storageDaoTestHelper.getS3ManagedBucketName()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), Boolean.TRUE.toString()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()))),
                    new StorageDirectory(testS3KeyPrefix), businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES, false),
                    StorageUnitStatusEntity.ENABLED, NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
                NO_RETENTION_EXPIRATION_DATE, HerdDaoSecurityHelper.SYSTEM_USER, resultBusinessObjectData.getCreatedOn()), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesWithMinimizedFilePath() throws Exception
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Build a new business object data create request with enabled discovery of storage files.
        BusinessObjectDataCreateRequest request =
            new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS, Lists.newArrayList(
                new StorageUnitCreateRequest(StorageEntity.MANAGED_STORAGE, new StorageDirectory(testS3KeyPrefix), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        assertEquals(
            new BusinessObjectData(resultBusinessObjectData.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, Lists.newArrayList(
                new StorageUnit(new Storage(StorageEntity.MANAGED_STORAGE, StoragePlatformEntity.S3, Lists.newArrayList(
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                        storageDaoTestHelper.getS3ManagedBucketName()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        S3_KEY_PREFIX_VELOCITY_TEMPLATE),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), Boolean.TRUE.toString()),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()))),
                    new StorageDirectory(testS3KeyPrefix), businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES, false),
                    StorageUnitStatusEntity.ENABLED, NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY,
                NO_RETENTION_EXPIRATION_DATE, HerdDaoSecurityHelper.SYSTEM_USER, resultBusinessObjectData.getCreatedOn()), resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesStorageDirectoryEndsWithSlashWithFullFilePath() throws Exception
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Create an S3 storage entity with a bucket name attribute with a value matching to the test S3 managed storage (required for unit test clean up).
        String testBucketName = storageDaoTestHelper.getS3ManagedBucketName();
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                testBucketName);

        // Build a new business object data create request with enabled discovery of storage files and with storage directory ending with a slash.
        String testStorageDirectoryPath = testS3KeyPrefix + "/";
        BusinessObjectDataCreateRequest request =
            new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS, Lists.newArrayList(
                new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(testStorageDirectoryPath), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        // Verify the results.
        assertEquals(
            new BusinessObjectData(resultBusinessObjectData.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, Lists.newArrayList(
                new StorageUnit(new Storage(STORAGE_NAME, StoragePlatformEntity.S3,
                    Lists.newArrayList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), testBucketName))),
                    new StorageDirectory(testStorageDirectoryPath),
                    businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES, false), StorageUnitStatusEntity.ENABLED,
                    NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)), NO_ATTRIBUTES,
                NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY, NO_RETENTION_EXPIRATION_DATE,
                HerdDaoSecurityHelper.SYSTEM_USER, resultBusinessObjectData.getCreatedOn()),
            resultBusinessObjectData);
    }

    @Test
    public void testCreateBusinessObjectDataDiscoverStorageFilesStorageDirectoryEndsWithSlashWithMinimizedFilePath() throws Exception
    {
        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // Create and upload to S3 managed storage a set of test files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Create an S3 storage entity with a bucket name attribute with a value matching to the test S3 managed storage (required for unit test clean up).
        String testBucketName = storageDaoTestHelper.getS3ManagedBucketName();
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                testBucketName);

        // Build a new business object data create request with enabled discovery of storage files and with storage directory ending with a slash.
        String testStorageDirectoryPath = testS3KeyPrefix + "/";
        BusinessObjectDataCreateRequest request =
            new BusinessObjectDataCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, BDATA_STATUS, Lists.newArrayList(
                new StorageUnitCreateRequest(STORAGE_NAME, new StorageDirectory(testStorageDirectoryPath), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES)),
                NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_CREATE_NEW_VERSION);

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        assertEquals(
            new BusinessObjectData(resultBusinessObjectData.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, Lists.newArrayList(
                new StorageUnit(new Storage(STORAGE_NAME, StoragePlatformEntity.S3,
                    Lists.newArrayList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), testBucketName))),
                    new StorageDirectory(testStorageDirectoryPath),
                    businessObjectDataServiceTestHelper.getTestStorageFiles(testS3KeyPrefix, SORTED_LOCAL_FILES, false), StorageUnitStatusEntity.ENABLED,
                    NO_STORAGE_UNIT_STATUS_HISTORY, NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)), NO_ATTRIBUTES,
                NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY, NO_RETENTION_EXPIRATION_DATE,
                HerdDaoSecurityHelper.SYSTEM_USER, resultBusinessObjectData.getCreatedOn()),
            resultBusinessObjectData);
    }

    @Test
    public void
    testCreateBusinessObjectDataPreRegistrationAssertDirectoryPathNotRequiredWhenStatusIsPreRegistrationAndDirectoryIsSetInResponseWithFullFilePath()
    {
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                "foo"));

        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setSubPartitionValues(null);
        businessObjectDataCreateRequest.setStatus("UPLOADING");
        businessObjectDataCreateRequest.setStorageUnits(Lists.newArrayList(new StorageUnitCreateRequest(STORAGE_NAME, null, null, null)));

        // Call the method under test to create a business object data object.
        BusinessObjectData businessObjectData =
            businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest, FILE_SIZE_REQUIRED, USE_FULL_FILE_PATH);

        assertEquals("foo", businessObjectData.getStorageUnits().get(0).getStorageDirectory().getDirectoryPath());
    }

    @Test
    public void
    testCreateBusinessObjectDataPreRegistrationAssertDirectoryPathNotRequiredWhenStatusIsPreRegistrationAndDirectoryIsSetInResponseWithMinimizedFilePath()
    {
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME);
        storageEntity.getAttributes().add(storageDaoTestHelper
            .createStorageAttributeEntity(storageEntity, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                "foo"));

        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setSubPartitionValues(null);
        businessObjectDataCreateRequest.setStatus("UPLOADING");
        businessObjectDataCreateRequest.setStorageUnits(Lists.newArrayList(new StorageUnitCreateRequest(STORAGE_NAME, null, null, null)));

        // Call the method under test to create a business object data object.
        BusinessObjectData businessObjectData =
            businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        assertEquals("foo", businessObjectData.getStorageUnits().get(0).getStorageDirectory().getDirectoryPath());
    }

    @Test
    public void
    testCreateBusinessObjectDataWithS3EmptyPartitionWithMinimizedFilePath()
    {
        // Create the initial version of the business object data without specifying storage files.
        BusinessObjectDataCreateRequest request = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();

        String directoryPath = request.getStorageUnits().get(0).getStorageDirectory().getDirectoryPath();

        List<StorageFile> storageFiles = request.getStorageUnits().get(0).getStorageFiles();

        for (StorageFile storageFile : storageFiles)
        {
            storageFile.setFilePath(directoryPath + StorageFileEntity.S3_EMPTY_PARTITION);
        }

        // Call the method under test to create a business object data object.
        BusinessObjectData resultBusinessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(request, FILE_SIZE_REQUIRED, NO_USE_FULL_FILE_PATH);

        // Verify the results.
        businessObjectDataServiceTestHelper.validateBusinessObjectData(request, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }
}
