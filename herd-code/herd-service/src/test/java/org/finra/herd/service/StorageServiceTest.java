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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.persistence.PersistenceException;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.junit.Ignore;
import org.junit.Test;

import org.finra.herd.core.Command;
import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStat;
import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageDailyUploadStat;
import org.finra.herd.model.api.xml.StorageDailyUploadStats;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests various functionality within the storage REST controller.
 */
public class StorageServiceTest extends AbstractServiceTest
{
    private static final int PAST_UPLOAD_DATES_TO_REPORT_ON = 7;

    private static final int TODAY_UPLOAD_DATE = 1;

    private static final int ADDITIONAL_UPLOAD_DATE = 1;

    private static final int BDEFS_PER_DAY = 3;

    private static final int FORMATS_PER_BDEF = 2;

    private static final int FILES_PER_FORMAT = 5;

    @Test
    public void testCreateStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();

        Storage storage = storageService.createStorage(storageCreateRequest);

        assertNotNull(storage);
        assertTrue(storage.getName().equals(storageCreateRequest.getName()));

        // Check if result list of attributes matches to the list from the create request.
        validateAttributes(storageCreateRequest.getAttributes(), storage.getAttributes());
    }

    @Test
    public void testCreateStorageMissingOptionalParameters() throws Exception
    {
        // Create and persist a valid storage without specifying optional parameters.
        StorageCreateRequest request = getNewStorageCreateRequest();
        request.setAttributes(null);
        Storage storage = storageService.createStorage(request);

        // Validate the returned object.
        assertNotNull(storage);
        assertTrue(storage.getName().equals(request.getName()));
        assertTrue(storage.getAttributes().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateStorageNoName() throws Exception
    {
        // Leave the storage name blank which is invalid.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageCreateRequest.setName(null);
        storageService.createStorage(storageCreateRequest);
    }

    @Test(expected = AlreadyExistsException.class)
    public void testCreateStorageAlreadyExists() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageService.createStorage(storageCreateRequest);

        // Try creating it again which is invalid since it already exists.
        storageService.createStorage(storageCreateRequest);
    }

    @Test
    public void testUpdateStorage() throws Exception
    {
        // Create a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageService.createStorage(storageCreateRequest);

        // Update the storage platform which is valid.
        StorageUpdateRequest storageUpdateRequest = new StorageUpdateRequest();

        // TODO: Update various attributes of the storage update request in the future when there is something to update.

        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageCreateRequest.getName()).build();
        storageService.updateStorage(alternateKey, storageUpdateRequest);

        // TODO: Add asserts to ensure fields that were update indeed got updated.
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testUpdateStorageNoExists() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();

        // Try updating a storage that doesn't yet exist which is invalid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageCreateRequest.getName()).build();
        storageService.updateStorage(alternateKey, new StorageUpdateRequest());
    }

    @Test
    public void testGetStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        String name = storageCreateRequest.getName();
        Storage storage = storageService.createStorage(storageCreateRequest);

        // Retrieve the storage by it's name which is valid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storage.getName()).build();
        storage = storageService.getStorage(alternateKey);
        assertNotNull(storage);
        assertTrue(storage.getName().equals(name));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetStorageInvalidName() throws Exception
    {
        // Try getting a storage that doesn't exist which is invalid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName("invalid" + getRandomSuffix()).build();
        storageService.getStorage(alternateKey);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testDeleteStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        String name = storageCreateRequest.getName();
        Storage storage = storageService.createStorage(storageCreateRequest);

        // Delete the storage by it's name which is valid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storage.getName()).build();
        storage = storageService.deleteStorage(alternateKey);
        assertNotNull(storage);
        assertTrue(storage.getName().equals(name));

        // Retrieve the storage by it's name and verify that it doesn't exist.
        storageService.getStorage(alternateKey);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testDeleteStorageInvalidName() throws Exception
    {
        // Delete a storage which doesn't exist.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(getNewStorageCreateRequest().getName()).build();
        storageService.deleteStorage(alternateKey);
    }

    /*
     * This test is ignored because the constraint validation is a DB dependent feature. This method had inconsistent behavior between Oracle and PostgreSQL.
     * Oracle was throwing the error after each statement, whereas PostgreSQL would not because it by default raises error only when transaction is committed.
     * 
     * Besides, this test case is not valid as a use case as normal transactions wouldn't delete after insert within same transaction.
     */
    @Ignore
    @Test(expected = PersistenceException.class)
    public void testDeleteStorageConstraintViolation() throws Exception
    {
        // Create a storage unit entity that refers to a newly created storage.
        final StorageUnitEntity storageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        executeWithoutLogging(SqlExceptionHelper.class, new Command()
        {
            @Override
            public void execute()
            {
                // Delete the storage which is invalid because there still exists a storage unit entity that references it.
                StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageUnitEntity.getStorage().getName()).build();
                storageService.deleteStorage(alternateKey);
            }
        });
    }

    @Test
    public void testGetStorages() throws Exception
    {
        // Create and persist test storage entities.
        for (StorageKey key : getTestStorageKeys())
        {
            createStorageEntity(key.getStorageName());
        }

        // Retrieve a list of storage keys.
        StorageKeys resultStorageKeys = storageService.getStorages();

        // Validate the returned object.
        assertNotNull(resultStorageKeys);
        assertTrue(resultStorageKeys.getStorageKeys().containsAll(getTestStorageKeys()));
    }

    @Test
    public void testGetStorageUploadStatsSimple()
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName("S3_MANAGED").build();
        storageService.getStorageUploadStats(alternateKey, null);
    }

    @Test
    public void testGetStorageUploadStatsByBusinessObjectDefinitionSimple()
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName("S3_MANAGED").build();
        storageService.getStorageUploadStatsByBusinessObjectDefinition(alternateKey, null);
    }

    /*
     * Ignoring this test as it requires using a database view, but it is impossible to update a view within a same transaction. This test should be enabled
     * again when in-memory database is implemented for unit tests.
     */
    @Ignore
    @Test
    public void testGetStorageUploadStats() throws JAXBException, IOException
    {
        SimpleDateFormat sdf = new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);
        Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();
        StorageDailyUploadStats uploadStats;

        // Prepare test environment.
        prepareUploadStatsTestData();

        // Validate that we get no records back, when we specify wrong (5 days into the future) upload date.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(STORAGE_NAME).build();
        uploadStats = storageService.getStorageUploadStats(alternateKey, storageHelper.getDateFromString(sdf.format(HerdDateUtils.addDays(currentDate, 5))));
        assertTrue(uploadStats.getStorageDailyUploadStats().isEmpty());

        // Validate upload stats for the entire range of upload dates (partition values) pre-populated in the test storage.
        int index = 0;
        Date startDate = HerdDateUtils.addDays(currentDate, -(PAST_UPLOAD_DATES_TO_REPORT_ON + ADDITIONAL_UPLOAD_DATE));
        Date endDate = HerdDateUtils.addDays(currentDate, ADDITIONAL_UPLOAD_DATE);
        long expectedTotalFiles = BDEFS_PER_DAY * FORMATS_PER_BDEF * FILES_PER_FORMAT;
        long expectedTotalBytes = expectedTotalFiles * FILE_SIZE_1_KB;

        for (Date date = startDate; !date.after(endDate); date = HerdDateUtils.addDays(date, 1))
        {
            // Retrieve and validate upload stats for each upload date (partition value) loaded in the test storage.
            uploadStats = storageService.getStorageUploadStats(alternateKey, storageHelper.getDateFromString(sdf.format(date)));
            assertTrue(uploadStats.getStorageDailyUploadStats().size() == 1);
            XMLGregorianCalendar expectedUploadDate = HerdDateUtils.getXMLGregorianCalendarValue(date);
            assertTrue(uploadStats.getStorageDailyUploadStats().get(0).getUploadDate().equals(expectedUploadDate));
            assertTrue(uploadStats.getStorageDailyUploadStats().get(0).getTotalFiles() == expectedTotalFiles);
            assertTrue(uploadStats.getStorageDailyUploadStats().get(0).getTotalBytes() == expectedTotalBytes);
            index++;
        }

        // Just in case, validate that we checked the expected number of records.
        assertTrue(index == (ADDITIONAL_UPLOAD_DATE + PAST_UPLOAD_DATES_TO_REPORT_ON + ADDITIONAL_UPLOAD_DATE + ADDITIONAL_UPLOAD_DATE));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetStorageUploadStatsWrongStorageName() throws JAXBException, IOException
    {
        SimpleDateFormat sdf = new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);
        Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();

        // Try to get storage upload statistics for a non-existing storage.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName("I_DO_NOT_EXIST").build();
        storageService.getStorageUploadStats(alternateKey, storageHelper.getDateFromString(sdf.format(currentDate)));
    }

    @Test
    public void testGetStorageUploadStatsInvalidUploadDateLength()
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(StorageEntity.MANAGED_STORAGE).build();

        // Try to get upload stats using invalid upload date values.
        for (String uploadDateString : Arrays.asList("  20140816", "2014-01-01-EXTRA-TEXT"))
        {
            try
            {
                storageService.getStorageUploadStats(alternateKey, storageHelper.getDateFromString(uploadDateString));
                fail(String.format("Should throw an IllegalArgumentException when upload date is not a valid date in %s format.",
                    AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.toUpperCase()));
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("A date value \"%s\" must contain %d characters and be in \"%s\" format.", uploadDateString.trim(),
                    AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.length(), AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.toUpperCase()), e.getMessage());
            }
        }
    }

    @Test
    public void testGetStorageUploadStatsCorrectUploadDateLengthInvalidUploadDateValue()
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(StorageEntity.MANAGED_STORAGE).build();

        // Try to get upload stats using invalid upload date values.
        for (String uploadDateString : Arrays.asList("INVALID_DT", "  20140816YZ", " 2014-13-01  ", "2014-12-32"))
        {
            try
            {
                storageService.getStorageUploadStats(alternateKey, storageHelper.getDateFromString(uploadDateString));
                fail(String.format("Should throw an IllegalArgumentException when upload date is not a valid date in %s format.",
                    AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.toUpperCase()));
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("A date value \"%s\" must be in \"%s\" format.", uploadDateString.trim(),
                    AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.toUpperCase()), e.getMessage());
            }
        }
    }

    /*
     * Ignoring this test as it requires using a database view, but it is impossible to update a view within a same transaction. This test should be enabled
     * again when in-memory database is implemented for unit tests.
     */
    @Ignore
    @Test
    public void testGetStorageUploadStatsNoUploadDateSpecified() throws JAXBException, IOException
    {
        StorageDailyUploadStats uploadStats;

        // Prepare test environment.
        prepareUploadStatsTestData();

        // Validate that we get back the number of records expected for the case when no upload date is specified.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(STORAGE_NAME).build();
        uploadStats = storageService.getStorageUploadStats(alternateKey, storageHelper.getDateFromString(" "));
        int expectedRecordCount = PAST_UPLOAD_DATES_TO_REPORT_ON + TODAY_UPLOAD_DATE;
        assertTrue(uploadStats.getStorageDailyUploadStats().size() == expectedRecordCount);

        // Validate the stats of business object data pre-loaded in the test storage.
        int index = 0;
        Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();
        long expectedTotalFiles = BDEFS_PER_DAY * FORMATS_PER_BDEF * FILES_PER_FORMAT;
        long expectedTotalBytes = expectedTotalFiles * FILE_SIZE_1_KB;
        for (Date date = HerdDateUtils.addDays(currentDate, -PAST_UPLOAD_DATES_TO_REPORT_ON); !date.after(currentDate); date = HerdDateUtils.addDays(date, 1))
        {
            // Validate upload stats for each date.
            StorageDailyUploadStat uploadStat = uploadStats.getStorageDailyUploadStats().get(index);
            assertTrue(uploadStat.getUploadDate().equals(HerdDateUtils.getXMLGregorianCalendarValue(date)));
            assertTrue(uploadStat.getTotalFiles() == expectedTotalFiles);
            assertTrue(uploadStat.getTotalBytes() == expectedTotalBytes);
            index++;
        }

        // Just in case, validate that we checked the expected number of upload stats records.
        assertTrue(index == expectedRecordCount);
    }

    /**
     * TODO: Ignoring this test as it requires using a database view, but it is impossible to update a view within a same transaction. This test should be
     * enabled again when in-memory database is implemented for unit tests. We should see if the H2 in-memory database supports views. Otherwise, we will need
     * to populate the "view" table with the appropriate data which is less ideal.
     */
    @Ignore
    @Test
    public void testGetStorageUploadStatsByBusinessObjectDefinition() throws JAXBException, IOException
    {
        SimpleDateFormat sdf = new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);
        Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();
        StorageBusinessObjectDefinitionDailyUploadStats uploadStats;

        // Prepare test environment.
        prepareUploadStatsTestData();

        // Validate that we get no records back, when we specify wrong (5 days into the future) upload date.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(STORAGE_NAME).build();
        uploadStats = storageService
            .getStorageUploadStatsByBusinessObjectDefinition(alternateKey, storageHelper.getDateFromString(sdf.format(HerdDateUtils.addDays(currentDate, 5))));
        assertTrue(uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().isEmpty());

        // Validate all days for business object data pre-created in the test storage.
        int index = 0;
        Date startDate = HerdDateUtils.addDays(currentDate, -(PAST_UPLOAD_DATES_TO_REPORT_ON + ADDITIONAL_UPLOAD_DATE));
        Date endDate = HerdDateUtils.addDays(currentDate, ADDITIONAL_UPLOAD_DATE);
        long expectedTotalFiles = FORMATS_PER_BDEF * FILES_PER_FORMAT;
        long expectedTotalBytes = expectedTotalFiles * FILE_SIZE_1_KB;

        for (Date date = startDate; !date.after(endDate); date = HerdDateUtils.addDays(date, 1))
        {
            uploadStats = storageService.getStorageUploadStatsByBusinessObjectDefinition(alternateKey, storageHelper.getDateFromString(sdf.format(date)));

            // For each upload date, iterate over business objects definitions and validate the upload stats.
            assertTrue(uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().size() == BDEFS_PER_DAY);
            XMLGregorianCalendar expectedUploadDate = HerdDateUtils.getXMLGregorianCalendarValue(date);
            for (int i = 0; i < BDEFS_PER_DAY; i++)
            {
                // Validate upload stats.
                StorageBusinessObjectDefinitionDailyUploadStat uploadStat = uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().get(i);
                String expectedBdefName = String.format("%s_%d", BDEF_NAME, i);
                assertTrue(uploadStat.getUploadDate().equals(expectedUploadDate));
                assertTrue(uploadStat.getNamespace().equals(NAMESPACE));
                assertTrue(uploadStat.getDataProviderName().equals(DATA_PROVIDER_NAME));
                assertTrue(uploadStat.getBusinessObjectDefinitionName().equals(expectedBdefName));
                assertTrue(uploadStat.getTotalFiles() == expectedTotalFiles);
                assertTrue(uploadStat.getTotalBytes() == expectedTotalBytes);
            }

            index++;
        }

        // Just in case, validate that we checked the expected number of upload dates.
        assertTrue(index == (ADDITIONAL_UPLOAD_DATE + PAST_UPLOAD_DATES_TO_REPORT_ON + TODAY_UPLOAD_DATE + ADDITIONAL_UPLOAD_DATE));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetStorageUploadStatsByBusinessObjectDefinitionWrongStorageName() throws JAXBException, IOException
    {
        SimpleDateFormat sdf = new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);
        Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();

        // Try to get storage upload statistics for a non-existing storage.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName("I_DO_NOT_EXIST").build();
        storageService.getStorageUploadStatsByBusinessObjectDefinition(alternateKey, storageHelper.getDateFromString(sdf.format(currentDate)));
    }

    /*
     * Ignoring this test as it requires using a database view, but it is impossible to update a view within a same transaction. This test should be enabled
     * again when in-memory database is implemented for unit tests.
     */
    @Ignore
    @Test
    public void testGetStorageUploadStatsByBusinessObjectDefinitionNoUploadDateSpecified() throws JAXBException, IOException
    {
        StorageBusinessObjectDefinitionDailyUploadStats uploadStats;

        // Prepare test environment.
        prepareUploadStatsTestData();

        // Validate that we get back the number of records expected for the case when no upload date is specified.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(STORAGE_NAME).build();
        uploadStats = storageService.getStorageUploadStatsByBusinessObjectDefinition(alternateKey, storageHelper.getDateFromString(" "));
        int expectedRecordCount = (PAST_UPLOAD_DATES_TO_REPORT_ON + TODAY_UPLOAD_DATE) * BDEFS_PER_DAY;
        assertTrue(uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().size() == expectedRecordCount);

        // Validate the upload stats.
        int index = 0;
        Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();
        long expectedTotalFiles = FORMATS_PER_BDEF * FILES_PER_FORMAT;
        long expectedTotalBytes = expectedTotalFiles * FILE_SIZE_1_KB;

        for (Date date = HerdDateUtils.addDays(currentDate, -PAST_UPLOAD_DATES_TO_REPORT_ON); !date.after(currentDate); date = HerdDateUtils.addDays(date, 1))
        {
            // For each upload date, iterate over business objects definitions and validate the upload stats.
            XMLGregorianCalendar expectedUploadDate = HerdDateUtils.getXMLGregorianCalendarValue(date);
            for (int i = 0; i < BDEFS_PER_DAY; i++)
            {
                // Validate each upload statistics record.
                StorageBusinessObjectDefinitionDailyUploadStat uploadStat = uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().get(index);
                String expectedBdefName = String.format("%s_%d", BDEF_NAME, i);
                assertTrue(uploadStat.getUploadDate().equals(expectedUploadDate));
                assertTrue(uploadStat.getNamespace().equals(NAMESPACE));
                assertTrue(uploadStat.getDataProviderName().equals(DATA_PROVIDER_NAME));
                assertTrue(uploadStat.getBusinessObjectDefinitionName().equals(expectedBdefName));
                assertTrue(uploadStat.getTotalFiles() == expectedTotalFiles);
                assertTrue(uploadStat.getTotalBytes() == expectedTotalBytes);
                index++;
            }
        }

        // Just in case, validate that we checked the expected number of upload stats records.
        assertTrue(index == expectedRecordCount);
    }

    /**
     * Creates (but does not persist) a new valid storage create request.
     *
     * @return a new storage.
     */
    private StorageCreateRequest getNewStorageCreateRequest()
    {
        String name = "StorageTest" + getRandomSuffix();
        StorageCreateRequest storageRequest = new StorageCreateRequest();
        storageRequest.setStoragePlatformName(StoragePlatformEntity.S3);
        storageRequest.setName(name);
        storageRequest.setAttributes(getNewAttributes());
        return storageRequest;
    }

    /**
     * Creates the relative database entities required to test the get upload stats REST API.
     */
    private void prepareUploadStatsTestData()
    {
        // Create relative database entities.
        StorageEntity storageEntity = createStorageEntity(STORAGE_NAME);
        createNamespaceEntity(NAMESPACE);
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Create test business object format file types.
        for (int i = 0; i < FORMATS_PER_BDEF; i++)
        {
            String formatFileTypeCode = String.format("%s_%d", FORMAT_FILE_TYPE_CODE, i);
            createFileTypeEntity(formatFileTypeCode, "Description of " + formatFileTypeCode);
        }

        // Create test business object definitions.
        for (int x = 0; x < BDEFS_PER_DAY; x++)
        {
            String bdefName = String.format("%s_%d", BDEF_NAME, x);
            createBusinessObjectDefinitionEntity(NAMESPACE, bdefName, DATA_PROVIDER_NAME, "Description of " + bdefName);

            // Create relative business object formats for each of the business object definitions.
            for (int y = 0; y < FORMATS_PER_BDEF; y++)
            {
                String formatUsageCode = String.format("%s_%d", FORMAT_USAGE_CODE, y);
                String formatFileTypeCode = String.format("%s_%d", FORMAT_FILE_TYPE_CODE, y);
                createBusinessObjectFormatEntity(NAMESPACE, bdefName, formatUsageCode, formatFileTypeCode, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                    Boolean.FALSE, PARTITION_KEY);

                // For each format, iterate over (ADDITIONAL_UPLOAD_DATE + PAST_UPLOAD_DATES_TO_REPORT_ON +
                // TODAY_UPLOAD_DATE + ADDITIONAL_UPLOAD_DATE) number of days...
                SimpleDateFormat sdf = new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);
                Date currentDate = HerdDateUtils.getCurrentCalendarNoTime().getTime();
                Date startDate = HerdDateUtils.addDays(currentDate, -(PAST_UPLOAD_DATES_TO_REPORT_ON + ADDITIONAL_UPLOAD_DATE));
                Date endDate = HerdDateUtils.addDays(currentDate, ADDITIONAL_UPLOAD_DATE);

                for (Date date = startDate; !date.after(endDate); date = HerdDateUtils.addDays(date, 1))
                {
                    String partitionValue = sdf.format(date);
                    BusinessObjectDataEntity bode =
                        createBusinessObjectDataEntity(NAMESPACE, bdefName, formatUsageCode, formatFileTypeCode, INITIAL_FORMAT_VERSION, partitionValue,
                            INITIAL_DATA_VERSION, Boolean.FALSE, BDATA_STATUS);
                    StorageUnitEntity storageUnitEntity =
                        createStorageUnitEntity(storageEntity, bode, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

                    // For each day and format, create database entries for the relative storage files.
                    for (int z = 0; z < FILES_PER_FORMAT; z++)
                    {
                        String s3FilePath = String.format("%s/%d_%s",
                            getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, bdefName, formatUsageCode, formatFileTypeCode, INITIAL_FORMAT_VERSION,
                                PARTITION_KEY, partitionValue, null, null, INITIAL_DATA_VERSION), z + 1, LOCAL_FILE);

                        StorageFileEntity storageFileEntity = createStorageFileEntity(storageUnitEntity, s3FilePath, FILE_SIZE_1_KB, ROW_COUNT_1000);
                        // For the storage upload stats unit tests, we need storageFileEntity.createdOn value
                        // to be set to match the relative partition value instead of defaulting to Oracle's SYSDATE.
                        storageFileEntity.setCreatedOn(new Timestamp(date.getTime()));
                    }
                }
            }
        }
    }
}
