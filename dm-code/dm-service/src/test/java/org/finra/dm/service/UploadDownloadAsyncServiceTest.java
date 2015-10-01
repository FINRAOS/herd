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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.dm.dao.impl.MockS3OperationsImpl;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.service.impl.UploadDownloadHelperServiceImpl;

public class UploadDownloadAsyncServiceTest extends AbstractServiceTest
{
    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    @Autowired
    private UploadDownloadAsyncService UploadDownloadAsyncService;

    @Autowired
    @Qualifier(value = "uploadDownloadAsyncServiceImpl")
    private UploadDownloadAsyncService uploadDownloadAsyncServiceImpl;

    @Test
    public void testPerformFileMoveSync() throws Exception
    {
        // Create Source and target data
        List<BusinessObjectDataEntity> entities = setUpDatabase();

        BusinessObjectDataEntity sourceBusinessObjectDataEntity = entities.get(0);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = entities.get(1);

        // Perform the file move.
        String[] statusUpdates = uploadDownloadHelperService.performFileMoveSync(dmDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                dmDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity), "", "", "FILE_NAME", MockS3OperationsImpl.MOCK_KMS_ID,
                emrHelper.getAwsParamsDto());

        // Refresh the data entities
        sourceBusinessObjectDataEntity = dmDaoHelper.getBusinessObjectDataEntity(dmDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity = dmDaoHelper.getBusinessObjectDataEntity(dmDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate that target status is marked as VALID
        assertEquals(BusinessObjectDataStatusEntity.VALID, targetBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.VALID, statusUpdates[1]);

        // Validate that source status is marked as DELETED
        assertEquals(BusinessObjectDataStatusEntity.DELETED, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.DELETED, statusUpdates[0]);
    }

    @Test
    public void testPerformFileMoveSyncError() throws Exception
    {
        Logger.getLogger(UploadDownloadHelperServiceImpl.class).setLevel(Level.OFF);

        // Create Source and target data
        List<BusinessObjectDataEntity> entities = setUpDatabase();

        BusinessObjectDataEntity sourceBusinessObjectDataEntity = entities.get(0);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = entities.get(1);

        // S3 copy will fail as fine is not found
        String[] statusUpdates = uploadDownloadHelperService.performFileMoveSync(dmDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                dmDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity), "", "", MockS3OperationsImpl.MOCK_S3_FILE_NAME_NOT_FOUND,
                MockS3OperationsImpl.MOCK_KMS_ID, emrHelper.getAwsParamsDto());

        // Refresh the data entities
        sourceBusinessObjectDataEntity = dmDaoHelper.getBusinessObjectDataEntity(dmDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity = dmDaoHelper.getBusinessObjectDataEntity(dmDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate that target status is marked as INVALID
        assertEquals(BusinessObjectDataStatusEntity.INVALID, targetBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, statusUpdates[1]);

        // Validate that source status is marked as DELETED
        assertEquals(BusinessObjectDataStatusEntity.DELETED, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.DELETED, statusUpdates[0]);
    }

    @Test
    public void testPerformFileMoveASync() throws Exception
    {
        Logger.getLogger(UploadDownloadHelperServiceImpl.class).setLevel(Level.OFF);

        // Create Source and target data
        List<BusinessObjectDataEntity> entities = setUpDatabase();

        BusinessObjectDataEntity sourceBusinessObjectDataEntity = entities.get(0);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = entities.get(1);

        // Trigger the notification
        Future<Void> future = UploadDownloadAsyncService.performFileMoveAsync(dmDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                dmDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity), "", "", "FILE_NAME", MockS3OperationsImpl.MOCK_KMS_ID,
                emrHelper.getAwsParamsDto());

        while (!future.isDone())
        {
            Thread.sleep(10);
        }
    }

    private List<BusinessObjectDataEntity> setUpDatabase()
    {
        // Create source business Object.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity =
                createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                        INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.RE_ENCRYPTING);

        // Create target business Object.
        BusinessObjectDataEntity targetBusinessObjectDataEntity =
                createBusinessObjectDataEntity(NAMESPACE_CD_2, BOD_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION,
                        PARTITION_VALUE,
                        INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.RE_ENCRYPTING);

        List<BusinessObjectDataEntity> entities = new ArrayList<>();
        entities.add(sourceBusinessObjectDataEntity);
        entities.add(targetBusinessObjectDataEntity);

        return entities;
    }

    /**
     * This method is to get the coverage for the upload download helper service method that starts the new transaction.
     */
    @Test
    public void testUploadDownloadHelperServiceMethodsAsync() throws Exception
    {
        Logger.getLogger(UploadDownloadHelperServiceImpl.class).setLevel(Level.OFF);

        // Create Source and target data
        List<BusinessObjectDataEntity> entities = setUpDatabase();

        BusinessObjectDataEntity sourceBusinessObjectDataEntity = entities.get(0);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = entities.get(1);

        uploadDownloadAsyncServiceImpl.performFileMoveAsync(dmDaoHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
            dmDaoHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity), "", "", "FILE_NAME", MockS3OperationsImpl.MOCK_KMS_ID,
            emrHelper.getAwsParamsDto());
    }
}
