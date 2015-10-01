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
import static org.junit.Assert.assertNull;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.dm.dao.impl.MockS3OperationsImpl;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.api.xml.UploadSingleInitiationResponse;
import org.finra.dm.service.impl.UploadDownloadServiceImpl;
import org.finra.dm.service.impl.UploadDownloadServiceImpl.CompleteUploadSingleMessageResult;

public class UploadDownloadServiceImplTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "uploadDownloadServiceImpl")
    private UploadDownloadService uploadDownloadServiceImpl;

    @Autowired
    private UploadDownloadService uploadDownloadService;

    /**
     * This method is to get the coverage for the upload download helper service method that starts the new transaction.
     */
    @Test
    public void testUploadDownloadServiceMethodsNewTx() throws Exception
    {
        Logger.getLogger(UploadDownloadServiceImpl.class).setLevel(Level.OFF);

        CompleteUploadSingleMessageResult result = uploadDownloadServiceImpl.performCompleteUploadSingleMessage("key_does_not_exist");

        assertNull(result.getSourceBusinessObjectDataKey());
        assertNull(result.getSourceNewStatus());
        assertNull(result.getSourceOldStatus());
        assertNull(result.getTargetBusinessObjectDataKey());
        assertNull(result.getTargetNewStatus());
        assertNull(result.getTargetOldStatus());
    }

    @Test
    public void testS3MessageS3FileSizeMismatch() throws Exception
    {
        Logger.getLogger(UploadDownloadServiceImpl.class).setLevel(Level.OFF);

        createDatabaseEntitiesForUploadDownloadTesting();

        UploadSingleInitiationResponse resultUploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(
            createUploadSingleInitiationRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NAMESPACE_CD_2, BOD_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, MockS3OperationsImpl.MOCK_S3_FILE_NAME_0_BYTE_SIZE));

        String filePath = resultUploadSingleInitiationResponse.getSourceBusinessObjectData().getStorageUnits().get(0).getStorageFiles().get(0).getFilePath();

        CompleteUploadSingleMessageResult result = uploadDownloadService.performCompleteUploadSingleMessage(filePath);

        assertEquals(BusinessObjectDataStatusEntity.DELETED, result.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getSourceOldStatus());

        assertEquals(BusinessObjectDataStatusEntity.INVALID, result.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, result.getTargetOldStatus());

        System.out.println(result);
    }
}
