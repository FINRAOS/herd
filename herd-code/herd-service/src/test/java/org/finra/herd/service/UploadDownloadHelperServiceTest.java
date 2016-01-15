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
import static org.junit.Assert.fail;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.service.impl.UploadDownloadHelperServiceImpl;

public class UploadDownloadHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "uploadDownloadHelperServiceImpl")
    private UploadDownloadHelperService uploadDownloadHelperServiceImpl;

    /**
     * This method is to get the coverage for the upload download helper service method that starts the new transaction.
     */
    @Test
    public void testUploadDownloadHelperServiceMethodsNewTx() throws Exception
    {
        Logger.getLogger(UploadDownloadHelperServiceImpl.class).setLevel(Level.OFF);

        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION);

        uploadDownloadHelperServiceImpl.performFileMoveSync(businessObjectDataKey, businessObjectDataKey, null, null, null, null, emrHelper.getAwsParamsDto());

        try
        {
            uploadDownloadHelperServiceImpl.updateBusinessObjectDataStatus(businessObjectDataKey, null);
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }
}
