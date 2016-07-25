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
package org.finra.herd.rest;

import java.util.Arrays;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.service.helper.Hive13DdlGenerator;

/**
 * This class tests generateBusinessObjectDataDdl functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerGenerateBusinessObjectDataDdlTest extends AbstractRestTest
{
    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueList()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;

        // Retrieve business object data ddl.
        request = getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataRestController.generateBusinessObjectDataDdl(request);

        // Validate the results.
        validateBusinessObjectDataDdl(request, getExpectedDdl(), resultDdl);

        // Retrieve business object data ddl when request partition value list has duplicate values.
        request = getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.getPartitionValueFilters().get(0).getPartitionValues().add(UNSORTED_PARTITION_VALUES.get(0));
        resultDdl = businessObjectDataRestController.generateBusinessObjectDataDdl(request);

        // Validate the results.
        validateBusinessObjectDataDdl(request, getExpectedDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueListStandalonePartitionValueFilter()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;

        // Retrieve business object data ddl using request with a standalone partition value filter.
        request = getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        request.setPartitionValueFilter(request.getPartitionValueFilters().get(0));
        request.setPartitionValueFilters(null);
        resultDdl = businessObjectDataRestController.generateBusinessObjectDataDdl(request);

        // Validate the results.
        validateBusinessObjectDataDdl(request, getExpectedDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlPartitionValueRange()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataDdlTesting();
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);

        BusinessObjectDataDdlRequest request;
        BusinessObjectDataDdl resultDdl;
        String expectedDdl;

        // Retrieve business object data ddl when start partition value is less than the end partition value.
        request = getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, END_PARTITION_VALUE, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataRestController.generateBusinessObjectDataDdl(request);

        // Validate the results.
        expectedDdl = getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, PROCESS_DATE_AVAILABLE_PARTITION_VALUES,
            SUBPARTITION_VALUES, false, true, true);
        validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);

        // Retrieve business object data ddl when start partition value is equal to the end partition value.
        request = getTestBusinessObjectDataDdlRequest(START_PARTITION_VALUE, START_PARTITION_VALUE, CUSTOM_DDL_NAME);
        resultDdl = businessObjectDataRestController.generateBusinessObjectDataDdl(request);

        // Validate the results.
        expectedDdl = getExpectedDdl(PARTITION_COLUMNS.length, FIRST_COLUMN_NAME, FIRST_COLUMN_DATA_TYPE, ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
            FileTypeEntity.TXT_FILE_TYPE, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, Arrays.asList(START_PARTITION_VALUE), SUBPARTITION_VALUES,
            false, true, true);
        validateBusinessObjectDataDdl(request, expectedDdl, resultDdl);
    }
}
