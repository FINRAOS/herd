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
package org.finra.herd.service.activiti.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.FieldExtension;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.BusinessObjectDataSearchResultPagingInfoDto;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

public class SearchBusinessObjectDataTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testSearchBusinessObjectDataWithXML() throws Exception
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest = getBusinessObjectDataSearchRequest();
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataSearchRequest", "${businessObjectDataSearchRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataSearchRequest", xmlHelper.objectToXml(businessObjectDataSearchRequest)));
        parameters.add(buildParameter("pageNum", PAGE_NUMBER_ONE.toString()));
        parameters.add(buildParameter("pageSize", PAGE_SIZE_ONE_THOUSAND.toString()));

        // Get the expected result by calling the service method directly.
        BusinessObjectDataSearchResultPagingInfoDto result =
            this.businessObjectDataService.searchBusinessObjectData(PAGE_NUMBER_ONE, PAGE_SIZE_ONE_THOUSAND, businessObjectDataSearchRequest);

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(result.getBusinessObjectDataSearchResult()));

        testActivitiServiceTaskSuccess(SearchBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testSearchBusinessObjectDataWithJson() throws Exception
    {
        this.businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest = getBusinessObjectDataSearchRequest();
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataSearchRequest", "${businessObjectDataSearchRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataSearchRequest", jsonHelper.objectToJson(businessObjectDataSearchRequest)));
        parameters.add(buildParameter("pageNum", PAGE_NUMBER_ONE.toString()));
        parameters.add(buildParameter("pageSize", PAGE_SIZE_ONE_THOUSAND.toString()));

        // Get the expected result by calling the service method directly.
        BusinessObjectDataSearchResultPagingInfoDto result =
            this.businessObjectDataService.searchBusinessObjectData(PAGE_NUMBER_ONE, PAGE_SIZE_ONE_THOUSAND, businessObjectDataSearchRequest);

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(result.getBusinessObjectDataSearchResult()));

        testActivitiServiceTaskSuccess(SearchBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testSearchBusinessObjectDataWithBadParameter() throws Exception
    {
        this.businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest = getBusinessObjectDataSearchRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataSearchRequest", "${businessObjectDataSearchRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "bad_type"));
        parameters.add(buildParameter("businessObjectDataSearchRequest", jsonHelper.objectToJson(businessObjectDataSearchRequest)));

        String expectedBadFormatMessage = "\"ContentType\" must be a valid value of either \"xml\" or \"json\".";

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, expectedBadFormatMessage);

        testActivitiServiceTaskFailure(SearchBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    private BusinessObjectDataSearchRequest getBusinessObjectDataSearchRequest()
    {
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKeys.add(businessObjectDataSearchKey);
        BusinessObjectDataSearchFilter businessObjectDataSearchFilter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        businessObjectDataSearchFilters.add(businessObjectDataSearchFilter);
        businessObjectDataSearchRequest.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);

        return businessObjectDataSearchRequest;
    }
}
