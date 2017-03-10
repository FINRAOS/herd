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
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;


public class SearchBusinessObjectDataTest extends HerdActivitiServiceTaskTest
{
    /**
     * This method tests the search business object data task with xml format
     */
    @Test
    public void testSearchBusinessObjectDataWithXML() throws Exception
    {

        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();
        BusinessObjectDataSearchRequest searchRequest = getBusinessObjectDataSearchRequest();
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataSearchRequest", "${businessObjectDataSearchRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataSearchRequest", xmlHelper.objectToXml(searchRequest)));

        //the actual result from calling the service directly
        BusinessObjectDataSearchResult result = this.businessObjectDataService.searchBusinessObjectData(searchRequest);

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(result));

        testActivitiServiceTaskSuccess(SearchBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);

    }


    /**
     * This method tests the search business object data task with json format
     */
    @Test
    public void testSearchBusinessObjectDataWithJson() throws Exception
    {
        this.businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();
        BusinessObjectDataSearchRequest searchRequest = getBusinessObjectDataSearchRequest();
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataSearchRequest", "${businessObjectDataSearchRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataSearchRequest", jsonHelper.objectToJson(searchRequest)));

        //the actual result from calling the service directly
        BusinessObjectDataSearchResult result = this.businessObjectDataService.searchBusinessObjectData(searchRequest);

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(result));

        testActivitiServiceTaskSuccess(SearchBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }


    @Test
    public void testSearchBusinessObjectDataWithBadParameter() throws Exception
    {
        this.businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();
        BusinessObjectDataSearchRequest searchRequest = getBusinessObjectDataSearchRequest() ;

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataSearchRequest", "${businessObjectDataSearchRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "bad_type"));
        parameters.add(buildParameter("businessObjectDataSearchRequest", jsonHelper.objectToJson(searchRequest)));

        String expectedBadFormatMessage = "\"ContentType\" must be a valid value of either \"xml\" or \"json\".";

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, expectedBadFormatMessage);

        testActivitiServiceTaskFailure(SearchBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    private  BusinessObjectDataSearchRequest getBusinessObjectDataSearchRequest()
    {
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        key.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        return request;
    }

}

