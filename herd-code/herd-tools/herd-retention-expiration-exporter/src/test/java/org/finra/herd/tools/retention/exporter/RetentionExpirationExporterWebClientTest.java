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
package org.finra.herd.tools.retention.exporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;

import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.model.BusinessObjectDataSearchRequest;
import org.finra.herd.sdk.model.BusinessObjectDataSearchResult;
import org.finra.herd.sdk.model.BusinessObjectDefinition;

public class RetentionExpirationExporterWebClientTest extends AbstractExporterTest
{
    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        retentionExpirationExporterWebClient.getRegServerAccessParamsDto().setUseSsl(false);
        BusinessObjectDefinition result = retentionExpirationExporterWebClient.getBusinessObjectDefinition(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);
        assertNotNull(result);
    }

    @Test
    public void testGetBusinessObjectDefinitionException() throws Exception
    {
        try
        {
            retentionExpirationExporterWebClient.getBusinessObjectDefinition(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);
        }
        catch (ApiException e)
        {
            assertEquals("testThrowIoException", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionUseSsl() throws Exception
    {
        retentionExpirationExporterWebClient.getRegServerAccessParamsDto().setUseSsl(true);
        BusinessObjectDefinition result = retentionExpirationExporterWebClient.getBusinessObjectDefinition(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);
        assertNotNull(result);
    }

    @Test
    public void testSearchBusinessObjectDataException() throws Exception
    {
        try
        {
            retentionExpirationExporterWebClient.searchBusinessObjectData(new BusinessObjectDataSearchRequest(), 1, 2);
        }
        catch (ApiException e)
        {
            assertEquals("testThrowIoException", e.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDataPageNum1() throws Exception
    {
        retentionExpirationExporterWebClient.getRegServerAccessParamsDto().setUseSsl(false);
        BusinessObjectDataSearchResult result = retentionExpirationExporterWebClient.searchBusinessObjectData(new BusinessObjectDataSearchRequest(), 1, 2);
        assertNotNull(result);
        assertEquals(2, CollectionUtils.size(result.getBusinessObjectDataElements()));
    }

    @Test
    public void testSearchBusinessObjectDataPageNum2() throws Exception
    {
        retentionExpirationExporterWebClient.getRegServerAccessParamsDto().setUseSsl(false);
        BusinessObjectDataSearchResult result = retentionExpirationExporterWebClient.searchBusinessObjectData(new BusinessObjectDataSearchRequest(), 2, 2);
        assertNotNull(result);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataElements()));
    }

    @Test
    public void testSearchBusinessObjectDataUseSsl() throws Exception
    {
        retentionExpirationExporterWebClient.getRegServerAccessParamsDto().setUseSsl(true);
        BusinessObjectDataSearchResult result = retentionExpirationExporterWebClient.searchBusinessObjectData(new BusinessObjectDataSearchRequest(), 1, 2);
        assertNotNull(result);
    }
}
