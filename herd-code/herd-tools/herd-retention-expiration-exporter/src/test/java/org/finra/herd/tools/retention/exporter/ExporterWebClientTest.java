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

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.xml.bind.JAXBException;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;

public class ExporterWebClientTest extends AbstractExporterTest
{
    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        runGetBusinessObjectDefinitionTest(false);
    }

    @Test
    public void testGetBusinessObjectDefinitionUseSsl() throws Exception
    {
        runGetBusinessObjectDefinitionTest(true);
    }

    @Test
    public void testSearchBusinessObjectData() throws Exception
    {
        runSearchBusinessObjectDataTest(false);
    }

    @Test
    public void testSearchBusinessObjectDataUseSsl() throws Exception
    {
        runSearchBusinessObjectDataTest(true);
    }

    /**
     * Calls getBusinessObjectDefinition() method and makes assertions.
     *
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws java.io.IOException
     * @throws javax.xml.bind.JAXBException
     * @throws java.net.URISyntaxException
     */
    private void runGetBusinessObjectDefinitionTest(boolean useSsl) throws IOException, JAXBException, URISyntaxException
    {
        exporterWebClient.getRegServerAccessParamsDto().setUseSsl(useSsl);
        BusinessObjectDefinition result = exporterWebClient.getBusinessObjectDefinition(NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);
        assertNotNull(result);
    }

    /**
     * Calls getBusinessObjectDefinition() method and makes assertions.
     *
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws java.io.IOException
     * @throws javax.xml.bind.JAXBException
     * @throws java.net.URISyntaxException
     */
    private void runSearchBusinessObjectDataTest(boolean useSsl) throws IOException, JAXBException, URISyntaxException
    {
        exporterWebClient.getRegServerAccessParamsDto().setUseSsl(useSsl);
        BusinessObjectDataSearchResult result = exporterWebClient.searchBusinessObjectData(new BusinessObjectDataSearchRequest(), 1);
        assertNotNull(result);
    }
}
