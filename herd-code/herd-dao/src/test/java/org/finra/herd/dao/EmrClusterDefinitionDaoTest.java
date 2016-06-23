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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

public class EmrClusterDefinitionDaoTest extends AbstractDaoTest
{
    /**
     * Tests the happy path scenario by providing all the parameters.
     */
    @Test
    public void testGetEmrClusterDefinitionByAltKey() throws IOException
    {
        // Create and persist an EMR cluster definition entity.
        createEmrClusterDefinitionEntity(createNamespaceEntity(NAMESPACE), EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Call the API to query the newly added entity by providing the app and EMT cluster definition details
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult =
            emrClusterDefinitionDao.getEmrClusterDefinitionByAltKey(new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME));

        // Fail if there is any problem in the result
        assertNotNull(emrClusterDefinitionEntityResult);
        assertEquals(NAMESPACE, emrClusterDefinitionEntityResult.getNamespace().getCode());
        assertEquals(EMR_CLUSTER_DEFINITION_NAME, emrClusterDefinitionEntityResult.getName());
    }

    /**
     * Tests the scenario by providing a EMR Cluster name that doesn't exist.
     */
    @Test
    public void testGetEmrClusterDefinitionByAltKeyDefinitionNameNoExists() throws IOException
    {
        // Create and persist an EMR Cluster definition entity.
        createEmrClusterDefinitionEntity(createNamespaceEntity(NAMESPACE), EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Call the API to query the newly added entity by providing the app and a definition name that doesn't exist.
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult =
            emrClusterDefinitionDao.getEmrClusterDefinitionByAltKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME_2);

        // Validate the results.
        assertNull(emrClusterDefinitionEntityResult);
    }

    /**
     * Tests the scenario by providing the wrong app name.
     */
    @Test
    public void testGetEmrClusterDefinitionByAltKeyNamespaceNoExists() throws IOException
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE);
        createNamespaceEntity(NAMESPACE_2);

        // Create a EMR Cluster definition entity
        createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Call the API to query the newly added entity by providing the app and a definition name that doesn't exist.
        EmrClusterDefinitionEntity emrClusterDefinitionEntityResult =
            emrClusterDefinitionDao.getEmrClusterDefinitionByAltKey(NAMESPACE_2, EMR_CLUSTER_DEFINITION_NAME_2);

        // Validate the results.
        assertNull(emrClusterDefinitionEntityResult);
    }

    /**
     * Tests the scenario by finding multiple EMR Cluster definition records.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetEmrClusterDefinitionByAltKeyMultipleRecordsFound() throws IOException
    {
        // Create namespace database entities.
        NamespaceEntity namespaceEntity = createNamespaceEntity(NAMESPACE);

        // Create two EMR cluster definitions different.
        for (String definitionName : Arrays.asList(EMR_CLUSTER_DEFINITION_NAME.toUpperCase(), EMR_CLUSTER_DEFINITION_NAME.toLowerCase()))
        {
            // Create a EMR cluster definition entity.
            createEmrClusterDefinitionEntity(namespaceEntity, definitionName,
                IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));
        }

        // Try to retrieve the the job definition.
        emrClusterDefinitionDao.getEmrClusterDefinitionByAltKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);
    }

    @Test
    public void testGetEmrClusterDefinitionsByNamespace() throws IOException
    {
        // Create and persist an EMR cluster definition entity.
        createEmrClusterDefinitionEntity(createNamespaceEntity(NAMESPACE), EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Retrieve a list of EMR cluster definition keys.
        assertEquals(Arrays.asList(emrClusterDefinitionKey), emrClusterDefinitionDao.getEmrClusterDefinitionsByNamespace(NAMESPACE));

        // Get business object data attribute by passing namespace parameter value in uppercase.
        assertEquals(Arrays.asList(emrClusterDefinitionKey), emrClusterDefinitionDao.getEmrClusterDefinitionsByNamespace(NAMESPACE.toUpperCase()));

        // Get business object data attribute by passing namespace parameter value in lowercase.
        assertEquals(Arrays.asList(emrClusterDefinitionKey), emrClusterDefinitionDao.getEmrClusterDefinitionsByNamespace(NAMESPACE.toLowerCase()));

        // Try an invalid value for the namespace input parameter.
        assertTrue(emrClusterDefinitionDao.getEmrClusterDefinitionsByNamespace("I_DO_NO_EXIST").isEmpty());
    }
}
