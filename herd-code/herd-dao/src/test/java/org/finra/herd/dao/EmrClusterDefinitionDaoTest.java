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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

public class EmrClusterDefinitionDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetEmrClusterDefinitionByNamespaceAndName() throws IOException
    {
        // Create two namespace entities.
        List<NamespaceEntity> namespaceEntities =
            Arrays.asList(namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE), namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2));

        // Create an EMR cluster definition entity for the first namespace.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDaoTestHelper
            .createEmrClusterDefinitionEntity(namespaceEntities.get(0), EMR_CLUSTER_DEFINITION_NAME,
                IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream(), StandardCharsets.UTF_8));

        // Retrieve the relative EMR cluster definition entity.
        assertEquals(emrClusterDefinitionEntity,
            emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntities.get(0), EMR_CLUSTER_DEFINITION_NAME));

        // Test case insensitivity of EMR cluster definition name.
        assertEquals(emrClusterDefinitionEntity,
            emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntities.get(0), EMR_CLUSTER_DEFINITION_NAME.toUpperCase()));
        assertEquals(emrClusterDefinitionEntity,
            emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntities.get(0), EMR_CLUSTER_DEFINITION_NAME.toLowerCase()));

        // Confirm that no EMR cluster definition gets selected when using wrong input parameters.
        assertNull(emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntities.get(1), EMR_CLUSTER_DEFINITION_NAME));
        assertNull(emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntities.get(0), I_DO_NOT_EXIST));
    }

    @Test
    public void testGetEmrClusterDefinitionByNamespaceAndNameMultipleRecordsFound() throws IOException
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create two EMR cluster definitions with the same EMR cluster definition name, but with different capitalization/case.
        for (String emrClusterDefinitionName : Arrays.asList(EMR_CLUSTER_DEFINITION_NAME.toUpperCase(), EMR_CLUSTER_DEFINITION_NAME.toLowerCase()))
        {
            emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, emrClusterDefinitionName,
                IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream(), StandardCharsets.UTF_8));
        }

        // Try to retrieve EMR cluster definition by namespace entity and EMR cluster definition name.
        try
        {
            emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Found more than one EMR cluster definition with parameters {namespace=\"%s\", emrClusterDefinitionName=\"%s\"}.", NAMESPACE,
                    EMR_CLUSTER_DEFINITION_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetEmrClusterDefinitionKeysByNamespace() throws IOException
    {
        // Create two namespace entities.
        List<NamespaceEntity> namespaceEntities =
            Arrays.asList(namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE), namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2));

        // Create two EMR cluster definitions for the first namespace with EMR cluster definition names sorted in descending order.
        for (String emrClusterDefinitionName : Lists.newArrayList(EMR_CLUSTER_DEFINITION_NAME_2, EMR_CLUSTER_DEFINITION_NAME))
        {
            emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntities.get(0), emrClusterDefinitionName,
                IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream(), StandardCharsets.UTF_8));
        }

        // Retrieve a list of EMR cluster definition keys.
        assertEquals(Lists.newArrayList(new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME),
            new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME_2)),
            emrClusterDefinitionDao.getEmrClusterDefinitionKeysByNamespace(namespaceEntities.get(0)));

        // Confirm that no EMR cluster definition keys get selected when passing wrong namespace entity.
        assertTrue(emrClusterDefinitionDao.getEmrClusterDefinitionKeysByNamespace(namespaceEntities.get(1)).isEmpty());
    }
}
