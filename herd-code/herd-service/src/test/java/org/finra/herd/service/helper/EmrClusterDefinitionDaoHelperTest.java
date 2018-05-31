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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.EmrClusterDefinitionDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the EMR cluster definition DAO helper.
 */
public class EmrClusterDefinitionDaoHelperTest extends AbstractServiceTest
{
    @Mock
    private EmrClusterDefinitionDao emrClusterDefinitionDao;

    @InjectMocks
    private EmrClusterDefinitionDaoHelper emrClusterDefinitionDaoHelper;

    @Mock
    private NamespaceDao namespaceDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetEmrClusterDefinitionEntity()
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();

        // Create an EMR cluster definition entity.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = new EmrClusterDefinitionEntity();

        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Mock the external calls.
        when(namespaceDao.getNamespaceByCd(NAMESPACE)).thenReturn(namespaceEntity);
        when(emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME))
            .thenReturn(emrClusterDefinitionEntity);

        // Call the method under test.
        EmrClusterDefinitionEntity result = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);

        // Verify the external calls.
        verify(namespaceDao).getNamespaceByCd(NAMESPACE);
        verify(emrClusterDefinitionDao).getEmrClusterDefinitionByNamespaceAndName(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(emrClusterDefinitionEntity, result);
    }

    @Test
    public void testGetEmrClusterDefinitionEntityNamespaceNoExists()
    {
        // Create an EMR cluster definition key.
        EmrClusterDefinitionKey emrClusterDefinitionKey = new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME);

        // Mock the external calls.
        when(namespaceDao.getNamespaceByCd(NAMESPACE)).thenReturn(null);

        // Try to call the method under test.
        try
        {
            emrClusterDefinitionDaoHelper.getEmrClusterDefinitionEntity(emrClusterDefinitionKey);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("EMR cluster definition with name \"%s\" doesn't exist for namespace \"%s\".", EMR_CLUSTER_DEFINITION_NAME, NAMESPACE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(namespaceDao).getNamespaceByCd(NAMESPACE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetEmrClusterDefinitionKeys()
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();

        // Create a list of EMR cluster definition keys.
        List<EmrClusterDefinitionKey> emrClusterDefinitionKeys = Lists.newArrayList(new EmrClusterDefinitionKey(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME));

        // Mock the external calls.
        when(namespaceDao.getNamespaceByCd(NAMESPACE)).thenReturn(namespaceEntity);
        when(emrClusterDefinitionDao.getEmrClusterDefinitionKeysByNamespace(namespaceEntity)).thenReturn(emrClusterDefinitionKeys);

        // Call the method under test.
        List<EmrClusterDefinitionKey> result = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionKeys(NAMESPACE);

        // Verify the external calls.
        verify(namespaceDao).getNamespaceByCd(NAMESPACE);
        verify(emrClusterDefinitionDao).getEmrClusterDefinitionKeysByNamespace(namespaceEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(emrClusterDefinitionKeys, result);
    }

    @Test
    public void testGetEmrClusterDefinitionKeysNamespaceNoExists()
    {
        // Mock the external calls.
        when(namespaceDao.getNamespaceByCd(NAMESPACE)).thenReturn(null);

        // Call the method under test.
        List<EmrClusterDefinitionKey> result = emrClusterDefinitionDaoHelper.getEmrClusterDefinitionKeys(NAMESPACE);

        // Verify the external calls.
        verify(namespaceDao).getNamespaceByCd(NAMESPACE);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(emrClusterDefinitionDao, namespaceDao);
    }
}
