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

import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.ExternalInterfaceDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

public class ExternalInterfaceDaoHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ExternalInterfaceDao externalInterfaceDao;

    @InjectMocks
    private ExternalInterfaceDaoHelper externalInterfaceDaoHelper = new ExternalInterfaceDaoHelper();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetExternalInterfaceEntity()
    {
        // Create an external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();

        // Mock the external calls.
        when(externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE)).thenReturn(externalInterfaceEntity);

        // Call the method under test.
        ExternalInterfaceEntity result = externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE);

        // Validate the results.
        assertEquals(externalInterfaceEntity, result);

        // Verify the external calls.
        verify(externalInterfaceDao).getExternalInterfaceByName(EXTERNAL_INTERFACE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetExternalInterfaceEntityExternalInterfaceNoExists()
    {
        // Mock the external calls.
        when(externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE)).thenReturn(null);

        // Specify the expected exception.
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage(String.format("External interface with name \"%s\" doesn't exist.", EXTERNAL_INTERFACE));

        // Call the method under test.
        externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE);

        // Verify the external calls.
        verify(externalInterfaceDao).getExternalInterfaceByName(EXTERNAL_INTERFACE);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(externalInterfaceDao);
    }
}
