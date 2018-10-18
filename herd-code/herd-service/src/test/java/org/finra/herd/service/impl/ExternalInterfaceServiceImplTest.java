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
package org.finra.herd.service.impl;

import static org.finra.herd.dao.AbstractDaoTest.CREATED_BY;
import static org.finra.herd.dao.AbstractDaoTest.CREATED_ON;
import static org.finra.herd.dao.AbstractDaoTest.DESCRIPTION;
import static org.finra.herd.dao.AbstractDaoTest.DISPLAY_NAME_FIELD;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE_2;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE_3;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.ExternalInterfaceDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.ExternalInterface;
import org.finra.herd.model.api.xml.ExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.ExternalInterfaceKey;
import org.finra.herd.model.api.xml.ExternalInterfaceKeys;
import org.finra.herd.model.api.xml.ExternalInterfaceUpdateRequest;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.ExternalInterfaceDaoHelper;

/**
 * This class tests functionality within the external interface service implementation.
 */
public class ExternalInterfaceServiceImplTest
{
    private static final String DISPLAY_NAME_FIELD_WITH_EXTRA_SPACES = DISPLAY_NAME_FIELD + "    ";

    private static final String EXTERNAL_INTERFACE_NAME_WITH_EXTRA_SPACES = EXTERNAL_INTERFACE + "    ";

    private static final ExternalInterfaceKey EXTERNAL_INTERFACE_KEY = new ExternalInterfaceKey()
    {{
        setExternalInterfaceName(EXTERNAL_INTERFACE);
    }};

    private static final ExternalInterfaceKey EXTERNAL_INTERFACE_KEY_WITH_EXTRA_SPACES_IN_NAME = new ExternalInterfaceKey()
    {{
        setExternalInterfaceName(EXTERNAL_INTERFACE_NAME_WITH_EXTRA_SPACES);
    }};

    private static final ExternalInterfaceCreateRequest EXTERNAL_INTERFACE_CREATE_REQUEST = new ExternalInterfaceCreateRequest()
    {{
        setExternalInterfaceKey(EXTERNAL_INTERFACE_KEY);
        setDisplayName(DISPLAY_NAME_FIELD);
    }};

    private static final ExternalInterfaceCreateRequest EXTERNAL_INTERFACE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME = new ExternalInterfaceCreateRequest()
    {{
        setExternalInterfaceKey(EXTERNAL_INTERFACE_KEY_WITH_EXTRA_SPACES_IN_NAME);
        setDisplayName(DISPLAY_NAME_FIELD);
    }};

    private static final ExternalInterfaceUpdateRequest EXTERNAL_INTERFACE_UPDATE_REQUEST = new ExternalInterfaceUpdateRequest()
    {{
        setDisplayName(DISPLAY_NAME_FIELD);
        setDescription(DESCRIPTION);
    }};

    private static final ExternalInterfaceUpdateRequest EXTERNAL_INTERFACE_UPDATE_REQUEST_WITH_EXTRA_SPACES_IN_DISPLAY_NAME =
        new ExternalInterfaceUpdateRequest()
        {{
            setDisplayName(DISPLAY_NAME_FIELD_WITH_EXTRA_SPACES);
            setDescription(DESCRIPTION);
        }};

    private static final ExternalInterfaceEntity EXTERNAL_INTERFACE_ENTITY = new ExternalInterfaceEntity()
    {{
        setCode(EXTERNAL_INTERFACE);
        setCreatedBy(CREATED_BY);
        setUpdatedBy(CREATED_BY);
        setCreatedOn(new Timestamp(CREATED_ON.getMillisecond()));
    }};

    private static final List<String> ALL_EXTERNAL_INTERFACE_NAMES = Arrays.asList(EXTERNAL_INTERFACE, EXTERNAL_INTERFACE_2, EXTERNAL_INTERFACE_3);

    @InjectMocks
    private ExternalInterfaceServiceImpl externalInterfaceService;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private ExternalInterfaceDao externalInterfaceDao;

    @Mock
    private ExternalInterfaceDaoHelper externalInterfaceDaoHelper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateExternalInterface()
    {
        when(externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE)).thenReturn(null);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString(), anyString())).thenReturn(EXTERNAL_INTERFACE);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(DISPLAY_NAME_FIELD);
        when(externalInterfaceDao.saveAndRefresh(any(ExternalInterfaceEntity.class))).thenReturn(EXTERNAL_INTERFACE_ENTITY);

        ExternalInterfaceKey externalInterfaceKey = new ExternalInterfaceKey(EXTERNAL_INTERFACE);
        ExternalInterfaceCreateRequest externalInterfaceCreateRequest =
            new ExternalInterfaceCreateRequest(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION);

        ExternalInterface externalInterface = externalInterfaceService.createExternalInterface(externalInterfaceCreateRequest);
        assertEquals(EXTERNAL_INTERFACE, externalInterface.getExternalInterfaceKey().getExternalInterfaceName());

        verify(alternateKeyHelper).validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE);
        verify(alternateKeyHelper).validateStringParameter("display name", DISPLAY_NAME_FIELD);
        verify(externalInterfaceDao).getExternalInterfaceByName(EXTERNAL_INTERFACE);
        verify(externalInterfaceDao).saveAndRefresh(any(ExternalInterfaceEntity.class));

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateExternalInterfaceAlreadyExists()
    {
        expectedException.expect(AlreadyExistsException.class);
        expectedException.expectMessage(String.format("Unable to create external interface \"%s\" because it already exists.", EXTERNAL_INTERFACE));

        when(externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE)).thenReturn(EXTERNAL_INTERFACE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString(), anyString())).thenReturn(EXTERNAL_INTERFACE);
        externalInterfaceService.createExternalInterface(EXTERNAL_INTERFACE_CREATE_REQUEST);
    }

    @Test
    public void testGetExternalInterface()
    {
        when(externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE)).thenReturn(EXTERNAL_INTERFACE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString(), anyString())).thenReturn(EXTERNAL_INTERFACE);

        ExternalInterface externalInterface = externalInterfaceService.getExternalInterface(EXTERNAL_INTERFACE_KEY);
        assertEquals(EXTERNAL_INTERFACE, externalInterface.getExternalInterfaceKey().getExternalInterfaceName());
        verify(alternateKeyHelper).validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE_KEY.getExternalInterfaceName());
        verify(externalInterfaceDaoHelper).getExternalInterfaceEntity(EXTERNAL_INTERFACE);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetExternalInterfaceNullKey()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("An external interface key must be specified");

        externalInterfaceService.getExternalInterface(null);
    }


    @Test
    public void testDeleteExternalInterface()
    {
        when(externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE)).thenReturn(EXTERNAL_INTERFACE_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString(), anyString())).thenReturn(EXTERNAL_INTERFACE);

        ExternalInterface externalInterface = externalInterfaceService.deleteExternalInterface(EXTERNAL_INTERFACE_KEY);
        assertEquals(EXTERNAL_INTERFACE, externalInterface.getExternalInterfaceKey().getExternalInterfaceName());
        verify(alternateKeyHelper).validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE);
        verify(externalInterfaceDaoHelper).getExternalInterfaceEntity(EXTERNAL_INTERFACE);
        verify(externalInterfaceDao).delete(EXTERNAL_INTERFACE_ENTITY);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteExternalInterfaceNullKey()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("An external interface key must be specified");

        externalInterfaceService.deleteExternalInterface(null);
    }

    @Test
    public void testGetExternalInterfaces()
    {
        when(externalInterfaceDao.getExternalInterfaces()).thenReturn(ALL_EXTERNAL_INTERFACE_NAMES);

        ExternalInterfaceKeys externalInterfaceKeys = externalInterfaceService.getExternalInterfaces();

        assertNotNull(externalInterfaceKeys);
        List<ExternalInterfaceKey> externalInterfaceKeyList = externalInterfaceKeys.getExternalInterfaceKeys();
        assertEquals(ALL_EXTERNAL_INTERFACE_NAMES.size(), externalInterfaceKeyList.size());

        // verify the order is reserved
        assertEquals(EXTERNAL_INTERFACE, externalInterfaceKeyList.get(0).getExternalInterfaceName());
        assertEquals(EXTERNAL_INTERFACE_2, externalInterfaceKeyList.get(1).getExternalInterfaceName());
        assertEquals(EXTERNAL_INTERFACE_3, externalInterfaceKeyList.get(2).getExternalInterfaceName());

        verify(externalInterfaceDao).getExternalInterfaces();

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetExternalInterfacesEmptyList()
    {
        when(externalInterfaceDao.getExternalInterfaces()).thenReturn(Collections.emptyList());
        ExternalInterfaceKeys externalInterfaceKeys = externalInterfaceService.getExternalInterfaces();

        assertNotNull(externalInterfaceKeys);
        assertEquals(0, externalInterfaceKeys.getExternalInterfaceKeys().size());

        verify(externalInterfaceDao).getExternalInterfaces();

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateExternalInterface()
    {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString(), anyString())).thenReturn(EXTERNAL_INTERFACE);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(DISPLAY_NAME_FIELD);
        when(externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE)).thenReturn(EXTERNAL_INTERFACE_ENTITY);
        when(externalInterfaceDao.saveAndRefresh(any(ExternalInterfaceEntity.class))).thenReturn(EXTERNAL_INTERFACE_ENTITY);

        ExternalInterface externalInterface = externalInterfaceService.updateExternalInterface(EXTERNAL_INTERFACE_KEY, EXTERNAL_INTERFACE_UPDATE_REQUEST);
        assertEquals(DISPLAY_NAME_FIELD, externalInterface.getDisplayName());

        verify(alternateKeyHelper).validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE_KEY.getExternalInterfaceName());
        verify(alternateKeyHelper).validateStringParameter("display name", DISPLAY_NAME_FIELD);
        verify(externalInterfaceDaoHelper).getExternalInterfaceEntity(EXTERNAL_INTERFACE);
        verify(externalInterfaceDao).saveAndRefresh(any(ExternalInterfaceEntity.class));

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateExternalInterfaceCreateRequestExtraSpaces()
    {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString(), anyString())).thenReturn(EXTERNAL_INTERFACE);

        assertEquals(EXTERNAL_INTERFACE_NAME_WITH_EXTRA_SPACES,
            EXTERNAL_INTERFACE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME.getExternalInterfaceKey().getExternalInterfaceName());

        externalInterfaceService.validateExternalInterfaceCreateRequest(EXTERNAL_INTERFACE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME);

        // White space should be trimmed now
        assertEquals(EXTERNAL_INTERFACE, EXTERNAL_INTERFACE_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME.getExternalInterfaceKey().getExternalInterfaceName());
    }

    @Test
    public void testValidateAndTrimExternalInterfaceKeyExtraSpaces()
    {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString(), anyString())).thenReturn(EXTERNAL_INTERFACE);

        ExternalInterfaceKey externalInterfaceKeyWithExtraSpacesInName = new ExternalInterfaceKey(EXTERNAL_INTERFACE_NAME_WITH_EXTRA_SPACES);

        assertEquals(EXTERNAL_INTERFACE_NAME_WITH_EXTRA_SPACES, externalInterfaceKeyWithExtraSpacesInName.getExternalInterfaceName());

        externalInterfaceService.validateAndTrimExternalInterfaceKey(externalInterfaceKeyWithExtraSpacesInName);

        // White space should be trimmed now
        assertEquals(EXTERNAL_INTERFACE, externalInterfaceKeyWithExtraSpacesInName.getExternalInterfaceName());
    }

    @Test
    public void testValidateExternalInterfaceUpdateRequestExtraSpaces()
    {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(DISPLAY_NAME_FIELD);

        assertEquals(DISPLAY_NAME_FIELD_WITH_EXTRA_SPACES, EXTERNAL_INTERFACE_UPDATE_REQUEST_WITH_EXTRA_SPACES_IN_DISPLAY_NAME.getDisplayName());

        externalInterfaceService.validateExternalInterfaceUpdateRequest(EXTERNAL_INTERFACE_UPDATE_REQUEST_WITH_EXTRA_SPACES_IN_DISPLAY_NAME);

        // White space should be trimmed now
        assertEquals(DISPLAY_NAME_FIELD, EXTERNAL_INTERFACE_UPDATE_REQUEST_WITH_EXTRA_SPACES_IN_DISPLAY_NAME.getDisplayName());
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(externalInterfaceDaoHelper, alternateKeyHelper, externalInterfaceDao);
    }
}
