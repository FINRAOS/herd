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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.security.access.AccessDeniedException;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.NamespaceIamRoleAuthorizationDao;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;
import org.finra.herd.service.AbstractServiceTest;

public class NamespaceIamRoleAuthorizationHelperTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @Mock
    private NamespaceIamRoleAuthorizationDao namespaceIamRoleAuthorizationDao;

    @InjectMocks
    private NamespaceIamRoleAuthorizationHelper namespaceIamRoleAuthorizationHelper;

    @Before
    public void before()
    {
        initMocks(this);
    }

    @Test
    public void checkPermissionsAssertNoErrorWhenNamespaceAuthorizedToAllRoles()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        String iamRoleName1 = "iamRoleName1";
        String iamRoleName2 = "iamRoleName2";
        Collection<String> requestedIamRoleNames = Arrays.asList(iamRoleName1, iamRoleName2);

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setIamRoleName(iamRoleName1);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setIamRoleName(iamRoleName2);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, requestedIamRoleNames);

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void checkPermissionsAssertAccessDeniedWhenNamespaceNotAuthorizedToOneRole()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("namespace");
        String iamRoleName1 = "iamRoleName1";
        String iamRoleName2 = "iamRoleName2";
        Collection<String> requestedIamRoleNames = Arrays.asList(iamRoleName1, iamRoleName2);

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setIamRoleName(iamRoleName1);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        try
        {
            namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, requestedIamRoleNames);
            fail();
        }
        catch (AccessDeniedException e)
        {
            assertEquals("The namespace \"namespace\" does not have access to the following IAM roles: [iamRoleName2]", e.getMessage());
        }

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void checkPermissionsAssertAccessDeniedWhenNamespaceNotAuthorizedToAllRole()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("namespace");
        String iamRoleName1 = "iamRoleName1";
        String iamRoleName2 = "iamRoleName2";
        Collection<String> requestedIamRoleNames = Arrays.asList(iamRoleName1, iamRoleName2);

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        try
        {
            namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, requestedIamRoleNames);
            fail();
        }
        catch (AccessDeniedException e)
        {
            assertEquals("The namespace \"namespace\" does not have access to the following IAM roles: [iamRoleName1, iamRoleName2]", e.getMessage());
        }

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void checkPermissionsAssertBlankRequestRoleIgnored()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        String iamRoleName1 = BLANK_TEXT;
        String iamRoleName2 = "iamRoleName2";
        Collection<String> requestedIamRoleNames = Arrays.asList(iamRoleName1, iamRoleName2);

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setIamRoleName(iamRoleName2);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, requestedIamRoleNames);

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void checkPermissionsAssertDoNothingWhenAuthorizationDisabled()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        Collection<String> requestedIamRoleNames = new ArrayList<>();

        when(configurationHelper.getBooleanProperty(any())).thenReturn(false);

        namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, requestedIamRoleNames);

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void checkPermissionsWithArrayAssertNoErrorWhenNamespaceAuthorizedToAllRoles()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        String iamRoleName1 = "iamRoleName1";
        String iamRoleName2 = "iamRoleName2";

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setIamRoleName(iamRoleName1);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setIamRoleName(iamRoleName2);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, iamRoleName1, iamRoleName2);

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void checkPermissionsAssertRoleNameIsTrimmed()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        String iamRoleName1 = "iamRoleName1";
        String iamRoleName2 = "iamRoleName2";
        Collection<String> requestedIamRoleNames = Arrays.asList(StringUtils.wrap(iamRoleName1, BLANK_TEXT), StringUtils.wrap(iamRoleName2, BLANK_TEXT));

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setIamRoleName(iamRoleName1);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setIamRoleName(iamRoleName2);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, requestedIamRoleNames);

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void checkPermissionsAssertRoleNameIsCaseInsensitive()
    {
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        String iamRoleName1 = "iamRoleName1";
        String iamRoleName2 = "iamRoleName2";
        Collection<String> requestedIamRoleNames = Arrays.asList(StringUtils.capitalize(iamRoleName1), StringUtils.capitalize(iamRoleName2));

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setIamRoleName(iamRoleName1);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setIamRoleName(iamRoleName2);
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(configurationHelper.getBooleanProperty(any())).thenReturn(true);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        namespaceIamRoleAuthorizationHelper.checkPermissions(expectedNamespaceEntity, requestedIamRoleNames);

        verify(configurationHelper).getBooleanProperty(ConfigurationValue.NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(configurationHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void testCreateNamespaceIamRoleAuthorizationEntity()
    {
        // Create the namespace IAM role authorization key.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);

        // Create the expected namespace entity.
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode(NAMESPACE);

        // Mock the external calls.
        when(namespaceDaoHelper.getNamespaceEntity(NAMESPACE)).thenReturn(expectedNamespaceEntity);

        // Call the method under test.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            namespaceIamRoleAuthorizationHelper.createNamespaceIamRoleAuthorizationEntity(namespaceIamRoleAuthorizationKey, IAM_ROLE_DESCRIPTION);

        // Validate the results.
        assertEquals(NAMESPACE, namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
        assertEquals(IAM_ROLE_NAME, namespaceIamRoleAuthorizationEntity.getIamRoleName());
        assertEquals(IAM_ROLE_DESCRIPTION, namespaceIamRoleAuthorizationEntity.getDescription());

        // Verify the external calls.
        verify(namespaceDaoHelper).getNamespaceEntity(NAMESPACE);
        verifyNoMoreInteractions(namespaceDaoHelper);
    }

    @Test
    public void testCreateNamespaceIamRoleAuthorizationEntityWithBlankIamRoleDescription()
    {
        // Create the namespace IAM role authorization key.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);

        // Create the expected namespace entity.
        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode(NAMESPACE);

        // Mock the external calls.
        when(namespaceDaoHelper.getNamespaceEntity(NAMESPACE)).thenReturn(expectedNamespaceEntity);

        // Call the method under test.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            namespaceIamRoleAuthorizationHelper.createNamespaceIamRoleAuthorizationEntity(namespaceIamRoleAuthorizationKey, BLANK_TEXT);

        // Validate the results.
        assertEquals(NAMESPACE, namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
        assertEquals(IAM_ROLE_NAME, namespaceIamRoleAuthorizationEntity.getIamRoleName());
        assertNull(namespaceIamRoleAuthorizationEntity.getDescription());

        // Verify the external calls.
        verify(namespaceDaoHelper).getNamespaceEntity(NAMESPACE);
        verifyNoMoreInteractions(namespaceDaoHelper);
    }

    @Test
    public void testValidateAndTrimNamespaceIamRoleAuthorizationKey()
    {
        // Create a namespace IAM role authorization key.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE_2);
        when(alternateKeyHelper.validateStringParameter("An", "IAM role name", IAM_ROLE_NAME)).thenReturn(IAM_ROLE_NAME_2);

        // Call the method under test.
        namespaceIamRoleAuthorizationHelper.validateAndTrimNamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationKey);

        // Validate the results.
        assertEquals(new NamespaceIamRoleAuthorizationKey(NAMESPACE_2, IAM_ROLE_NAME_2), namespaceIamRoleAuthorizationKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("An", "IAM role name", IAM_ROLE_NAME);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimNamespaceIamRoleAuthorizationKeyMissingSecurityFunctionKey()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A namespace IAM role authorization key must be specified.");

        // Call the method under test.
        namespaceIamRoleAuthorizationHelper.validateAndTrimNamespaceIamRoleAuthorizationKey(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
