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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.finra.herd.dao.NamespaceIamRoleAuthorizationDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.IamRole;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorization;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizations;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.impl.NamespaceIamRoleAuthorizationServiceImpl;

public class NamespaceIamRoleAuthorizationServiceTest
{
    @InjectMocks
    private NamespaceIamRoleAuthorizationServiceImpl namespaceIamRoleAuthorizationServiceImpl;

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @Mock
    private NamespaceIamRoleAuthorizationDao namespaceIamRoleAuthorizationDao;

    @Before
    public void before()
    {
        initMocks(this);
    }

    @Test
    public void createNamespaceIamRoleAuthorizationAssertDependenciesCalledAndResultExpected()
    {
        IamRole expectedIamRole1 = new IamRole("iamRoleName1", "iamRoleDescription1");
        IamRole expectedIamRole2 = new IamRole("iamRoleName2", " ");
        List<IamRole> expectedIamRoles = Arrays.asList(expectedIamRole1, expectedIamRole2);
        NamespaceIamRoleAuthorizationCreateRequest expectedRequest = new NamespaceIamRoleAuthorizationCreateRequest("namespace", expectedIamRoles);

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(Collections.emptyList());

        NamespaceIamRoleAuthorization response = namespaceIamRoleAuthorizationServiceImpl.createNamespaceIamRoleAuthorization(expectedRequest);

        assertNotNull(response);
        assertEquals(expectedNamespaceEntity.getCode(), response.getNamespace());
        assertNotNull(response.getIamRoles());
        assertEquals(expectedIamRoles.size(), response.getIamRoles().size());
        {
            IamRole iamRole = response.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(expectedIamRole1.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(expectedIamRole1.getIamRoleDescription(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = response.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(expectedIamRole2.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(null, iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedRequest.getNamespace());
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verify(namespaceIamRoleAuthorizationDao).saveAndRefresh(
            namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), expectedIamRole1.getIamRoleName(),
                expectedIamRole1.getIamRoleDescription()));
        verify(namespaceIamRoleAuthorizationDao)
            .saveAndRefresh(namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), expectedIamRole2.getIamRoleName(), null));
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void createNamespaceIamRoleAuthorizationAssertErrorWhenAuthorizationAlreadyExist()
    {
        IamRole expectedIamRole1 = new IamRole("iamRoleName1", "iamRoleDescription1");
        IamRole expectedIamRole2 = new IamRole("iamRoleName2", "iamRoleDescription2");
        List<IamRole> expectedIamRoles = Arrays.asList(expectedIamRole1, expectedIamRole2);
        NamespaceIamRoleAuthorizationCreateRequest expectedRequest = new NamespaceIamRoleAuthorizationCreateRequest("namespace", expectedIamRoles);

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(Arrays.asList(new NamespaceIamRoleAuthorizationEntity()));

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.createNamespaceIamRoleAuthorization(expectedRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AlreadyExistsException.class, e.getClass());
            assertEquals(String.format("Namespace IAM role authorizations with namespace \"%s\" already exist", expectedNamespaceEntity.getCode()),
                e.getMessage());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedRequest.getNamespace());
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void createNamespaceIamRoleAuthorizationAssertErrorWhenRoleNameIsBlank()
    {
        IamRole expectedIamRole1 = new IamRole("iamRoleName1", "iamRoleDescription1");
        IamRole expectedIamRole2 = new IamRole(" ", "iamRoleDescription2");
        List<IamRole> expectedIamRoles = Arrays.asList(expectedIamRole1, expectedIamRole2);
        NamespaceIamRoleAuthorizationCreateRequest expectedRequest = new NamespaceIamRoleAuthorizationCreateRequest("namespace", expectedIamRoles);

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.createNamespaceIamRoleAuthorization(expectedRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("IAM role name must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void createNamespaceIamRoleAuthorizationAssertErrorWhenIamRolesEmpty()
    {
        List<IamRole> expectedIamRoles = Arrays.asList();
        NamespaceIamRoleAuthorizationCreateRequest expectedRequest = new NamespaceIamRoleAuthorizationCreateRequest("namespace", expectedIamRoles);

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.createNamespaceIamRoleAuthorization(expectedRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("At least 1 IAM roles must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void createNamespaceIamRoleAuthorizationAssertErrorWhenNamespaceIsBlank()
    {
        IamRole expectedIamRole1 = new IamRole("iamRoleName1", "iamRoleDescription1");
        IamRole expectedIamRole2 = new IamRole("iamRoleName2", " ");
        List<IamRole> expectedIamRoles = Arrays.asList(expectedIamRole1, expectedIamRole2);
        NamespaceIamRoleAuthorizationCreateRequest expectedRequest = new NamespaceIamRoleAuthorizationCreateRequest(" ", expectedIamRoles);

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.createNamespaceIamRoleAuthorization(expectedRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Namespace must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void createNamespaceIamRoleAuthorizationAssertInputsTrimmed()
    {
        IamRole expectedIamRole1 = new IamRole(" iamRoleName1 ", " iamRoleDescription1 ");
        IamRole expectedIamRole2 = new IamRole(" iamRoleName2 ", " ");
        List<IamRole> expectedIamRoles = Arrays.asList(expectedIamRole1, expectedIamRole2);
        NamespaceIamRoleAuthorizationCreateRequest expectedRequest = new NamespaceIamRoleAuthorizationCreateRequest(" namespace ", expectedIamRoles);

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(Collections.emptyList());

        NamespaceIamRoleAuthorization response = namespaceIamRoleAuthorizationServiceImpl.createNamespaceIamRoleAuthorization(expectedRequest);

        assertNotNull(response);
        assertEquals(expectedNamespaceEntity.getCode(), response.getNamespace());
        assertNotNull(response.getIamRoles());
        assertEquals(expectedIamRoles.size(), response.getIamRoles().size());
        {
            IamRole iamRole = response.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(expectedIamRole1.getIamRoleName().trim(), iamRole.getIamRoleName());
            assertEquals(expectedIamRole1.getIamRoleDescription().trim(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = response.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(expectedIamRole2.getIamRoleName().trim(), iamRole.getIamRoleName());
            assertEquals(null, iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedRequest.getNamespace().trim());
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verify(namespaceIamRoleAuthorizationDao).saveAndRefresh(
            namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), expectedIamRole1.getIamRoleName().trim(),
                expectedIamRole1.getIamRoleDescription().trim()));
        verify(namespaceIamRoleAuthorizationDao)
            .saveAndRefresh(namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), expectedIamRole2.getIamRoleName().trim(), null));
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void getNamespaceIamRoleAuthorizationAssertDependenciesCalledAndResultExpected()
    {
        String expectedNamespace = "namespace";

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("iamRoleName2");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        NamespaceIamRoleAuthorization namespaceIamRoleAuthorization =
            namespaceIamRoleAuthorizationServiceImpl.getNamespaceIamRoleAuthorization(expectedNamespace);

        assertNotNull(namespaceIamRoleAuthorization);
        assertEquals(expectedNamespaceEntity.getCode(), namespaceIamRoleAuthorization.getNamespace());
        assertNotNull(namespaceIamRoleAuthorization.getIamRoles());
        assertEquals(2, namespaceIamRoleAuthorization.getIamRoles().size());
        {
            IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity2.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity2.getDescription(), iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void getNamespaceIamRoleAuthorizationAssertInputsTrimmed()
    {
        String expectedNamespace = " namespace ";

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("iamRoleName2");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        NamespaceIamRoleAuthorization namespaceIamRoleAuthorization =
            namespaceIamRoleAuthorizationServiceImpl.getNamespaceIamRoleAuthorization(expectedNamespace);

        assertNotNull(namespaceIamRoleAuthorization);
        assertEquals(expectedNamespaceEntity.getCode(), namespaceIamRoleAuthorization.getNamespace());
        assertNotNull(namespaceIamRoleAuthorization.getIamRoles());
        assertEquals(2, namespaceIamRoleAuthorization.getIamRoles().size());
        {
            IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity2.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity2.getDescription(), iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace.trim());
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void getNamespaceIamRoleAuthorizationAssertErrorWhenNoEntitiesFound()
    {
        String expectedNamespace = "namespace";

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = Collections.emptyList();

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.getNamespaceIamRoleAuthorization(expectedNamespace);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(ObjectNotFoundException.class, e.getClass());
            assertEquals(String.format("Namespace IAM role authorizations for namespace \"%s\" do not exist", expectedNamespaceEntity.getCode()),
                e.getMessage());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void getNamespaceIamRoleAuthorizationAssertErrorWhenNamespaceIsBlank()
    {
        String expectedNamespace = " ";

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.getNamespaceIamRoleAuthorization(expectedNamespace);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Namespace must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void getNamespaceIamRoleAuthorizationsAssertCallsDependenciesAndResultExpected()
    {
        NamespaceEntity namespaceEntity1 = new NamespaceEntity();
        namespaceEntity1.setCode("namespace1");

        NamespaceEntity namespaceEntity2 = new NamespaceEntity();
        namespaceEntity2.setCode("namespace2");

        List<NamespaceIamRoleAuthorizationEntity> expectedNamespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("iamRoleName2");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity3 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity3.setNamespace(namespaceEntity2);
        namespaceIamRoleAuthorizationEntity3.setIamRoleName("iamRoleName3");
        namespaceIamRoleAuthorizationEntity3.setDescription("description3");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity3);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity4 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity4.setNamespace(namespaceEntity2);
        namespaceIamRoleAuthorizationEntity4.setIamRoleName("iamRoleName4");
        namespaceIamRoleAuthorizationEntity4.setDescription("description4");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity4);

        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(expectedNamespaceIamRoleAuthorizationEntities);

        NamespaceIamRoleAuthorizations result = namespaceIamRoleAuthorizationServiceImpl.getNamespaceIamRoleAuthorizations();

        assertNotNull(result);
        assertNotNull(result.getNamespaceIamRoleAuthorizations());
        assertEquals(2, result.getNamespaceIamRoleAuthorizations().size());
        {
            NamespaceIamRoleAuthorization namespaceIamRoleAuthorization = result.getNamespaceIamRoleAuthorizations().get(0);
            assertNotNull(namespaceIamRoleAuthorization);
            assertEquals(namespaceEntity1.getCode(), namespaceIamRoleAuthorization.getNamespace());
            assertNotNull(namespaceIamRoleAuthorization.getIamRoles());
            assertEquals(2, namespaceIamRoleAuthorization.getIamRoles().size());
            {
                IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(0);
                assertNotNull(iamRole);
                assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), iamRole.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), iamRole.getIamRoleDescription());
            }
            {
                IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(1);
                assertNotNull(iamRole);
                assertEquals(namespaceIamRoleAuthorizationEntity2.getIamRoleName(), iamRole.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity2.getDescription(), iamRole.getIamRoleDescription());
            }
        }
        {
            NamespaceIamRoleAuthorization namespaceIamRoleAuthorization = result.getNamespaceIamRoleAuthorizations().get(1);
            assertNotNull(namespaceIamRoleAuthorization);
            assertEquals(namespaceEntity2.getCode(), namespaceIamRoleAuthorization.getNamespace());
            assertNotNull(namespaceIamRoleAuthorization.getIamRoles());
            assertEquals(2, namespaceIamRoleAuthorization.getIamRoles().size());
            {
                IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(0);
                assertNotNull(iamRole);
                assertEquals(namespaceIamRoleAuthorizationEntity3.getIamRoleName(), iamRole.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity3.getDescription(), iamRole.getIamRoleDescription());
            }
            {
                IamRole iamRole = namespaceIamRoleAuthorization.getIamRoles().get(1);
                assertNotNull(iamRole);
                assertEquals(namespaceIamRoleAuthorizationEntity4.getIamRoleName(), iamRole.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity4.getDescription(), iamRole.getIamRoleDescription());
            }
        }

        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(null);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void getNamespaceIamRoleAuthorizationsAssertResultEmptyWhenDaoReturnsEmpty()
    {
        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(Collections.emptyList());

        NamespaceIamRoleAuthorizations result = namespaceIamRoleAuthorizationServiceImpl.getNamespaceIamRoleAuthorizations();

        assertNotNull(result);
        assertNotNull(result.getNamespaceIamRoleAuthorizations());
        assertEquals(0, result.getNamespaceIamRoleAuthorizations().size());

        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(null);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void updateNamespaceIamRoleAuthorizationAssertCallsDependenciesAndResultExpected()
    {
        String expectedNamespace = "namespace";
        List<IamRole> iamRoles = new ArrayList<>();
        IamRole iamRole1 = new IamRole("iamRoleName1", "iamRoleDescription1");
        iamRoles.add(iamRole1);
        IamRole iamRole2 = new IamRole("iamRoleName2", " ");
        iamRoles.add(iamRole2);
        NamespaceIamRoleAuthorizationUpdateRequest expectedNamespaceIamRoleAuthorizationUpdateRequest =
            new NamespaceIamRoleAuthorizationUpdateRequest(iamRoles);

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity);

        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        NamespaceIamRoleAuthorization result =
            namespaceIamRoleAuthorizationServiceImpl.updateNamespaceIamRoleAuthorization(expectedNamespace, expectedNamespaceIamRoleAuthorizationUpdateRequest);

        assertNotNull(result);
        assertEquals(expectedNamespaceEntity.getCode(), result.getNamespace());
        assertNotNull(result.getIamRoles());
        assertEquals(2, result.getIamRoles().size());
        {
            IamRole iamRole = result.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(iamRole1.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(iamRole1.getIamRoleDescription(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = result.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(iamRole2.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(null, iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verify(namespaceIamRoleAuthorizationDao).delete(namespaceIamRoleAuthorizationEntity);
        verify(namespaceIamRoleAuthorizationDao).saveAndRefresh(
            namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), iamRole1.getIamRoleName(), iamRole1.getIamRoleDescription()));
        verify(namespaceIamRoleAuthorizationDao)
            .saveAndRefresh(namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), iamRole2.getIamRoleName(), null));
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void updateNamespaceIamRoleAuthorizationAssertInputsTrimmed()
    {
        String expectedNamespace = " namespace ";
        List<IamRole> iamRoles = new ArrayList<>();
        IamRole iamRole1 = new IamRole(" iamRoleName1 ", " iamRoleDescription1 ");
        iamRoles.add(iamRole1);
        IamRole iamRole2 = new IamRole(" iamRoleName2 ", " ");
        iamRoles.add(iamRole2);
        NamespaceIamRoleAuthorizationUpdateRequest expectedNamespaceIamRoleAuthorizationUpdateRequest =
            new NamespaceIamRoleAuthorizationUpdateRequest(iamRoles);

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity);

        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(namespaceIamRoleAuthorizationEntities);

        NamespaceIamRoleAuthorization result =
            namespaceIamRoleAuthorizationServiceImpl.updateNamespaceIamRoleAuthorization(expectedNamespace, expectedNamespaceIamRoleAuthorizationUpdateRequest);

        assertNotNull(result);
        assertEquals(expectedNamespaceEntity.getCode(), result.getNamespace());
        assertNotNull(result.getIamRoles());
        assertEquals(2, result.getIamRoles().size());
        {
            IamRole iamRole = result.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(iamRole1.getIamRoleName().trim(), iamRole.getIamRoleName());
            assertEquals(iamRole1.getIamRoleDescription().trim(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = result.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(iamRole2.getIamRoleName().trim(), iamRole.getIamRoleName());
            assertEquals(null, iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace.trim());
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verify(namespaceIamRoleAuthorizationDao).delete(namespaceIamRoleAuthorizationEntity);
        verify(namespaceIamRoleAuthorizationDao).saveAndRefresh(
            namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), iamRole1.getIamRoleName().trim(),
                iamRole1.getIamRoleDescription().trim()));
        verify(namespaceIamRoleAuthorizationDao)
            .saveAndRefresh(namespaceIamRoleAuthorizationEntityEq(expectedNamespaceEntity.getCode(), iamRole2.getIamRoleName().trim(), null));
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void updateNamespaceIamRoleAuthorizationAssertErrorWhenDaoReturnsEmpty()
    {
        String expectedNamespace = "namespace";
        List<IamRole> iamRoles = new ArrayList<>();
        IamRole iamRole1 = new IamRole("iamRoleName1", "iamRoleDescription1");
        iamRoles.add(iamRole1);
        IamRole iamRole2 = new IamRole("iamRoleName2", " ");
        iamRoles.add(iamRole2);
        NamespaceIamRoleAuthorizationUpdateRequest expectedNamespaceIamRoleAuthorizationUpdateRequest =
            new NamespaceIamRoleAuthorizationUpdateRequest(iamRoles);

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);

        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(Collections.emptyList());

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.updateNamespaceIamRoleAuthorization(expectedNamespace, expectedNamespaceIamRoleAuthorizationUpdateRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(ObjectNotFoundException.class, e.getClass());
            assertEquals(String.format("Namespace IAM role authorizations for namespace \"%s\" do not exist", expectedNamespaceEntity.getCode()),
                e.getMessage());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void updateNamespaceIamRoleAuthorizationAssertErrorWhenRoleNameIsBlank()
    {
        String expectedNamespace = "namespace";
        List<IamRole> iamRoles = new ArrayList<>();
        IamRole iamRole1 = new IamRole(" ", "iamRoleDescription1");
        iamRoles.add(iamRole1);
        IamRole iamRole2 = new IamRole(" ", " ");
        iamRoles.add(iamRole2);
        NamespaceIamRoleAuthorizationUpdateRequest expectedNamespaceIamRoleAuthorizationUpdateRequest =
            new NamespaceIamRoleAuthorizationUpdateRequest(iamRoles);

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.updateNamespaceIamRoleAuthorization(expectedNamespace, expectedNamespaceIamRoleAuthorizationUpdateRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("IAM role name must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void updateNamespaceIamRoleAuthorizationAssertErrorWhenIamRolesEmpty()
    {
        String expectedNamespace = "namespace";
        NamespaceIamRoleAuthorizationUpdateRequest expectedNamespaceIamRoleAuthorizationUpdateRequest =
            new NamespaceIamRoleAuthorizationUpdateRequest(Collections.emptyList());

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.updateNamespaceIamRoleAuthorization(expectedNamespace, expectedNamespaceIamRoleAuthorizationUpdateRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("At least 1 IAM roles must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void updateNamespaceIamRoleAuthorizationAssertErrorWhenNamespaceIsBlank()
    {
        String expectedNamespace = " ";
        List<IamRole> iamRoles = new ArrayList<>();
        IamRole iamRole1 = new IamRole("iamRoleName1", "iamRoleDescription1");
        iamRoles.add(iamRole1);
        IamRole iamRole2 = new IamRole("iamRoleName2", " ");
        iamRoles.add(iamRole2);
        NamespaceIamRoleAuthorizationUpdateRequest expectedNamespaceIamRoleAuthorizationUpdateRequest =
            new NamespaceIamRoleAuthorizationUpdateRequest(iamRoles);

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.updateNamespaceIamRoleAuthorization(expectedNamespace, expectedNamespaceIamRoleAuthorizationUpdateRequest);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Namespace must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void deleteNamespaceIamRoleAuthorizationAssertCallsDependenciesAndResultExpected()
    {
        String expectedNamespace = "namespace";

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);

        List<NamespaceIamRoleAuthorizationEntity> expectedNamespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("iamRoleName2");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(expectedNamespaceIamRoleAuthorizationEntities);

        NamespaceIamRoleAuthorization result = namespaceIamRoleAuthorizationServiceImpl.deleteNamespaceIamRoleAuthorization(expectedNamespace);
        assertNotNull(result);
        assertEquals(expectedNamespaceEntity.getCode(), result.getNamespace());
        assertNotNull(result.getIamRoles());
        assertEquals(2, result.getIamRoles().size());
        {
            IamRole iamRole = result.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = result.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity2.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity2.getDescription(), iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verify(namespaceIamRoleAuthorizationDao).delete(namespaceIamRoleAuthorizationEntity1);
        verify(namespaceIamRoleAuthorizationDao).delete(namespaceIamRoleAuthorizationEntity2);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void deleteNamespaceIamRoleAuthorizationAssertInputsAreTrimmed()
    {
        String expectedNamespace = " namespace ";

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);

        List<NamespaceIamRoleAuthorizationEntity> expectedNamespaceIamRoleAuthorizationEntities = new ArrayList<>();
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity1);
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(expectedNamespaceEntity);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("iamRoleName2");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        expectedNamespaceIamRoleAuthorizationEntities.add(namespaceIamRoleAuthorizationEntity2);

        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(expectedNamespaceIamRoleAuthorizationEntities);

        NamespaceIamRoleAuthorization result = namespaceIamRoleAuthorizationServiceImpl.deleteNamespaceIamRoleAuthorization(expectedNamespace);
        assertNotNull(result);
        assertEquals(expectedNamespaceEntity.getCode(), result.getNamespace());
        assertNotNull(result.getIamRoles());
        assertEquals(2, result.getIamRoles().size());
        {
            IamRole iamRole = result.getIamRoles().get(0);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), iamRole.getIamRoleDescription());
        }
        {
            IamRole iamRole = result.getIamRoles().get(1);
            assertNotNull(iamRole);
            assertEquals(namespaceIamRoleAuthorizationEntity2.getIamRoleName(), iamRole.getIamRoleName());
            assertEquals(namespaceIamRoleAuthorizationEntity2.getDescription(), iamRole.getIamRoleDescription());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace.trim());
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verify(namespaceIamRoleAuthorizationDao).delete(namespaceIamRoleAuthorizationEntity1);
        verify(namespaceIamRoleAuthorizationDao).delete(namespaceIamRoleAuthorizationEntity2);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void deleteNamespaceIamRoleAuthorizationAssertErrorWhenDaoReturnsEmpty()
    {
        String expectedNamespace = "namespace";

        NamespaceEntity expectedNamespaceEntity = new NamespaceEntity();
        expectedNamespaceEntity.setCode("NAMESPACE");

        when(namespaceDaoHelper.getNamespaceEntity(any())).thenReturn(expectedNamespaceEntity);

        when(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(any())).thenReturn(Collections.emptyList());

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.deleteNamespaceIamRoleAuthorization(expectedNamespace);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(ObjectNotFoundException.class, e.getClass());
            assertEquals(String.format("Namespace IAM role authorizations for namespace \"%s\" do not exist", expectedNamespaceEntity.getCode()),
                e.getMessage());
        }

        verify(namespaceDaoHelper).getNamespaceEntity(expectedNamespace);
        verify(namespaceIamRoleAuthorizationDao).getNamespaceIamRoleAuthorizations(expectedNamespaceEntity);
        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    @Test
    public void deleteNamespaceIamRoleAuthorizationAssertErrorWhenNamespaceIsBlank()
    {
        String expectedNamespace = " ";

        try
        {
            namespaceIamRoleAuthorizationServiceImpl.deleteNamespaceIamRoleAuthorization(expectedNamespace);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Namespace must be specified", e.getMessage());
        }

        verifyNoMoreInteractions(namespaceDaoHelper, namespaceIamRoleAuthorizationDao);
    }

    /**
     * Creates and returns an ArgumentMatcher that matches when the given parameters equal NamespaceIamRoleAuthorizationEntity.
     *
     * @param expectedNamespace The namespace to match
     * @param expectedIamRoleName The role name to match
     * @param expectedDescription The description to match
     *
     * @return The argument matcher
     */
    private NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntityEq(String expectedNamespace, String expectedIamRoleName,
        String expectedDescription)
    {
        return argThat(new ArgumentMatcher<NamespaceIamRoleAuthorizationEntity>()
        {
            @Override
            public boolean matches(Object argument)
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = (NamespaceIamRoleAuthorizationEntity) argument;
                String namespace = namespaceIamRoleAuthorizationEntity.getNamespace().getCode();
                String iamRoleName = namespaceIamRoleAuthorizationEntity.getIamRoleName();
                String description = namespaceIamRoleAuthorizationEntity.getDescription();

                return Objects.equal(namespace, expectedNamespace) && Objects.equal(iamRoleName, expectedIamRoleName) &&
                    Objects.equal(description, expectedDescription);
            }
        });
    }
}
