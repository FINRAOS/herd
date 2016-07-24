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

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;

public class NamespaceIamRoleAuthorizationDaoTest extends AbstractDaoTest
{
    @Autowired
    private NamespaceIamRoleAuthorizationDao namespaceIamRoleAuthorizationDao;

    @Test
    public void getNamespaceIamRoleAuthorizationsAssertFilterByNamespaceWhenGiven()
    {
        NamespaceEntity namespaceEntity1 = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        NamespaceEntity namespaceEntity2 = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        {
            List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizations =
                namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(namespaceEntity1);

            assertNotNull(namespaceIamRoleAuthorizations);
            assertEquals(0, namespaceIamRoleAuthorizations.size());
        }

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity1);

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(namespaceEntity2);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("iamRoleName2");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity2);

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity3 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity3.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity3.setIamRoleName("iamRoleName3");
        namespaceIamRoleAuthorizationEntity3.setDescription("description3");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity3);

        {
            List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizations =
                namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(namespaceEntity1);

            assertNotNull(namespaceIamRoleAuthorizations);
            assertEquals(2, namespaceIamRoleAuthorizations.size());
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = namespaceIamRoleAuthorizations.get(0);
                assertNotNull(namespaceIamRoleAuthorizationEntity);
                assertEquals(namespaceEntity1.getCode(), namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
                assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), namespaceIamRoleAuthorizationEntity.getDescription());
            }
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = namespaceIamRoleAuthorizations.get(1);
                assertNotNull(namespaceIamRoleAuthorizationEntity);
                assertEquals(namespaceEntity1.getCode(), namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
                assertEquals(namespaceIamRoleAuthorizationEntity3.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity3.getDescription(), namespaceIamRoleAuthorizationEntity.getDescription());
            }
        }
    }

    @Test
    public void getNamespaceIamRoleAuthorizationsAssertOrderByRoleName()
    {
        NamespaceEntity namespaceEntity1 = namespaceDaoTestHelper.createNamespaceEntity();

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("Z");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity1);

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("A");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity2);

        {
            List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizations =
                namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(namespaceEntity1);

            assertNotNull(namespaceIamRoleAuthorizations);
            assertEquals(2, namespaceIamRoleAuthorizations.size());
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = namespaceIamRoleAuthorizations.get(0);
                assertNotNull(namespaceIamRoleAuthorizationEntity);
                assertEquals(namespaceEntity1.getCode(), namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
                assertEquals(namespaceIamRoleAuthorizationEntity2.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity2.getDescription(), namespaceIamRoleAuthorizationEntity.getDescription());
            }
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = namespaceIamRoleAuthorizations.get(1);
                assertNotNull(namespaceIamRoleAuthorizationEntity);
                assertEquals(namespaceEntity1.getCode(), namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
                assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), namespaceIamRoleAuthorizationEntity.getDescription());
            }
        }
    }

    @Test
    public void getNamespaceIamRoleAuthorizationsAssertGetAllWhenNamespaceIsNotGiven()
    {
        NamespaceEntity namespaceEntity1 = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        NamespaceEntity namespaceEntity2 = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity1 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity1.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity1.setIamRoleName("iamRoleName1");
        namespaceIamRoleAuthorizationEntity1.setDescription("description1");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity1);

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity2 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity2.setNamespace(namespaceEntity2);
        namespaceIamRoleAuthorizationEntity2.setIamRoleName("iamRoleName2");
        namespaceIamRoleAuthorizationEntity2.setDescription("description2");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity2);

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity3 = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity3.setNamespace(namespaceEntity1);
        namespaceIamRoleAuthorizationEntity3.setIamRoleName("iamRoleName3");
        namespaceIamRoleAuthorizationEntity3.setDescription("description3");
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity3);

        {
            List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizations = namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(null);

            assertNotNull(namespaceIamRoleAuthorizations);
            assertEquals(3, namespaceIamRoleAuthorizations.size());
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = namespaceIamRoleAuthorizations.get(0);
                assertNotNull(namespaceIamRoleAuthorizationEntity);
                assertEquals(namespaceEntity1.getCode(), namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
                assertEquals(namespaceIamRoleAuthorizationEntity1.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity1.getDescription(), namespaceIamRoleAuthorizationEntity.getDescription());
            }
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = namespaceIamRoleAuthorizations.get(1);
                assertNotNull(namespaceIamRoleAuthorizationEntity);
                assertEquals(namespaceEntity1.getCode(), namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
                assertEquals(namespaceIamRoleAuthorizationEntity3.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity3.getDescription(), namespaceIamRoleAuthorizationEntity.getDescription());
            }
            {
                NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = namespaceIamRoleAuthorizations.get(2);
                assertNotNull(namespaceIamRoleAuthorizationEntity);
                assertEquals(namespaceEntity2.getCode(), namespaceIamRoleAuthorizationEntity.getNamespace().getCode());
                assertEquals(namespaceIamRoleAuthorizationEntity2.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getIamRoleName());
                assertEquals(namespaceIamRoleAuthorizationEntity2.getDescription(), namespaceIamRoleAuthorizationEntity.getDescription());
            }
        }
    }
}
