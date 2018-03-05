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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.CredStashHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.BusinessObjectFormatService;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.DataProviderDaoHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

public class RelationalTableRegistrationHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private BusinessObjectDataDao businessObjectDataDao;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private BusinessObjectFormatService businessObjectFormatService;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CredStashHelper credStashHelper;

    @Mock
    private DataProviderDaoHelper dataProviderDaoHelper;

    @Mock
    private MessageNotificationEventService messageNotificationEventService;

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @InjectMocks
    private RelationalTableRegistrationHelperServiceImpl relationalTableRegistrationHelperServiceImpl;

    @Mock
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetPassword()
    {
        // Call the method under test.
        String result =
            relationalTableRegistrationHelperServiceImpl.getPassword(new RelationalStorageAttributesDto(JDBC_URL, USERNAME, PASSWORD, NO_USER_CREDENTIAL_NAME));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(PASSWORD, result);
    }

    @Test
    public void testGetPasswordGetCredentialFromCredStash() throws Exception
    {
        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_RELATIONAL_STORAGE_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME))
            .thenThrow(new CredStashGetCredentialFailedException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            relationalTableRegistrationHelperServiceImpl.getPassword(new RelationalStorageAttributesDto(JDBC_URL, USERNAME, NO_PASSWORD, USER_CREDENTIAL_NAME));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("%s: %s", CredStashGetCredentialFailedException.class.getName(), ERROR_MESSAGE), e.getMessage());
        }

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_RELATIONAL_STORAGE_ENCRYPTION_CONTEXT);
        verify(credStashHelper).getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetPasswordGetCredentialFromCredStashFailed() throws Exception
    {
        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_RELATIONAL_STORAGE_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME)).thenReturn(PASSWORD);

        // Call the method under test.
        String result =
            relationalTableRegistrationHelperServiceImpl.getPassword(new RelationalStorageAttributesDto(JDBC_URL, USERNAME, NO_PASSWORD, USER_CREDENTIAL_NAME));

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_RELATIONAL_STORAGE_ENCRYPTION_CONTEXT);
        verify(credStashHelper).getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(PASSWORD, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDataDao, businessObjectDataHelper, businessObjectDataStatusDaoHelper,
            businessObjectDefinitionDao, businessObjectDefinitionDaoHelper, businessObjectFormatDaoHelper, businessObjectFormatHelper,
            businessObjectFormatService, configurationHelper, credStashHelper, dataProviderDaoHelper, messageNotificationEventService, namespaceDaoHelper,
            searchIndexUpdateHelper, storageDaoHelper, storageHelper, storageUnitStatusDaoHelper);
    }
}
