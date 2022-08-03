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
package org.finra.herd.dao.impl;

import static org.finra.herd.dao.AbstractDaoTest.CREDSTASH_ENCRYPTION_CONTEXT;
import static org.finra.herd.dao.AbstractDaoTest.ERROR_MESSAGE;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_ATTRIBUTE_USER_FULL_NAME;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_ATTRIBUTE_USER_SHORT_ID;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_ATTRIBUTE_USER_JOB_TITLE;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_BASE;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_URL;
import static org.finra.herd.dao.AbstractDaoTest.LDAP_USER_DN;
import static org.finra.herd.dao.AbstractDaoTest.PASSWORD;
import static org.finra.herd.dao.AbstractDaoTest.USER_CREDENTIAL_NAME;
import static org.finra.herd.dao.AbstractDaoTest.USER_EMAIL_ADDRESS;
import static org.finra.herd.dao.AbstractDaoTest.USER_FULL_NAME;
import static org.finra.herd.dao.AbstractDaoTest.USER_ID;
import static org.finra.herd.dao.AbstractDaoTest.SHORT_USER_ID;
import static org.finra.herd.dao.AbstractDaoTest.USER_JOB_TITLE;
import static org.finra.herd.dao.AbstractDaoTest.USER_TELEPHONE_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.ldap.query.LdapQueryBuilder.query;

import java.util.Collections;
import java.util.List;

import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.query.LdapQuery;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.LdapOperations;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.CredStashHelper;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the SubjectMatterExpert DAO implementation.
 */
public class SubjectMatterExpertDaoImplTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CredStashHelper credStashHelper;

    @Mock
    private LdapOperations ldapOperations;

    @InjectMocks
    private SubjectMatterExpertDaoImpl subjectMatterExpertDaoImpl;

    @Captor
    private ArgumentCaptor<LdapQuery> ldapQueryArgumentCaptor;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSubjectMatterExpertByUserIdWithDomain() throws Exception
    {
        testGetSubjectMatterExpertByKey(USER_ID, SHORT_USER_ID);
    }

    @Test
    public void testGetSubjectMatterExpertByUserIdWithoutDomain() throws Exception
    {
        testGetSubjectMatterExpertByKey(SHORT_USER_ID, SHORT_USER_ID);
    }

    public void testGetSubjectMatterExpertByKey(String userId, String shortUserId) throws Exception
    {
        // Create a subject matter expert key.
        SubjectMatterExpertKey subjectMatterExpertKey = new SubjectMatterExpertKey(userId);

        // Create subject matter expert contact details initialised with test data.
        SubjectMatterExpertContactDetails subjectMatterExpertContactDetails =
            new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_URL)).thenReturn(LDAP_URL);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_BASE)).thenReturn(LDAP_BASE);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_USER_DN)).thenReturn(LDAP_USER_DN);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_USER_CREDENTIAL_NAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME)).thenReturn(PASSWORD);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_SHORT_ID)).thenReturn(LDAP_ATTRIBUTE_USER_SHORT_ID);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME)).thenReturn(LDAP_ATTRIBUTE_USER_FULL_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE)).thenReturn(LDAP_ATTRIBUTE_USER_JOB_TITLE);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS)).thenReturn(LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER)).thenReturn(LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER);

        when(ldapOperations.search(any(LdapTemplate.class), ldapQueryArgumentCaptor.capture(),
            any(SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper.class))).thenReturn(
            Collections.singletonList(subjectMatterExpertContactDetails));

        // Call the method under test.
        SubjectMatterExpertContactDetails result = subjectMatterExpertDaoImpl.getSubjectMatterExpertByKey(subjectMatterExpertKey);

        // Validate the results.
        assertEquals(subjectMatterExpertContactDetails, result);

        // Verify the external calls.
        assertEquals(query().where(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_SHORT_ID)).is(shortUserId).filter(),
            ldapQueryArgumentCaptor.getValue().filter());
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_URL);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_BASE);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_USER_DN);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_USER_CREDENTIAL_NAME);
        verify(credStashHelper).getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);
        verify(configurationHelper, times(2)).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_SHORT_ID);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER);
        verify(ldapOperations).search(any(LdapTemplate.class), any(LdapQuery.class),
            any(SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSubjectMatterExpertByKeyCredStashGetCredentialFailedException() throws Exception
    {
        // Create a subject matter expert key.
        SubjectMatterExpertKey subjectMatterExpertKey = new SubjectMatterExpertKey(USER_ID);

        // Create subject matter expert contact details initialised with test data.
        SubjectMatterExpertContactDetails subjectMatterExpertContactDetails =
            new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_URL)).thenReturn(LDAP_URL);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_BASE)).thenReturn(LDAP_BASE);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_USER_DN)).thenReturn(LDAP_USER_DN);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_USER_CREDENTIAL_NAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME)).thenThrow(
            new CredStashGetCredentialFailedException(ERROR_MESSAGE));

        // Specify the expected exception.
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(ERROR_MESSAGE);

        // Try to call the method under test.
        subjectMatterExpertDaoImpl.getSubjectMatterExpertByKey(subjectMatterExpertKey);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_URL);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_BASE);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_USER_DN);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_USER_CREDENTIAL_NAME);
        verify(credStashHelper).getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSubjectMatterExpertByKeyUserNoExists() throws Exception
    {
        // Create a subject matter expert key.
        SubjectMatterExpertKey subjectMatterExpertKey = new SubjectMatterExpertKey(USER_ID);

        // Create subject matter expert contact details initialised with test data.
        SubjectMatterExpertContactDetails subjectMatterExpertContactDetails =
            new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER);

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_URL)).thenReturn(LDAP_URL);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_BASE)).thenReturn(LDAP_BASE);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_USER_DN)).thenReturn(LDAP_USER_DN);
        when(configurationHelper.getProperty(ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT)).thenReturn(CREDSTASH_ENCRYPTION_CONTEXT);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_USER_CREDENTIAL_NAME)).thenReturn(USER_CREDENTIAL_NAME);
        when(credStashHelper.getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME)).thenReturn(PASSWORD);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_SHORT_ID)).thenReturn(LDAP_ATTRIBUTE_USER_SHORT_ID);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME)).thenReturn(LDAP_ATTRIBUTE_USER_FULL_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE)).thenReturn(LDAP_ATTRIBUTE_USER_JOB_TITLE);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS)).thenReturn(LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS);
        when(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER)).thenReturn(LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER);
        when(ldapOperations.search(any(LdapTemplate.class), any(LdapQuery.class),
            any(SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper.class))).thenReturn(Collections.emptyList());

        // Call the method under test.
        SubjectMatterExpertContactDetails result = subjectMatterExpertDaoImpl.getSubjectMatterExpertByKey(subjectMatterExpertKey);

        // Validate the results.
        assertNull(result);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_URL);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_BASE);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_USER_DN);
        verify(configurationHelper).getProperty(ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_USER_CREDENTIAL_NAME);
        verify(credStashHelper).getCredentialFromCredStash(CREDSTASH_ENCRYPTION_CONTEXT, USER_CREDENTIAL_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_SHORT_ID);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS);
        verify(configurationHelper).getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER);
        verify(ldapOperations).search(any(LdapTemplate.class), any(LdapQuery.class),
            any(SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testSubjectMatterExpertContactDetailsMapper() throws Exception
    {
        // Create a subject matter expert contact details mapper.
        SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper subjectMatterExpertContactDetailsMapper =
            new SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper(LDAP_ATTRIBUTE_USER_FULL_NAME, LDAP_ATTRIBUTE_USER_JOB_TITLE,
                LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS, LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER);

        // Create attributes object with ignoreCase flag set to "true".
        BasicAttributes attributes = new BasicAttributes(true);

        // Populate the attributes with predefined set of results.
        attributes.put(new BasicAttribute(LDAP_ATTRIBUTE_USER_FULL_NAME, USER_FULL_NAME));
        attributes.put(new BasicAttribute(LDAP_ATTRIBUTE_USER_JOB_TITLE, USER_JOB_TITLE));
        attributes.put(new BasicAttribute(LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS, USER_EMAIL_ADDRESS));
        attributes.put(new BasicAttribute(LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER, USER_TELEPHONE_NUMBER));

        // Map the results.
        List<SubjectMatterExpertContactDetails> result = Collections.singletonList(subjectMatterExpertContactDetailsMapper.mapFromAttributes(attributes));

        // Validate the results.
        assertEquals(
            Collections.singletonList(new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER)),
            result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(configurationHelper, credStashHelper, ldapOperations);
    }
}
