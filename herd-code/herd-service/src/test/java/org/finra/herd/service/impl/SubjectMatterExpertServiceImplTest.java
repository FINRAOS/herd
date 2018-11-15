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

import static org.finra.herd.dao.AbstractDaoTest.USER_EMAIL_ADDRESS;
import static org.finra.herd.dao.AbstractDaoTest.USER_FULL_NAME;
import static org.finra.herd.dao.AbstractDaoTest.USER_ID;
import static org.finra.herd.dao.AbstractDaoTest.USER_ID_2;
import static org.finra.herd.dao.AbstractDaoTest.USER_JOB_TITLE;
import static org.finra.herd.dao.AbstractDaoTest.USER_TELEPHONE_NUMBER;
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

import org.finra.herd.dao.SubjectMatterExpertDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SubjectMatterExpert;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;
import org.finra.herd.service.helper.AlternateKeyHelper;

/**
 * This class tests various functionality within the subject matter expert service.
 */
public class SubjectMatterExpertServiceImplTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private SubjectMatterExpertDao subjectMatterExpertDao;

    @InjectMocks
    private SubjectMatterExpertServiceImpl subjectMatterExpertServiceImpl;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSubjectMatterExpert() throws Exception
    {
        // Create a subject matter expert key.
        SubjectMatterExpertKey originalSubjectMatterExpertKey = new SubjectMatterExpertKey(USER_ID);
        SubjectMatterExpertKey updatedSubjectMatterExpertKey = new SubjectMatterExpertKey(USER_ID_2);

        // Create subject matter expert contact details initialised with test data.
        SubjectMatterExpertContactDetails subjectMatterExpertContactDetails =
            new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("user id", USER_ID)).thenReturn(USER_ID_2);
        when(subjectMatterExpertDao.getSubjectMatterExpertByKey(updatedSubjectMatterExpertKey)).thenReturn(subjectMatterExpertContactDetails);

        // Call the method under test.
        SubjectMatterExpert result = subjectMatterExpertServiceImpl.getSubjectMatterExpert(originalSubjectMatterExpertKey);

        // Validate the results.
        assertEquals(new SubjectMatterExpert(updatedSubjectMatterExpertKey, subjectMatterExpertContactDetails), result);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("user id", USER_ID);
        verify(subjectMatterExpertDao).getSubjectMatterExpertByKey(updatedSubjectMatterExpertKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSubjectMatterExpertMissingSubjectMatterExpertKey()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A subject matter expert key must be specified.");

        // Try to call the method under test.
        subjectMatterExpertServiceImpl.getSubjectMatterExpert(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSubjectMatterExpertUserNoExists()
    {
        // Create a subject matter expert key.
        SubjectMatterExpertKey originalSubjectMatterExpertKey = new SubjectMatterExpertKey(USER_ID);
        SubjectMatterExpertKey updatedSubjectMatterExpertKey = new SubjectMatterExpertKey(USER_ID_2);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("user id", USER_ID)).thenReturn(USER_ID_2);
        when(subjectMatterExpertDao.getSubjectMatterExpertByKey(updatedSubjectMatterExpertKey)).thenReturn(null);

        // Specify the expected exception.
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage(String.format("The subject matter expert with user id \"%s\" does not exist.", USER_ID_2));

        // Try to call the method under test.
        subjectMatterExpertServiceImpl.getSubjectMatterExpert(originalSubjectMatterExpertKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("user id", USER_ID);
        verify(subjectMatterExpertDao).getSubjectMatterExpertByKey(updatedSubjectMatterExpertKey);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, subjectMatterExpertDao);
    }
}
