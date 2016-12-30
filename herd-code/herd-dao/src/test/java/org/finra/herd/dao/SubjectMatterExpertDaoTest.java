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

import org.junit.Test;

import org.finra.herd.dao.impl.MockLdapOperations;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;

public class SubjectMatterExpertDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetSubjectMatterExpertByKey()
    {
        // Get contact details for the subject matter expert.
        assertEquals(new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER),
            subjectMatterExpertDao.getSubjectMatterExpertByKey(new SubjectMatterExpertKey(USER_ID)));

        // Get contact details for the subject matter expert without a phone number attribute.
        assertEquals(new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, NO_USER_TELEPHONE_NUMBER), subjectMatterExpertDao
            .getSubjectMatterExpertByKey(new SubjectMatterExpertKey(MockLdapOperations.MOCK_USER_ID_ATTRIBUTE_USER_TELEPHONE_NUMBER_NO_EXISTS)));

        // Try to get contact details for the subject matter expert that does not exist.
        assertNull(subjectMatterExpertDao.getSubjectMatterExpertByKey(new SubjectMatterExpertKey(MockLdapOperations.MOCK_USER_ID_USER_NO_EXISTS)));
    }
}
