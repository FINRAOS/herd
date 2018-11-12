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
import static org.springframework.ldap.query.LdapQueryBuilder.query;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.query.LdapQuery;

import org.finra.herd.dao.impl.SubjectMatterExpertDaoImpl;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests the functionality of LDAP operations.
 */
public class LdapOperationsTest extends AbstractDaoTest
{
    @Test
    public void testSearch()
    {
        // Create and initialize an LDAP context source.
        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(LDAP_URL);
        contextSource.setBase(LDAP_BASE);
        contextSource.setUserDn(LDAP_USER_DN);
        contextSource.setPassword(PASSWORD);
        contextSource.afterPropertiesSet();

        // Create an LDAP template.
        LdapTemplate ldapTemplate = new LdapTemplate(contextSource);

        // Create an LDAP query.
        LdapQuery ldapQuery = query().where((String) ConfigurationValue.LDAP_ATTRIBUTE_USER_ID.getDefaultValue()).is(USER_ID);

        // Create a subject matter expert contact details mapper.
        SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper subjectMatterExpertContactDetailsMapper =
            new SubjectMatterExpertDaoImpl.SubjectMatterExpertContactDetailsMapper((String) ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME.getDefaultValue(),
                (String) ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE.getDefaultValue(),
                (String) ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS.getDefaultValue(),
                (String) ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER.getDefaultValue());

        // Gets information for the specified subject matter expert.
        List<SubjectMatterExpertContactDetails> result = ldapOperations.search(ldapTemplate, ldapQuery, subjectMatterExpertContactDetailsMapper);

        // Validate the results.
        assertEquals(
            Collections.singletonList(new SubjectMatterExpertContactDetails(USER_FULL_NAME, USER_JOB_TITLE, USER_EMAIL_ADDRESS, USER_TELEPHONE_NUMBER)),
            result);
    }
}
