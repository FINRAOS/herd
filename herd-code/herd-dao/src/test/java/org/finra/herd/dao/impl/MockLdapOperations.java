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

import java.util.ArrayList;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.query.LdapQuery;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.LdapOperations;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * Mocked implementation of {@link org.finra.herd.dao.LdapOperations}. Does not actually use {@link org.springframework.ldap.core.LdapTemplate}. Instead, a
 * predefined result is returned.
 */
public class MockLdapOperations implements LdapOperations
{
    /**
     * The user id to hint operation not to return user's phone number.
     */
    public static final String MOCK_USER_ID_ATTRIBUTE_USER_TELEPHONE_NUMBER_NO_EXISTS = "mock_user_id_attribute_user_telephone_number_no_exists";

    /**
     * The user id to hint operation to return no results back.
     */
    public static final String MOCK_USER_ID_USER_NO_EXISTS = "mock_user_id_user_no_exists";

    @Autowired
    ConfigurationHelper configurationHelper;

    /**
     * Executes {@link org.springframework.ldap.core.LdapTemplate#search(org.springframework.ldap.query.LdapQuery,
     * org.springframework.ldap.core.AttributesMapper)}.
     *
     * @param ldapTemplate the LDAP template to use
     * @param query the LDAP query specification
     * @param mapper the <code>Attributes</code> to supply all found Attributes to
     *
     * @return the predefined LDAP search results constructed by the given {@link org.springframework.ldap.core.AttributesMapper}
     */
    @Override
    public <T> List<T> search(LdapTemplate ldapTemplate, LdapQuery query, AttributesMapper<T> mapper)
    {
        // Create an empty results list.
        List<T> results = new ArrayList<>();

        // Get the query filter as a string.
        String filter = query.filter().toString();

        // Check if we need to respond with the predefined result.
        if (!filter.contains(MOCK_USER_ID_USER_NO_EXISTS))
        {
            // Create attributes object with ignoreCase flag set to "true".
            BasicAttributes attributes = new BasicAttributes(true);

            // Populate the attributes with predefined set of results.
            attributes
                .put(new BasicAttribute(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME), AbstractDaoTest.USER_FULL_NAME));
            attributes
                .put(new BasicAttribute(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE), AbstractDaoTest.USER_JOB_TITLE));
            attributes.put(
                new BasicAttribute(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS), AbstractDaoTest.USER_EMAIL_ADDRESS));

            // Check if it is OK to add the user phone number attribute.
            if (!filter.contains(MOCK_USER_ID_ATTRIBUTE_USER_TELEPHONE_NUMBER_NO_EXISTS))
            {
                attributes.put(new BasicAttribute(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER),
                    AbstractDaoTest.USER_TELEPHONE_NUMBER));
            }

            // Map the results.
            try
            {
                results.add(mapper.mapFromAttributes(attributes));
            }
            catch (NamingException e)
            {
                // Do nothing.
            }
        }

        // Return the results.
        return results;
    }
}
