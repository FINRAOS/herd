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

import static org.springframework.ldap.query.LdapQueryBuilder.query;

import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.stereotype.Repository;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.LdapOperations;
import org.finra.herd.dao.SubjectMatterExpertDao;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.CredStashHelper;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;
import org.finra.herd.model.dto.ConfigurationValue;

@Repository
public class SubjectMatterExpertDaoImpl implements SubjectMatterExpertDao
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubjectMatterExpertDaoImpl.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CredStashHelper credStashHelper;

    @Autowired
    private LdapOperations ldapOperations;

    @Override
    public SubjectMatterExpertContactDetails getSubjectMatterExpertByKey(SubjectMatterExpertKey subjectMatterExpertKey)
    {
        // Get LDAP specific configuration settings.
        final String ldapUrl = configurationHelper.getProperty(ConfigurationValue.LDAP_URL);
        final String ldapBase = configurationHelper.getProperty(ConfigurationValue.LDAP_BASE);
        final String ldapUserDn = configurationHelper.getProperty(ConfigurationValue.LDAP_USER_DN);
        final String credStashEncryptionContext = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT);
        final String ldapUserCredentialName = configurationHelper.getProperty(ConfigurationValue.LDAP_USER_CREDENTIAL_NAME);

        // Log configuration values being used to create LDAP context source.
        LOGGER.info("Creating LDAP context source using the following parameters: {}=\"{}\" {}=\"{}\" {}=\"{}\" {}=\"{}\" {}=\"{}\"...",
            ConfigurationValue.LDAP_URL.getKey(), ldapUrl, ConfigurationValue.LDAP_BASE.getKey(), ldapBase, ConfigurationValue.LDAP_USER_DN.getKey(),
            ldapUserDn, ConfigurationValue.CREDSTASH_HERD_ENCRYPTION_CONTEXT.getKey(), credStashEncryptionContext,
            ConfigurationValue.LDAP_USER_CREDENTIAL_NAME.getKey(), ldapUserCredentialName);

        // Retrieve LDAP user password from the credstash.
        String ldapUserPassword;
        try
        {
            ldapUserPassword = credStashHelper.getCredentialFromCredStash(credStashEncryptionContext, ldapUserCredentialName);
        }
        catch (CredStashGetCredentialFailedException e)
        {
            throw new IllegalStateException(e);
        }

        // Create and initialize an LDAP context source.
        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(ldapUrl);
        contextSource.setBase(ldapBase);
        contextSource.setUserDn(ldapUserDn);
        contextSource.setPassword(ldapUserPassword);

        // Create an LDAP template.
        LdapTemplate ldapTemplate = new LdapTemplate(contextSource);

        // Create an LDAP query.
        LdapQuery ldapQuery = query().where(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_ID)).is(subjectMatterExpertKey.getUserId());

        // Create a subject matter expert contact details mapper.
        SubjectMatterExpertContactDetailsMapper subjectMatterExpertContactDetailsMapper =
            new SubjectMatterExpertContactDetailsMapper(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME),
                configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE),
                configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS),
                configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER));

        // Gets information for the specified subject matter expert.
        List<SubjectMatterExpertContactDetails> subjectMatterExpertContactDetailsList =
            ldapOperations.search(ldapTemplate, ldapQuery, subjectMatterExpertContactDetailsMapper);

        // Return the results.
        return CollectionUtils.isNotEmpty(subjectMatterExpertContactDetailsList) ? subjectMatterExpertContactDetailsList.get(0) : null;
    }

    static class SubjectMatterExpertContactDetailsMapper implements AttributesMapper<SubjectMatterExpertContactDetails>
    {
        /**
         * The LDAP attribute id for user's e-mail address
         */
        private String userEmailAddressAttribute;

        /**
         * The LDAP attribute id for user's full name
         */
        private String userFullNameAttribute;

        /**
         * The LDAP attribute id for user's job title
         */
        private String userJobTitleAttribute;

        /**
         * The LDAP attribute id for user's telephone number
         */
        private String userTelephoneNumberAttribute;

        /**
         * Fully-initialising value constructor.
         *
         * @param userFullNameAttribute the LDAP attribute id for user's full name
         * @param userJobTitleAttribute the LDAP attribute id for user's job title
         * @param userEmailAddressAttribute the LDAP attribute id for user's e-mail address
         * @param userTelephoneNumberAttribute the LDAP attribute id for user's telephone number
         */
        SubjectMatterExpertContactDetailsMapper(String userFullNameAttribute, String userJobTitleAttribute, String userEmailAddressAttribute,
            String userTelephoneNumberAttribute)
        {
            this.userFullNameAttribute = userFullNameAttribute;
            this.userJobTitleAttribute = userJobTitleAttribute;
            this.userEmailAddressAttribute = userEmailAddressAttribute;
            this.userTelephoneNumberAttribute = userTelephoneNumberAttribute;
        }

        /**
         * Map attributes to a subject matter expert details object. The supplied attributes are the attributes from a single search result.
         *
         * @param attributes the attributes from a search result
         *
         * @return the subject matter expert contact details
         * @throws NamingException if a naming exception was encountered while retrieving attribute value
         */
        public SubjectMatterExpertContactDetails mapFromAttributes(Attributes attributes) throws NamingException
        {
            SubjectMatterExpertContactDetails subjectMatterExpertContactDetails = new SubjectMatterExpertContactDetails();
            subjectMatterExpertContactDetails.setFullName(getAttributeById(attributes, userFullNameAttribute));
            subjectMatterExpertContactDetails.setJobTitle(getAttributeById(attributes, userJobTitleAttribute));
            subjectMatterExpertContactDetails.setEmailAddress(getAttributeById(attributes, userEmailAddressAttribute));
            subjectMatterExpertContactDetails.setTelephoneNumber(getAttributeById(attributes, userTelephoneNumberAttribute));
            return subjectMatterExpertContactDetails;
        }

        /**
         * Retrieves a single attribute value from attributes per specified attribute id.
         *
         * @param attributes the attributes from a search result
         * @param attributeId the attribute id
         *
         * @return the value of the attribute, or null if attribute value is not present
         */
        private String getAttributeById(Attributes attributes, String attributeId) throws NamingException
        {
            String attributeValue = null;

            Attribute attribute = attributes.get(attributeId);
            if (attribute != null)
            {
                attributeValue = (String) attribute.get();
            }

            return attributeValue;
        }
    }
}
