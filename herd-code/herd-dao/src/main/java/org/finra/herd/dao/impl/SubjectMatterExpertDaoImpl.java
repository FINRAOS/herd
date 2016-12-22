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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Repository;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.LdapOperations;
import org.finra.herd.dao.SubjectMatterExpertDao;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;
import org.finra.herd.model.dto.ConfigurationValue;

@Repository
public class SubjectMatterExpertDaoImpl implements SubjectMatterExpertDao
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private LdapOperations ldapOperations;

    @Autowired
    private LdapTemplate ldapTemplate;

    @Override
    public SubjectMatterExpertContactDetails getSubjectMatterExpertByKey(SubjectMatterExpertKey subjectMatterExpertKey)
    {
        List<SubjectMatterExpertContactDetails> subjectMatterExpertContactDetailsList = ldapOperations.search(ldapTemplate,
            query().where(configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_ID)).is(subjectMatterExpertKey.getUserId()),
            new SubjectMatterExpertContactDetailsMapper(subjectMatterExpertKey.getUserId(),
                configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_FULL_NAME),
                configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_JOB_TITLE),
                configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS),
                configurationHelper.getProperty(ConfigurationValue.LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER)));

        return CollectionUtils.isNotEmpty(subjectMatterExpertContactDetailsList) ? subjectMatterExpertContactDetailsList.get(0) : null;
    }

    private static class SubjectMatterExpertContactDetailsMapper implements AttributesMapper<SubjectMatterExpertContactDetails>
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
         * User if of the subject matter expert.
         */
        private String userId;

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
         * @param userId the user id of the subject matter expert
         * @param userFullNameAttribute the LDAP attribute id for user's full name
         * @param userJobTitleAttribute the LDAP attribute id for user's job title
         * @param userEmailAddressAttribute the LDAP attribute id for user's e-mail address
         * @param userTelephoneNumberAttribute the LDAP attribute id for user's telephone number
         */
        public SubjectMatterExpertContactDetailsMapper(String userId, String userFullNameAttribute, String userJobTitleAttribute,
            String userEmailAddressAttribute, String userTelephoneNumberAttribute)
        {
            this.userId = userId;
            this.userFullNameAttribute = userFullNameAttribute;
            this.userJobTitleAttribute = userJobTitleAttribute;
            this.userEmailAddressAttribute = userEmailAddressAttribute;
            this.userTelephoneNumberAttribute = userTelephoneNumberAttribute;
        }

        /**
         * Map Attributes to a subject matter expert details object. The supplied attributes are the attributes from a single search result.
         *
         * @param attributes the attributes from a search result
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
