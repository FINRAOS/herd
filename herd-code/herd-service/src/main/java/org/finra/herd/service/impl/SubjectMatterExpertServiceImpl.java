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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.SubjectMatterExpertDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SubjectMatterExpert;
import org.finra.herd.model.api.xml.SubjectMatterExpertContactDetails;
import org.finra.herd.model.api.xml.SubjectMatterExpertKey;
import org.finra.herd.service.SubjectMatterExpertService;
import org.finra.herd.service.helper.AlternateKeyHelper;

/**
 * The subject matter expert service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SubjectMatterExpertServiceImpl implements SubjectMatterExpertService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private SubjectMatterExpertDao subjectMatterExpertDao;

    @Override
    public SubjectMatterExpert getSubjectMatterExpert(SubjectMatterExpertKey subjectMatterExpertKey)
    {
        // Validate and trim the subject matter expert key.
        validateSubjectMatterExpertKey(subjectMatterExpertKey);

        // Get the information for the subject matter expert.
        SubjectMatterExpertContactDetails subjectMatterExpertContactDetails = subjectMatterExpertDao.getSubjectMatterExpertByKey(subjectMatterExpertKey);
        if (subjectMatterExpertContactDetails == null)
        {
            throw new ObjectNotFoundException(
                String.format("The subject matter expert with user id \"%s\" does not exist.", subjectMatterExpertKey.getUserId()));
        }

        // Create and return the subject matter expert.
        return new SubjectMatterExpert(subjectMatterExpertKey, subjectMatterExpertContactDetails);
    }

    /**
     * Validates the subject matter expert key. This method also trims the key parameters.
     *
     * @param key the subject matter expert key
     */
    private void validateSubjectMatterExpertKey(SubjectMatterExpertKey key)
    {
        Assert.notNull(key, "A subject matter expert key must be specified.");
        key.setUserId(alternateKeyHelper.validateStringParameter("user id", key.getUserId()));
    }
}
