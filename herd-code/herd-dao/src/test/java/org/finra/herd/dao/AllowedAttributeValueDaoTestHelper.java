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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.jpa.AllowedAttributeValueEntity;
import org.finra.herd.model.jpa.AttributeValueListEntity;

@Component
public class AllowedAttributeValueDaoTestHelper
{
    @Autowired
    private AllowedAttributeValueDao allowedAttributeValueDao;

    @Autowired
    private AttributeValueListDao attributeValueListDao;

    /**
     * Returns a list of test allowed attribute value entities.
     *
     * @return the list of allowed attribute value entities
     */
    public List<AllowedAttributeValueEntity> createAllowedAttributeValueEntities(AttributeValueListKey attributeValueListKey,
        List<String> allowedAttributeValues)
    {
        AttributeValueListEntity attributeValueListEntity = attributeValueListDao.getAttributeValueListByKey(attributeValueListKey);
        if (attributeValueListEntity == null)
        {
            attributeValueListEntity = attributeValueListDao.saveAndRefresh(attributeValueListEntity);
        }

        List<AllowedAttributeValueEntity> allowedAttributeValueEntities = new ArrayList<>();
        for (String allowedAttributeValue : allowedAttributeValues)
        {
            AllowedAttributeValueEntity allowedAttributeValueEntity = new AllowedAttributeValueEntity();
            allowedAttributeValueEntity.setAttributeValueList(attributeValueListEntity);
            allowedAttributeValueEntity.setAllowedAttributeValue(allowedAttributeValue);
            allowedAttributeValueEntities.add(allowedAttributeValueDao.saveAndRefresh(allowedAttributeValueEntity));
        }
        return allowedAttributeValueEntities;
    }

    /**
     * Returns an unsorted list of test allowed attribute values.
     *
     * @return the unsorted list of allowed attribute values
     */
    public List<String> getTestUnsortedAllowedAttributeValues()
    {
        return Arrays.asList("1456", "5634", "9876", "3457", "5679", "9098", "1678");
    }

}
