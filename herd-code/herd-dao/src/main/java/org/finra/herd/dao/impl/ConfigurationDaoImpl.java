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

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.ConfigurationDao;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.ConfigurationEntity_;

@Repository
public class ConfigurationDaoImpl extends AbstractHerdDao implements ConfigurationDao
{
    @Override
    public ConfigurationEntity getConfigurationByKey(String key)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<ConfigurationEntity> criteria = builder.createQuery(ConfigurationEntity.class);

        // The criteria root is the configuration entity.
        Root<ConfigurationEntity> configurationEntity = criteria.from(ConfigurationEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryCodeRestriction = builder.equal(configurationEntity.get(ConfigurationEntity_.key), key);

        // Add all clauses for the query.
        criteria.select(configurationEntity).where(queryCodeRestriction);

        // Execute the query and return the result.
        return executeSingleResultQuery(criteria, String.format("Found more than one configuration entry with \"%s\" key.", key));
    }
}
