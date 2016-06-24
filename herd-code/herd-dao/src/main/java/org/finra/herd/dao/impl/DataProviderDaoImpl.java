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

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.dao.DataProviderDao;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.DataProviderEntity_;

@Repository
public class DataProviderDaoImpl extends AbstractHerdDao implements DataProviderDao
{
    @Override
    public DataProviderEntity getDataProviderByKey(DataProviderKey dataProviderKey)
    {
        return getDataProviderByName(dataProviderKey.getDataProviderName());
    }

    @Override
    public DataProviderEntity getDataProviderByName(String dataProviderName)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<DataProviderEntity> criteria = builder.createQuery(DataProviderEntity.class);

        // The criteria root is the data provider.
        Root<DataProviderEntity> dataProviderEntity = criteria.from(DataProviderEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate queryRestriction = builder.equal(builder.upper(dataProviderEntity.get(DataProviderEntity_.name)), dataProviderName.toUpperCase());

        criteria.select(dataProviderEntity).where(queryRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one data provider with name=\"%s\".", dataProviderName));
    }

    @Override
    public List<DataProviderKey> getDataProviders()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the data provider.
        Root<DataProviderEntity> dataProviderEntity = criteria.from(DataProviderEntity.class);

        // Get the columns.
        Path<String> dataProviderNameColumn = dataProviderEntity.get(DataProviderEntity_.name);

        // Add the select clause.
        criteria.select(dataProviderNameColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(dataProviderNameColumn));

        // Run the query to get a list of data provider names back.
        List<String> dataProviderNames = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned data provider names.
        List<DataProviderKey> dataProviderKeys = new ArrayList<>();
        for (String dataProviderName : dataProviderNames)
        {
            dataProviderKeys.add(new DataProviderKey(dataProviderName));
        }

        return dataProviderKeys;
    }
}
