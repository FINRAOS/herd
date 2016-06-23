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
import java.util.Date;
import java.util.List;

import javax.persistence.Tuple;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.springframework.stereotype.Repository;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.StorageUploadStatsDao;
import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStat;
import org.finra.herd.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.herd.model.api.xml.StorageDailyUploadStat;
import org.finra.herd.model.api.xml.StorageDailyUploadStats;
import org.finra.herd.model.dto.DateRangeDto;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.model.jpa.StorageFileViewEntity;
import org.finra.herd.model.jpa.StorageFileViewEntity_;

@Repository
public class StorageUploadStatsDaoImpl extends AbstractHerdDao implements StorageUploadStatsDao
{
    @Override
    public StorageDailyUploadStats getStorageUploadStats(StorageAlternateKeyDto storageAlternateKey, DateRangeDto dateRange)
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        Root<StorageFileViewEntity> storageFileViewEntity = criteria.from(StorageFileViewEntity.class);

        Path<Date> createdDate = storageFileViewEntity.get(StorageFileViewEntity_.createdDate);

        Expression<Long> totalFilesExpression = builder.count(storageFileViewEntity.get(StorageFileViewEntity_.storageFileId));
        Expression<Long> totalBytesExpression = builder.sum(storageFileViewEntity.get(StorageFileViewEntity_.fileSizeInBytes));

        Predicate storageNameRestriction = builder.equal(builder.upper(storageFileViewEntity.get(StorageFileViewEntity_.storageCode)), storageAlternateKey
            .getStorageName().toUpperCase());
        Predicate createDateRestriction = builder.and(builder.greaterThanOrEqualTo(createdDate, dateRange.getLowerDate()), builder.lessThanOrEqualTo(
            createdDate, dateRange.getUpperDate()));

        criteria.multiselect(createdDate, totalFilesExpression, totalBytesExpression);
        criteria.where(builder.and(storageNameRestriction, createDateRestriction));

        List<Expression<?>> grouping = new ArrayList<>();
        grouping.add(createdDate);

        criteria.groupBy(grouping);

        criteria.orderBy(builder.asc(createdDate));

        // Retrieve and return the storage upload statistics.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();
        StorageDailyUploadStats uploadStats = new StorageDailyUploadStats();

        for (Tuple tuple : tuples)
        {
            StorageDailyUploadStat uploadStat = new StorageDailyUploadStat();
            uploadStats.getStorageDailyUploadStats().add(uploadStat);
            uploadStat.setUploadDate(HerdDateUtils.getXMLGregorianCalendarValue(tuple.get(createdDate)));
            uploadStat.setTotalFiles(tuple.get(totalFilesExpression));
            uploadStat.setTotalBytes(tuple.get(totalBytesExpression));
        }

        return uploadStats;
    }

    @Override
    public StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinition(StorageAlternateKeyDto storageAlternateKey,
        DateRangeDto dateRange)
    {
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Tuple> criteria = builder.createTupleQuery();

        Root<StorageFileViewEntity> storageFileViewEntity = criteria.from(StorageFileViewEntity.class);

        Path<String> namespaceCode = storageFileViewEntity.get(StorageFileViewEntity_.namespaceCode);
        Path<String> dataProviderCode = storageFileViewEntity.get(StorageFileViewEntity_.dataProviderCode);
        Path<String> businessObjectDefinitionName = storageFileViewEntity.get(StorageFileViewEntity_.businessObjectDefinitionName);
        Path<Date> createdDate = storageFileViewEntity.get(StorageFileViewEntity_.createdDate);
        Expression<Long> totalFilesExpression = builder.count(storageFileViewEntity.get(StorageFileViewEntity_.storageFileId));
        Expression<Long> totalBytesExpression = builder.sum(storageFileViewEntity.get(StorageFileViewEntity_.fileSizeInBytes));

        Predicate storageNameRestriction = builder.equal(builder.upper(storageFileViewEntity.get(StorageFileViewEntity_.storageCode)), storageAlternateKey
            .getStorageName().toUpperCase());
        Predicate createDateRestriction = builder.and(builder.greaterThanOrEqualTo(createdDate, dateRange.getLowerDate()), builder.lessThanOrEqualTo(
            createdDate, dateRange.getUpperDate()));

        criteria.multiselect(createdDate, namespaceCode, dataProviderCode, businessObjectDefinitionName, totalFilesExpression, totalBytesExpression);
        criteria.where(builder.and(storageNameRestriction, createDateRestriction));

        // Create the group by clause.
        List<Expression<?>> grouping = new ArrayList<>();
        grouping.add(createdDate);
        grouping.add(namespaceCode);
        grouping.add(dataProviderCode);
        grouping.add(businessObjectDefinitionName);
        criteria.groupBy(grouping);

        // Create the order by clause.
        criteria.orderBy(builder.asc(createdDate), builder.asc(namespaceCode), builder.asc(dataProviderCode), builder.asc(businessObjectDefinitionName));

        // Retrieve and return the storage upload statistics.
        List<Tuple> tuples = entityManager.createQuery(criteria).getResultList();
        StorageBusinessObjectDefinitionDailyUploadStats uploadStats = new StorageBusinessObjectDefinitionDailyUploadStats();

        for (Tuple tuple : tuples)
        {
            StorageBusinessObjectDefinitionDailyUploadStat uploadStat = new StorageBusinessObjectDefinitionDailyUploadStat();
            uploadStats.getStorageBusinessObjectDefinitionDailyUploadStats().add(uploadStat);
            uploadStat.setUploadDate(HerdDateUtils.getXMLGregorianCalendarValue(tuple.get(createdDate)));
            uploadStat.setNamespace(tuple.get(namespaceCode));
            uploadStat.setDataProviderName(tuple.get(dataProviderCode));
            uploadStat.setBusinessObjectDefinitionName(tuple.get(businessObjectDefinitionName));
            uploadStat.setTotalFiles(tuple.get(totalFilesExpression));
            uploadStat.setTotalBytes(tuple.get(totalBytesExpression));
        }

        return uploadStats;
    }
}
