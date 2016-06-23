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

import org.finra.herd.dao.FileTypeDao;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.FileTypeEntity_;

@Repository
public class FileTypeDaoImpl extends AbstractHerdDao implements FileTypeDao
{
    @Override
    public FileTypeEntity getFileTypeByCode(String code)
    {
        // Create the criteria builder and the criteria.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<FileTypeEntity> criteria = builder.createQuery(FileTypeEntity.class);

        // The criteria root is the file types.
        Root<FileTypeEntity> fileType = criteria.from(FileTypeEntity.class);

        // Create the standard restrictions (i.e. the standard where clauses).
        Predicate fileTypeCodeRestriction = builder.equal(builder.upper(fileType.get(FileTypeEntity_.code)), code.toUpperCase());

        criteria.select(fileType).where(fileTypeCodeRestriction);

        return executeSingleResultQuery(criteria, String.format("Found more than one file type with code \"%s\".", code));
    }

    @Override
    public List<FileTypeKey> getFileTypes()
    {
        // Create the criteria builder and a tuple style criteria query.
        CriteriaBuilder builder = entityManager.getCriteriaBuilder();
        CriteriaQuery<String> criteria = builder.createQuery(String.class);

        // The criteria root is the file type.
        Root<FileTypeEntity> fileTypeEntity = criteria.from(FileTypeEntity.class);

        // Get the columns.
        Path<String> fileTypeCodeColumn = fileTypeEntity.get(FileTypeEntity_.code);

        // Add the select clause.
        criteria.select(fileTypeCodeColumn);

        // Add the order by clause.
        criteria.orderBy(builder.asc(fileTypeCodeColumn));

        // Run the query to get a list of file type codes back.
        List<String> fileTypeCodes = entityManager.createQuery(criteria).getResultList();

        // Populate the "keys" objects from the returned file type codes.
        List<FileTypeKey> fileTypeKeys = new ArrayList<>();
        for (String fileTypeCode : fileTypeCodes)
        {
            fileTypeKeys.add(new FileTypeKey(fileTypeCode));
        }

        return fileTypeKeys;
    }
}
