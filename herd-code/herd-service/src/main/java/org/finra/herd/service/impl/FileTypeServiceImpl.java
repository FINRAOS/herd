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

import org.finra.herd.dao.FileTypeDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.FileType;
import org.finra.herd.model.api.xml.FileTypeCreateRequest;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.FileTypeKeys;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.service.FileTypeService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.FileTypeDaoHelper;

/**
 * The file type service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class FileTypeServiceImpl implements FileTypeService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private FileTypeDaoHelper fileTypeDaoHelper;

    @Autowired
    private FileTypeDao fileTypeDao;

    @Override
    public FileType createFileType(FileTypeCreateRequest fileTypeCreateRequest)
    {
        // Perform the validation.
        validateFileTypeCreateRequest(fileTypeCreateRequest);

        // Ensure a file type with the specified file type code doesn't already exist.
        FileTypeEntity fileTypeEntity = fileTypeDao.getFileTypeByCode(fileTypeCreateRequest.getFileTypeCode());
        if (fileTypeEntity != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create file type \"%s\" because it already exists.", fileTypeCreateRequest.getFileTypeCode()));
        }

        // Create a file type entity from the request information.
        fileTypeEntity = createFileTypeEntity(fileTypeCreateRequest);

        // Persist the new entity.
        fileTypeEntity = fileTypeDao.saveAndRefresh(fileTypeEntity);

        // Create and return the file type object from the persisted entity.
        return createFileTypeFromEntity(fileTypeEntity);
    }

    @Override
    public FileType getFileType(FileTypeKey fileTypeKey)
    {
        // Perform validation and trim.
        validateAndTrimFileTypeKey(fileTypeKey);

        // Retrieve and ensure that a file type already exists with the specified key.
        FileTypeEntity fileTypeEntity = fileTypeDaoHelper.getFileTypeEntity(fileTypeKey.getFileTypeCode());

        // Create and return the file type object from the persisted entity.
        return createFileTypeFromEntity(fileTypeEntity);
    }

    @Override
    public FileType deleteFileType(FileTypeKey fileTypeKey)
    {
        // Perform validation and trim.
        validateAndTrimFileTypeKey(fileTypeKey);

        // Retrieve and ensure that a file type already exists with the specified key.
        FileTypeEntity fileTypeEntity = fileTypeDaoHelper.getFileTypeEntity(fileTypeKey.getFileTypeCode());

        // Delete the file type
        fileTypeDao.delete(fileTypeEntity);

        // Create and return the file type object from the persisted entity.
        return createFileTypeFromEntity(fileTypeEntity);
    }

    @Override
    public FileTypeKeys getFileTypes()
    {
        FileTypeKeys fileTypeKeys = new FileTypeKeys();
        fileTypeKeys.getFileTypeKeys().addAll(fileTypeDao.getFileTypes());
        return fileTypeKeys;
    }

    /**
     * Validates the file type create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    void validateFileTypeCreateRequest(FileTypeCreateRequest request) throws IllegalArgumentException
    {
        request.setFileTypeCode(alternateKeyHelper.validateStringParameter("file type code", request.getFileTypeCode()));
    }

    /**
     * Validates a file type key. This method also trims the key parameters.
     *
     * @param fileTypeKey the file type key
     *
     * @return the the trimmed file type code
     * @throws IllegalArgumentException if any validation errors were found
     */
    void validateAndTrimFileTypeKey(FileTypeKey fileTypeKey) throws IllegalArgumentException
    {
        Assert.notNull(fileTypeKey, "A file type key must be specified.");
        fileTypeKey.setFileTypeCode(alternateKeyHelper.validateStringParameter("file type code", fileTypeKey.getFileTypeCode()));
    }

    /**
     * Creates a new file type entity from the request information.
     *
     * @param request the request
     *
     * @return the newly created file type entity
     */
    private FileTypeEntity createFileTypeEntity(FileTypeCreateRequest request)
    {
        // Create a new entity.
        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(request.getFileTypeCode());
        return fileTypeEntity;
    }

    /**
     * Creates the file type from the persisted entity.
     *
     * @param fileTypeEntity the newly persisted file type entity.
     *
     * @return the file type.
     */
    private FileType createFileTypeFromEntity(FileTypeEntity fileTypeEntity)
    {
        // Create the file type information.
        return new FileType(fileTypeEntity.getCode());
    }
}
