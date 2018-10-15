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

import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.ExternalInterfaceDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.ExternalInterface;
import org.finra.herd.model.api.xml.ExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.ExternalInterfaceKey;
import org.finra.herd.model.api.xml.ExternalInterfaceKeys;
import org.finra.herd.model.api.xml.ExternalInterfaceUpdateRequest;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.service.ExternalInterfaceService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.ExternalInterfaceDaoHelper;

/**
 * The external interface service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class ExternalInterfaceServiceImpl implements ExternalInterfaceService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private ExternalInterfaceDao externalInterfaceDao;

    @Autowired
    private ExternalInterfaceDaoHelper externalInterfaceDaoHelper;

    @Override
    public ExternalInterface createExternalInterface(ExternalInterfaceCreateRequest request)
    {
        // Perform the validation.
        validateExternalInterfaceCreateRequest(request);

        // Ensure an external interface with the specified external interface name doesn't already exist.
        ExternalInterfaceEntity externalInterfaceEntity =
            externalInterfaceDao.getExternalInterfaceByName(request.getExternalInterfaceKey().getExternalInterfaceName());
        if (externalInterfaceEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create external interface \"%s\" because it already exists.", request.getExternalInterfaceKey().getExternalInterfaceName()));
        }

        // Create an external interface entity from the request information.
        externalInterfaceEntity = createExternalInterfaceEntity(request);

        // Persist the new entity.
        externalInterfaceEntity = externalInterfaceDao.saveAndRefresh(externalInterfaceEntity);

        // Create and return the external interface object from the persisted entity.
        return createExternalInterfaceFromEntity(externalInterfaceEntity);
    }

    @Override
    public ExternalInterface getExternalInterface(ExternalInterfaceKey externalInterfaceKey)
    {
        // Perform validation and trim.
        validateAndTrimExternalInterfaceKey(externalInterfaceKey);

        // Retrieve and ensure that an external interface already exists with the specified name.
        ExternalInterfaceEntity externalInterfaceEntity =
            externalInterfaceDaoHelper.getExternalInterfaceEntity(externalInterfaceKey.getExternalInterfaceName());

        // Create and return the external interface object from the persisted entity.
        return createExternalInterfaceFromEntity(externalInterfaceEntity);
    }

    @Override
    public ExternalInterface deleteExternalInterface(ExternalInterfaceKey externalInterfaceKey)
    {
        // Perform validation and trim.
        validateAndTrimExternalInterfaceKey(externalInterfaceKey);

        // Retrieve and ensure that an external interface already exists with the specified name.
        ExternalInterfaceEntity externalInterfaceEntity =
            externalInterfaceDaoHelper.getExternalInterfaceEntity(externalInterfaceKey.getExternalInterfaceName());

        // Delete the external interface.
        externalInterfaceDao.delete(externalInterfaceEntity);

        // Create and return the external interface object from the deleted entity.
        return createExternalInterfaceFromEntity(externalInterfaceEntity);
    }

    @Override
    public ExternalInterfaceKeys getExternalInterfaces()
    {
        ExternalInterfaceKeys externalInterfaceKeys = new ExternalInterfaceKeys();
        externalInterfaceKeys.getExternalInterfaceKeys()
            .addAll(externalInterfaceDao.getExternalInterfaces().stream().map(ExternalInterfaceKey::new).collect(Collectors.toList()));
        return externalInterfaceKeys;
    }

    @Override
    public ExternalInterface updateExternalInterface(ExternalInterfaceKey externalInterfaceKey, ExternalInterfaceUpdateRequest request)
    {
        // Perform the validation.
        validateExternalInterfaceUpdateRequest(request);

        // Ensure an external interface with the specified external interface name exists.
        ExternalInterfaceEntity externalInterfaceEntity =
            externalInterfaceDaoHelper.getExternalInterfaceEntity(externalInterfaceKey.getExternalInterfaceName());

        // Update and persist the external interface entity.
        updateExternalInterfaceEntity(externalInterfaceEntity, request);

        // Create and return the external interface object from the persisted entity.
        return createExternalInterfaceFromEntity(externalInterfaceEntity);
    }

    /**
     * Validates the external interface create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    void validateExternalInterfaceCreateRequest(ExternalInterfaceCreateRequest request) throws IllegalArgumentException
    {
        ExternalInterfaceKey externalInterfaceKey = request.getExternalInterfaceKey();
        externalInterfaceKey
            .setExternalInterfaceName(alternateKeyHelper.validateStringParameter("external interface name", externalInterfaceKey.getExternalInterfaceName()));
    }

    /**
     * Validates the external interface update request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    void validateExternalInterfaceUpdateRequest(ExternalInterfaceUpdateRequest request) throws IllegalArgumentException
    {
        Assert.notNull(request, "An external interface update request must be specified.");

        request.setDisplayName(alternateKeyHelper.validateStringParameter("display name", request.getDisplayName()));

        // Verify the description has text, but do not trim.
        Assert.hasText(request.getDescription(), "A description must be specified.");
    }

    /**
     * Validates an external interface key. This method also trims the key parameters.
     *
     * @param externalInterfaceKey the external interface key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    void validateAndTrimExternalInterfaceKey(ExternalInterfaceKey externalInterfaceKey) throws IllegalArgumentException
    {
        Assert.notNull(externalInterfaceKey, "An external interface key must be specified.");
        externalInterfaceKey
            .setExternalInterfaceName(alternateKeyHelper.validateStringParameter("external interface name", externalInterfaceKey.getExternalInterfaceName()));
    }

    /**
     * Creates a new external interface entity from the request information.
     *
     * @param request the request
     *
     * @return the newly created external interface entity
     */
    private ExternalInterfaceEntity createExternalInterfaceEntity(ExternalInterfaceCreateRequest request)
    {
        // Create a new entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();
        externalInterfaceEntity.setCode(request.getExternalInterfaceKey().getExternalInterfaceName());
        externalInterfaceEntity.setDisplayName(request.getDisplayName());
        externalInterfaceEntity.setDescription(request.getDescription());
        return externalInterfaceEntity;
    }

    /**
     * Creates the external interface from the persisted entity.
     *
     * @param externalInterfaceEntity the newly persisted external interface entity.
     *
     * @return the external interface.
     */
    private ExternalInterface createExternalInterfaceFromEntity(ExternalInterfaceEntity externalInterfaceEntity)
    {
        // Create the external interface information.
        ExternalInterface externalInterface = new ExternalInterface();
        externalInterface.setExternalInterfaceKey(new ExternalInterfaceKey(externalInterfaceEntity.getCode()));
        externalInterface.setDisplayName(externalInterfaceEntity.getDisplayName());
        externalInterface.setDescription(externalInterfaceEntity.getDescription());
        return externalInterface;
    }

    /**
     * Updates and persists the external interface per the specified update request.
     *
     * @param externalInterfaceEntity the external interface entity
     * @param request the external interface request
     */
    private void updateExternalInterfaceEntity(ExternalInterfaceEntity externalInterfaceEntity, ExternalInterfaceUpdateRequest request)
    {
        externalInterfaceEntity.setDisplayName(request.getDisplayName());
        externalInterfaceEntity.setDescription(request.getDescription());
        externalInterfaceDao.saveAndRefresh(externalInterfaceEntity);
    }
}
