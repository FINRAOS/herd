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

import org.finra.herd.dao.BusinessObjectFormatExternalInterfaceDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterface;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.service.BusinessObjectFormatExternalInterfaceService;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceHelper;
import org.finra.herd.service.helper.ExternalInterfaceDaoHelper;

/**
 * The business object format to external interface mapping service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectFormatExternalInterfaceServiceImpl implements BusinessObjectFormatExternalInterfaceService
{
    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatExternalInterfaceDao businessObjectFormatExternalInterfaceDao;

    @Autowired
    private BusinessObjectFormatExternalInterfaceDaoHelper businessObjectFormatExternalInterfaceDaoHelper;

    @Autowired
    private BusinessObjectFormatExternalInterfaceHelper businessObjectFormatExternalInterfaceHelper;

    @Autowired
    private ExternalInterfaceDaoHelper externalInterfaceDaoHelper;

    @Override
    public BusinessObjectFormatExternalInterface createBusinessObjectFormatExternalInterface(
        BusinessObjectFormatExternalInterfaceCreateRequest businessObjectFormatExternalInterfaceCreateRequest)
    {
        // Validate and trim the business object format to external interface mapping create request.
        businessObjectFormatExternalInterfaceHelper
            .validateAndTrimBusinessObjectFormatExternalInterfaceCreateRequest(businessObjectFormatExternalInterfaceCreateRequest);

        // Get a business object format key from the request. Please note that the key is version-less.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(businessObjectFormatExternalInterfaceCreateRequest.getBusinessObjectFormatExternalInterfaceKey().getNamespace(),
                businessObjectFormatExternalInterfaceCreateRequest.getBusinessObjectFormatExternalInterfaceKey().getBusinessObjectDefinitionName(),
                businessObjectFormatExternalInterfaceCreateRequest.getBusinessObjectFormatExternalInterfaceKey().getBusinessObjectFormatUsage(),
                businessObjectFormatExternalInterfaceCreateRequest.getBusinessObjectFormatExternalInterfaceKey().getBusinessObjectFormatFileType(), null);

        // Get an external interface name from the request.
        String externalInterfaceName =
            businessObjectFormatExternalInterfaceCreateRequest.getBusinessObjectFormatExternalInterfaceKey().getExternalInterfaceName();

        // Retrieve and ensure that a business object format with the specified alternate key values exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Retrieve and ensure that an external interface with the specified name exists.
        ExternalInterfaceEntity externalInterfaceEntity = externalInterfaceDaoHelper.getExternalInterfaceEntity(externalInterfaceName);

        // Ensure a business object format to external interface mapping with the specified parameters doesn't already exist.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity);
        if (businessObjectFormatExternalInterfaceEntity != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create business object format to external interface mapping for \"%s\" namespace, " +
                    "\"%s\" business object definition name, \"%s\" business object format usage, \"%s\" business object format file type, and " +
                    "\"%s\" external interface name because it already exists.", businessObjectFormatKey.getNamespace(),
                businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
                businessObjectFormatKey.getBusinessObjectFormatFileType(), externalInterfaceName));
        }

        // Creates a business object format to external interface mapping entity.
        businessObjectFormatExternalInterfaceEntity = new BusinessObjectFormatExternalInterfaceEntity();
        businessObjectFormatExternalInterfaceEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectFormatExternalInterfaceEntity.setExternalInterface(externalInterfaceEntity);

        // Persist the new entity.
        businessObjectFormatExternalInterfaceDao.saveAndRefresh(businessObjectFormatExternalInterfaceEntity);

        // Create a business object format to external interface mapping object from the entity and return it.
        return businessObjectFormatExternalInterfaceHelper.createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity);
    }

    @Override
    public BusinessObjectFormatExternalInterface deleteBusinessObjectFormatExternalInterface(
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey)
    {
        // Validate and trim the business object format to external interface mapping key.
        businessObjectFormatExternalInterfaceHelper.validateAndTrimBusinessObjectFormatExternalInterfaceKey(businessObjectFormatExternalInterfaceKey);

        // Retrieve and ensure that a business object format to external interface mapping with the specified key exists.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity =
            businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);

        // Delete this business object format to external interface mapping entity.
        businessObjectFormatExternalInterfaceDao.delete(businessObjectFormatExternalInterfaceEntity);

        // Create a business object format to external interface mapping object from the entity and return it.
        return businessObjectFormatExternalInterfaceHelper.createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity);
    }

    @Override
    public BusinessObjectFormatExternalInterface getBusinessObjectFormatExternalInterface(
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey)
    {
        // Validate and trim the business object format to external interface mapping key.
        businessObjectFormatExternalInterfaceHelper.validateAndTrimBusinessObjectFormatExternalInterfaceKey(businessObjectFormatExternalInterfaceKey);

        // Retrieve and ensure that a business object format to external interface mapping with the specified key exists.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity =
            businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);

        // Create a business object format to external interface mapping object from the entity and return it.
        return businessObjectFormatExternalInterfaceHelper.createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity);
    }
}
