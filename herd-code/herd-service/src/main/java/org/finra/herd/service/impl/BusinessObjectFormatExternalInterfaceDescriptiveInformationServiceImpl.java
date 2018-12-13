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

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformation;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.service.BusinessObjectFormatExternalInterfaceDescriptiveInformationService;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceHelper;
import org.finra.herd.service.helper.ExternalInterfaceDaoHelper;

/**
 * The business object format external interface descriptive information service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectFormatExternalInterfaceDescriptiveInformationServiceImpl
    implements BusinessObjectFormatExternalInterfaceDescriptiveInformationService
{
    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatExternalInterfaceDaoHelper businessObjectFormatExternalInterfaceDaoHelper;

    @Autowired
    private BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper businessObjectFormatExternalInterfaceDescriptiveInformationHelper;

    @Autowired
    private BusinessObjectFormatExternalInterfaceHelper businessObjectFormatExternalInterfaceHelper;

    @Autowired
    private ExternalInterfaceDaoHelper externalInterfaceDaoHelper;


    @Override
    public BusinessObjectFormatExternalInterfaceDescriptiveInformation getBusinessObjectFormatExternalInterfaceDescriptiveInformation(
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey)
    {
        // Validate and trim the business object format external interface key.
        businessObjectFormatExternalInterfaceHelper.validateAndTrimBusinessObjectFormatExternalInterfaceKey(businessObjectFormatExternalInterfaceKey);

        // Ensure that a business object format to external interface mapping with the specified key exists.
        businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);

        // Retrieve and ensure that an external interface with the specified name exists.
        ExternalInterfaceEntity externalInterfaceEntity =
            externalInterfaceDaoHelper.getExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey.getExternalInterfaceName());

        // Get a business object format key from the request. Please note that the key is version-less.
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(businessObjectFormatExternalInterfaceKey.getNamespace(),
            businessObjectFormatExternalInterfaceKey.getBusinessObjectDefinitionName(), businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatUsage(),
            businessObjectFormatExternalInterfaceKey.getBusinessObjectFormatFileType(), null);

        // Retrieve and ensure that a business object format with the specified alternate key values exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Create a business object format external interface descriptive information object from the business object format external interface entity and the
        // external interface entity and return it.
        return businessObjectFormatExternalInterfaceDescriptiveInformationHelper
            .createBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities(businessObjectFormatEntity, externalInterfaceEntity);
    }
}
