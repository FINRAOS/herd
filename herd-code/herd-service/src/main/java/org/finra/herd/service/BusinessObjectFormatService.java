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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributeDefinitionsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatParentsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;

/**
 * The business object definition format.
 */
public interface BusinessObjectFormatService
{
    /**
     * Creates a new business object format.
     *
     * @param businessObjectFormatCreateRequest the business object format create request.
     *
     * @return the created business object format.
     */
    public BusinessObjectFormat createBusinessObjectFormat(BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest);

    /**
     * Updates a business object format.
     *
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectFormatUpdateRequest the business object format update request
     *
     * @return the updated business object format.
     */
    public BusinessObjectFormat updateBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatUpdateRequest businessObjectFormatUpdateRequest);

    /**
     * Gets a business object format for the specified key. This method starts a new transaction.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the business object format
     */
    public BusinessObjectFormat getBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Deletes a business object format.
     *
     * @param businessObjectFormatKey the business object format alternate key
     *
     * @return the business object format that was deleted
     */
    public BusinessObjectFormat deleteBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Gets a list of business object formats for the specified business object definition name.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param latestBusinessObjectFormatVersion specifies is only the latest (maximum) versions of the business object formats are returned
     *
     * @return the list of business object formats.
     */
    public BusinessObjectFormatKeys getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        boolean latestBusinessObjectFormatVersion);

    /**
     * Gets a list of business object formats for the specified business object definition name and business object format usage.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param businessObjectFormatUsage the business object format usage
     * @param latestBusinessObjectFormatVersion specifies is only the latest (maximum) versions of the business object formats are returned
     *
     * @return the list of business object formats.
     */
    public BusinessObjectFormatKeys getBusinessObjectFormatsWithFilters(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        String businessObjectFormatUsage, boolean latestBusinessObjectFormatVersion);

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating a table for the requested business object format. This
     * method starts a new transaction.
     *
     * @param businessObjectFormatDdlRequest the business object format DDL request
     *
     * @return the business object format DDL information
     */
    public BusinessObjectFormatDdl generateBusinessObjectFormatDdl(BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest);

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating tables for a collection of business object formats.
     * This method starts a new transaction.
     *
     * @param businessObjectFormatDdlCollectionRequest the business object format DDL collection request
     *
     * @return the business object format DDL information
     */
    public BusinessObjectFormatDdlCollectionResponse generateBusinessObjectFormatDdlCollection(
        BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest);

    /**
     * Update business object format parents
     *
     * @param businessObjectFormatKey business object format key
     * @param businessObjectFormatParentsUpdateRequest business object format parents update request
     *
     * @return business object format
     */
    public BusinessObjectFormat updateBusinessObjectFormatParents(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatParentsUpdateRequest businessObjectFormatParentsUpdateRequest);

    /**
     * Updates a business object format attributes.
     *
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectFormatAttributesUpdateRequest the business object format attributes update request
     *
     * @return the updated business object format.
     */
    public BusinessObjectFormat updateBusinessObjectFormatAttributes(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatAttributesUpdateRequest businessObjectFormatAttributesUpdateRequest);

    /**
     * Replaces the list of attribute definitions for an existing business object format.
     *
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectFormatAttributeDefinitionsUpdateRequest the business object format attribute definitions update request
     *
     * @return the updated business object format.
     */
    public BusinessObjectFormat updateBusinessObjectFormatAttributeDefinitions(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatAttributeDefinitionsUpdateRequest businessObjectFormatAttributeDefinitionsUpdateRequest);

    /**
     * Updates business object format retention information
     *
     * @param businessObjectFormatKey the business object format alternate key
     * @param businessObjectFormatRetentionInformationUpdateRequest business object format retention information update request
     *
     * @return updated business object format
     */
    public BusinessObjectFormat updateBusinessObjectFormatRetentionInformation(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatRetentionInformationUpdateRequest businessObjectFormatRetentionInformationUpdateRequest);

    /**
     * Updates business object format schema backwards compatible changes
     *
     * @param businessObjectFormatKey the business object format alternate key
     * @param businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest business object format schema backwards compatible changes update request
     *
     * @return updated business object format
     */
    public BusinessObjectFormat updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest);
}
