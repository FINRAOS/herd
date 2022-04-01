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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NamespaceSearchFilter;
import org.finra.herd.model.api.xml.NamespaceSearchKey;
import org.finra.herd.model.api.xml.NamespaceSearchRequest;
import org.finra.herd.model.api.xml.NamespaceSearchResponse;
import org.finra.herd.model.api.xml.NamespaceUpdateRequest;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.NamespaceService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.SearchableService;

/**
 * The namespace service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class NamespaceServiceImpl implements NamespaceService, SearchableService
{
    // Constant to hold the charge code field option for the search response.
    public final static String CHARGE_CODE_FIELD = "chargeCode".toLowerCase();

    // Constant to hold the s3 key prefix field option for the search response.
    public final static String S3_KEY_PREFIX_FIELD = "s3KeyPrefix".toLowerCase();

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceHelper namespaceHelper;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Override
    public Namespace createNamespace(NamespaceCreateRequest request)
    {
        // Validation and trim the request.
        validateNamespaceCreateRequest(request);

        // Get the namespace key.
        NamespaceKey namespaceKey = new NamespaceKey(request.getNamespaceCode());

        // Ensure a namespace with the specified namespace key doesn't already exist.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByKey(namespaceKey);
        if (namespaceEntity != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create namespace \"%s\" because it already exists.", namespaceKey.getNamespaceCode()));
        }

        // Create a namespace entity from the request information.
        namespaceEntity = createNamespaceEntity(request);

        // Persist the new entity.
        namespaceEntity = namespaceDao.saveAndRefresh(namespaceEntity);

        // Create and return the namespace object from the persisted entity.
        return createNamespaceFromEntity(namespaceEntity);
    }

    @Override
    public Namespace getNamespace(NamespaceKey namespaceKey)
    {
        // Perform validation and trim.
        namespaceHelper.validateNamespaceKey(namespaceKey);

        // Retrieve and ensure that a namespace already exists with the specified key.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespaceKey.getNamespaceCode());

        // Create and return the namespace object from the persisted entity.
        return createNamespaceFromEntity(namespaceEntity);
    }

    @Override
    public Namespace deleteNamespace(NamespaceKey namespaceKey)
    {
        // Perform validation and trim.
        namespaceHelper.validateNamespaceKey(namespaceKey);

        // Retrieve and ensure that a namespace already exists with the specified key.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespaceKey.getNamespaceCode());

        // Delete the namespace.
        namespaceDao.delete(namespaceEntity);

        // Create and return the namespace object from the deleted entity.
        return createNamespaceFromEntity(namespaceEntity);
    }

    @Override
    public NamespaceKeys getNamespaces()
    {
        NamespaceKeys namespaceKeys = new NamespaceKeys();
        namespaceKeys.getNamespaceKeys().addAll(namespaceDao.getNamespaceKeys());
        return namespaceKeys;
    }

    @NamespacePermission(fields = "#namespaceKey.namespaceCode", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public Namespace updateNamespaces(NamespaceKey namespaceKey, NamespaceUpdateRequest namespaceUpdateRequest)
    {
        // Perform validation and trim for namespaceCode.
        namespaceHelper.validateNamespaceKey(namespaceKey);

        // Perform validation and trim for chargeCode.
        validateAndTrimNamespaceUpdateRequest(namespaceUpdateRequest);

        // Retrieve and ensure that a namespace already exists with the specified key.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespaceKey.getNamespaceCode());

        // Update the namespace entity from the request information.
        namespaceEntity.setChargeCode(namespaceUpdateRequest.getChargeCode());

        // Save the entity.
        namespaceEntity = namespaceDao.saveAndRefresh(namespaceEntity);

        // Create and return the namespace object from the updated entity.
        return createNamespaceFromEntity(namespaceEntity);
    }

    @Override
    public NamespaceSearchResponse searchNamespaces(NamespaceSearchRequest namespaceSearchRequest, Set<String> fields)
    {
        // Validate and trim the request parameters.
        validateAndTrimNamespaceSearchRequest(namespaceSearchRequest);

        // Validate and trim the search response fields.
        validateSearchResponseFields(fields);

        List<NamespaceEntity> namespaceEntities = new ArrayList<>();

        // If search key is specified, use it to retrieve the namespaces.
        if (CollectionUtils.isNotEmpty(namespaceSearchRequest.getNamespaceSearchFilters()) && namespaceSearchRequest.getNamespaceSearchFilters().get(0) != null)
        {
            // Get the namespace search key.
            NamespaceSearchKey namespaceSearchKey = namespaceSearchRequest.getNamespaceSearchFilters().get(0).getNamespaceSearchKeys().get(0);

            // If charge code is specified, retrieve all namespaces by that charge code.
            if (StringUtils.isNotBlank(namespaceSearchKey.getChargeCode()))
            {
                namespaceEntities.addAll(namespaceDao.getNamespacesByChargeCode(namespaceSearchKey.getChargeCode()));
            }
            // The charge code is not specified, so select all namespaces registered in the system.
            else
            {
                // Retrieve all the namespaces.
                List<NamespaceEntity> allNamespaceEntities = namespaceDao.getNamespaces();

                // If mustHaveChargeCode flag is set to true, filter out namespaces without charge code from result.
                if (BooleanUtils.isTrue(namespaceSearchKey.isMustHaveChargeCode()))
                {
                    for (NamespaceEntity namespaceEntity : allNamespaceEntities)
                    {
                        if (StringUtils.isNotBlank(namespaceEntity.getChargeCode()))
                        {
                            namespaceEntities.add(namespaceEntity);
                        }
                    }
                }
                // Add all namespaces to result.
                else
                {
                    namespaceEntities.addAll(allNamespaceEntities);
                }
            }
        }
        // The search key is not specified, so select all namespaces registered in the system.
        else
        {
            namespaceEntities.addAll(namespaceDao.getNamespaces());
        }

        // Build the list of namespaces.
        List<Namespace> namespaces = new ArrayList<>();
        for (NamespaceEntity namespaceEntity : namespaceEntities)
        {
            namespaces.add(createNamespaceFromEntity(namespaceEntity, fields.contains(CHARGE_CODE_FIELD), fields.contains(S3_KEY_PREFIX_FIELD)));
        }

        // Build and return the namespace search response.
        return new NamespaceSearchResponse(namespaces);
    }

    /**
     * Validates the namespace create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateNamespaceCreateRequest(NamespaceCreateRequest request)
    {
        request.setNamespaceCode(alternateKeyHelper.validateStringParameter("namespace", request.getNamespaceCode()));

        if (request.getChargeCode() != null)
        {
            request.setChargeCode(request.getChargeCode().trim());
        }
    }

    /**
     * Creates a new namespace entity from the request information.
     *
     * @param request the request
     *
     * @return the newly created namespace entity
     */
    private NamespaceEntity createNamespaceEntity(NamespaceCreateRequest request)
    {
        // Create a new entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(request.getNamespaceCode());
        namespaceEntity.setChargeCode(request.getChargeCode());
        return namespaceEntity;
    }

    /**
     * Creates the namespace from the entity.
     *
     * @param namespaceEntity the namespace entity
     *
     * @return the namespace
     */
    private Namespace createNamespaceFromEntity(NamespaceEntity namespaceEntity)
    {
        // Create the namespace information.
        return createNamespaceFromEntity(namespaceEntity, true, true);
    }

    /**
     * Creates the namespace from the entity.
     *
     * @param namespaceEntity    the namespace entity
     * @param includeChargeCode  specifies to include charge code element
     * @param includeS3KeyPrefix specifies to include the s3 key prefix
     *
     * @return the namespace
     */
    private Namespace createNamespaceFromEntity(NamespaceEntity namespaceEntity, boolean includeChargeCode, boolean includeS3KeyPrefix)
    {

        Namespace namespace = new Namespace();

        namespace.setNamespaceCode(namespaceEntity.getCode());

        if (includeChargeCode)
        {
            namespace.setChargeCode(namespaceEntity.getChargeCode());
        }

        if (includeS3KeyPrefix)
        {
            namespace.setS3KeyPrefix(s3KeyPrefixHelper.s3KeyPrefixFormat(namespaceEntity.getCode()));
        }

        return namespace;
    }

    /**
     * Validates the namespace update request. This method also trims request parameters.
     *
     * @param namespaceUpdateRequest the namespace update request
     */
    private void validateAndTrimNamespaceUpdateRequest(NamespaceUpdateRequest namespaceUpdateRequest)
    {
        if (namespaceUpdateRequest.getChargeCode() != null)
        {
            namespaceUpdateRequest.setChargeCode(namespaceUpdateRequest.getChargeCode().trim());
        }
    }

    /**
     * Validate the namespace search request. This method also trims the request parameters.
     *
     * @param namespaceSearchRequest the namespace search request
     */
    private void validateAndTrimNamespaceSearchRequest(NamespaceSearchRequest namespaceSearchRequest)
    {
        // Validate namespace search request is specified.
        Assert.notNull(namespaceSearchRequest, "A namespace search request must be specified.");

        // Continue validation if the list of namespace search filters is not empty.
        if (CollectionUtils.isNotEmpty(namespaceSearchRequest.getNamespaceSearchFilters()) && namespaceSearchRequest.getNamespaceSearchFilters().get(0) != null)
        {
            // Validate that there is only one namespace search filter.
            Assert.isTrue(CollectionUtils.size(namespaceSearchRequest.getNamespaceSearchFilters()) == 1,
                "At most one namespace search filter must be specified.");

            // Get the namespace search filter.
            NamespaceSearchFilter namespaceSearchFilter = namespaceSearchRequest.getNamespaceSearchFilters().get(0);

            // Validate that exactly one namespace search key is specified.
            Assert.isTrue(
                CollectionUtils.size(namespaceSearchFilter.getNamespaceSearchKeys()) == 1 && namespaceSearchFilter.getNamespaceSearchKeys().get(0) != null,
                "Exactly one namespace search key must be specified.");

            // Get the namespace search key.
            NamespaceSearchKey namespaceSearchKey = namespaceSearchFilter.getNamespaceSearchKeys().get(0);

            // Trim charge code value.
            if (namespaceSearchKey.getChargeCode() != null)
            {
                namespaceSearchKey.setChargeCode(namespaceSearchKey.getChargeCode().trim());
            }
        }
    }

    @Override
    public Set<String> getValidSearchResponseFields()
    {
        return ImmutableSet.of(CHARGE_CODE_FIELD, S3_KEY_PREFIX_FIELD);
    }
}
