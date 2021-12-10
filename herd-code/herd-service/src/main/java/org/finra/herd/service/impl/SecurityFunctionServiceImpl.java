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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.SecurityFunctionDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.SecurityFunction;
import org.finra.herd.model.api.xml.SecurityFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityFunctionKeys;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.service.SecurityFunctionService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SecurityFunctionDaoHelper;

/**
 * The security function service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SecurityFunctionServiceImpl implements SecurityFunctionService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private SecurityFunctionDao securityFunctionDao;

    @Autowired
    private SecurityFunctionDaoHelper securityFunctionDaoHelper;

    @Override
    public SecurityFunction createSecurityFunction(SecurityFunctionCreateRequest request)
    {
        // Perform the validation.
        validateAndTrimSecurityFunctionCreateRequest(request);

        // Ensure a security function with the specified security function name doesn't already exist.
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDao.getSecurityFunctionByName(request.getSecurityFunctionName());
        if (securityFunctionEntity != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create security function \"%s\" because it already exists.", request.getSecurityFunctionName()));
        }

        // Create a security function entity from the request information.
        securityFunctionEntity = createSecurityFunctionEntity(request);

        // Persist the new entity.
        securityFunctionEntity = securityFunctionDao.saveAndRefresh(securityFunctionEntity);

        // Create and return the security function object from the persisted entity.
        return createSecurityFunctionFromEntity(securityFunctionEntity);
    }

    @Override
    public SecurityFunction deleteSecurityFunction(SecurityFunctionKey securityFunctionKey)
    {
        // Perform validation and trim.
        validateAndTrimSecurityFunctionKey(securityFunctionKey);

        // Retrieve and ensure that a security function already exists with the specified name.
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDaoHelper.getSecurityFunctionEntity(securityFunctionKey.getSecurityFunctionName());

        // Delete the security function.
        securityFunctionDao.delete(securityFunctionEntity);

        // Create and return the security function object from the deleted entity.
        return createSecurityFunctionFromEntity(securityFunctionEntity);
    }

    @Override
    public SecurityFunction getSecurityFunction(SecurityFunctionKey securityFunctionKey)
    {
        // Perform validation and trim.
        validateAndTrimSecurityFunctionKey(securityFunctionKey);

        // Retrieve and ensure that a security function already exists with the specified key.
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDaoHelper.getSecurityFunctionEntity(securityFunctionKey.getSecurityFunctionName());

        // Create and return the security function object from the persisted entity.
        return createSecurityFunctionFromEntity(securityFunctionEntity);
    }

    @Override
    public SecurityFunctionKeys getSecurityFunctions()
    {
        SecurityFunctionKeys securityFunctionKeys = new SecurityFunctionKeys();
        securityFunctionKeys.getSecurityFunctionKeys()
            .addAll(securityFunctionDao.getSecurityFunctions().stream().map(SecurityFunctionKey::new).collect(Collectors.toList()));
        return securityFunctionKeys;
    }

    /**
     * Creates a new security function entity from the request information.
     *
     * @param request the request
     *
     * @return the newly created security function entity
     */
    private SecurityFunctionEntity createSecurityFunctionEntity(SecurityFunctionCreateRequest request)
    {
        // Create a new entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(request.getSecurityFunctionName());
        return securityFunctionEntity;
    }

    /**
     * Creates the security function from the persisted entity.
     *
     * @param securityFunctionEntity the newly persisted security function entity.
     *
     * @return the security function.
     */
    private SecurityFunction createSecurityFunctionFromEntity(SecurityFunctionEntity securityFunctionEntity)
    {
        // Create the security function information.
        SecurityFunction securityFunction = new SecurityFunction();
        securityFunction.setSecurityFunctionName(securityFunctionEntity.getCode());
        return securityFunction;
    }

    /**
     * Validates the security function create request. This method also trims request parameters.
     *
     * @param securityFunctionCreateRequest the security function create request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateAndTrimSecurityFunctionCreateRequest(SecurityFunctionCreateRequest securityFunctionCreateRequest) throws IllegalArgumentException
    {
        Assert.notNull(securityFunctionCreateRequest, "A security function create request must be specified.");
        String securityFunctionName =
            alternateKeyHelper.validateStringParameter("security function name", securityFunctionCreateRequest.getSecurityFunctionName());
        Assert.isTrue(StringUtils.isAsciiPrintable(securityFunctionName), "A security function name must contain only ASCII printable characters.");
        securityFunctionCreateRequest.setSecurityFunctionName(securityFunctionName);
    }

    /**
     * Validates a security function key. This method also trims the key parameters.
     *
     * @param securityFunctionKey the security function key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateAndTrimSecurityFunctionKey(SecurityFunctionKey securityFunctionKey) throws IllegalArgumentException
    {
        Assert.notNull(securityFunctionKey, "A security function key must be specified.");
        securityFunctionKey
            .setSecurityFunctionName(alternateKeyHelper.validateStringParameter("security function name", securityFunctionKey.getSecurityFunctionName()));
    }
}
