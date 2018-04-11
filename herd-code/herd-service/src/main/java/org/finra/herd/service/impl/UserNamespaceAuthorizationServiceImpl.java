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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.model.api.xml.UserNamespaceAuthorization;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.UserNamespaceAuthorizations;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.UserNamespaceAuthorizationEntity;
import org.finra.herd.service.CurrentUserService;
import org.finra.herd.service.UserNamespaceAuthorizationService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.UserNamespaceAuthorizationHelper;

/**
 * The user namespace authorization service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class UserNamespaceAuthorizationServiceImpl implements UserNamespaceAuthorizationService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private CurrentUserService currentUserService;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    @Autowired
    private UserNamespaceAuthorizationHelper userNamespaceAuthorizationHelper;

    @NamespacePermission(fields = "#request?.userNamespaceAuthorizationKey?.namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public UserNamespaceAuthorization createUserNamespaceAuthorization(UserNamespaceAuthorizationCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateUserNamespaceAuthorizationCreateRequest(request);

        // Get the user namespace authorization key.
        UserNamespaceAuthorizationKey key = request.getUserNamespaceAuthorizationKey();

        // Ensure a user namespace authorization with the specified name doesn't already exist for the specified namespace.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key);
        if (userNamespaceAuthorizationEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create user namespace authorization with user id \"%s\" and namespace \"%s\" because it already exists.", key.getUserId(),
                    key.getNamespace()));
        }

        // Retrieve and ensure that namespace exists with the specified user namespace authorization namespace code.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(key.getNamespace());

        // Create and persist a new user namespace authorization entity from the request information.
        userNamespaceAuthorizationEntity = createUserNamespaceAuthorizationEntity(key.getUserId(), namespaceEntity, request.getNamespacePermissions());

        // Create and return the user namespace authorization object from the persisted entity.
        return createUserNamespaceAuthorizationFromEntity(userNamespaceAuthorizationEntity);
    }

    @NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public UserNamespaceAuthorization updateUserNamespaceAuthorization(UserNamespaceAuthorizationKey key, UserNamespaceAuthorizationUpdateRequest request)
    {
        // Validate and trim the key.
        validateUserNamespaceAuthorizationKey(key);

        // Validate and trim the request parameters.
        validateUserNamespaceAuthorizationUpdateRequest(request);

        // Retrieve and ensure that a user namespace authorization exists with the specified key.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = getUserNamespaceAuthorizationEntity(key);

        // Get the current user
        UserAuthorizations userAuthorizations = currentUserService.getCurrentUser();

        // If the current user id is equal to the user id in the namespace authorization key
        // and the user namespace authorization entity contains the grant permission
        if (userAuthorizations.getUserId().equalsIgnoreCase(key.getUserId()) && userNamespaceAuthorizationEntity.getGrantPermission())
        {
            // Assert that the request contains the grant namespace permission
            Assert.isTrue(request.getNamespacePermissions().contains(NamespacePermissionEnum.GRANT),
                "Users are not allowed to remove their own GRANT namespace permission." +
                    " Please include the GRANT namespace permission in this request, or have another user remove the GRANT permission.");
        }

        // Update the permissions.
        updateNamespacePermissions(userNamespaceAuthorizationEntity, request.getNamespacePermissions());
        userNamespaceAuthorizationDao.saveAndRefresh(userNamespaceAuthorizationEntity);

        // Create and return the user namespace authorization object from the updated entity.
        return createUserNamespaceAuthorizationFromEntity(userNamespaceAuthorizationEntity);
    }

    @NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public UserNamespaceAuthorization getUserNamespaceAuthorization(UserNamespaceAuthorizationKey key)
    {
        // Validate and trim the key.
        validateUserNamespaceAuthorizationKey(key);

        // Retrieve and ensure that a user namespace authorization exists with the specified key.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = getUserNamespaceAuthorizationEntity(key);

        // Create and return the user namespace authorization object from the persisted entity.
        return createUserNamespaceAuthorizationFromEntity(userNamespaceAuthorizationEntity);
    }

    @NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public UserNamespaceAuthorization deleteUserNamespaceAuthorization(UserNamespaceAuthorizationKey key)
    {
        // Validate and trim the key.
        validateUserNamespaceAuthorizationKey(key);

        // Retrieve and ensure that a user namespace authorization exists with the specified key.
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = getUserNamespaceAuthorizationEntity(key);

        // Delete the business object definition.
        userNamespaceAuthorizationDao.delete(userNamespaceAuthorizationEntity);

        // Create and return the user namespace authorization object from the deleted entity.
        return createUserNamespaceAuthorizationFromEntity(userNamespaceAuthorizationEntity);
    }

    @Override
    public UserNamespaceAuthorizations getUserNamespaceAuthorizationsByUserId(String userId)
    {
        // Validate and trim the user id.
        Assert.hasText(userId, "A user id must be specified.");
        String userIdLocal = userId.trim();

        // Retrieve and return a list of user namespace authorization entities for the specified user id.
        List<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntities =
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByUserId(userIdLocal);

        // Create and populate the user namespace authorizations object from the returned entities.
        UserNamespaceAuthorizations userNamespaceAuthorizations = new UserNamespaceAuthorizations();
        userNamespaceAuthorizations.getUserNamespaceAuthorizations().addAll(createUserNamespaceAuthorizationsFromEntities(userNamespaceAuthorizationEntities));

        return userNamespaceAuthorizations;
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public UserNamespaceAuthorizations getUserNamespaceAuthorizationsByNamespace(String namespace)
    {
        // Validate and trim the namespace code.
        Assert.hasText(namespace, "A namespace must be specified.");
        String namespaceLocal = namespace.trim();

        // Validate that specified namespace exists.
        namespaceDaoHelper.getNamespaceEntity(namespaceLocal);

        // Retrieve and return a list of user namespace authorization entities for the specified namespace.
        List<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntities =
            userNamespaceAuthorizationDao.getUserNamespaceAuthorizationsByNamespace(namespaceLocal);

        // Create and populate the user namespace authorizations object from the returned entities.
        UserNamespaceAuthorizations userNamespaceAuthorizations = new UserNamespaceAuthorizations();
        userNamespaceAuthorizations.getUserNamespaceAuthorizations().addAll(createUserNamespaceAuthorizationsFromEntities(userNamespaceAuthorizationEntities));

        return userNamespaceAuthorizations;
    }

    /**
     * Validates the user namespace authorization create request. This method also trims the request parameters.
     *
     * @param request the user namespace authorization create request
     */
    private void validateUserNamespaceAuthorizationCreateRequest(UserNamespaceAuthorizationCreateRequest request)
    {
        Assert.notNull(request, "A user namespace authorization create request must be specified.");

        validateUserNamespaceAuthorizationKey(request.getUserNamespaceAuthorizationKey());
        validateNamespacePermissions(request.getNamespacePermissions());
    }

    /**
     * Validates the user namespace authorization update request. This method also trims the request parameters.
     *
     * @param request the user namespace authorization update request
     */
    private void validateUserNamespaceAuthorizationUpdateRequest(UserNamespaceAuthorizationUpdateRequest request)
    {
        Assert.notNull(request, "A user namespace authorization update request must be specified.");

        validateNamespacePermissions(request.getNamespacePermissions());
    }

    /**
     * Validates the user namespace authorization key. This method also trims the key parameters.
     *
     * @param key the user namespace authorization key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateUserNamespaceAuthorizationKey(UserNamespaceAuthorizationKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A user namespace authorization key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setUserId(alternateKeyHelper.validateStringParameter("user id", key.getUserId()));
    }

    /**
     * Validates a list of namespace permissions.
     *
     * @param namespacePermissions the list of namespace permissions
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateNamespacePermissions(List<NamespacePermissionEnum> namespacePermissions) throws IllegalArgumentException
    {
        Assert.isTrue(!CollectionUtils.isEmpty(namespacePermissions), "Namespace permissions must be specified.");

        // Ensure permission isn't a duplicate by using a hash set with uppercase permission values for case insensitivity.
        Set<NamespacePermissionEnum> validatedNamespacePermissions = new HashSet<>();

        // Validate the permissions.
        for (NamespacePermissionEnum namespacePermission : namespacePermissions)
        {
            // Fail if duplicate permission value is detected.
            if (validatedNamespacePermissions.contains(namespacePermission))
            {
                throw new IllegalArgumentException(String.format("Duplicate namespace permission \"%s\" is found.", namespacePermission.value()));
            }

            validatedNamespacePermissions.add(namespacePermission);
        }
    }

    /**
     * Creates and persists a new user namespace authorization entity.
     *
     * @param userId the user id
     * @param namespaceEntity the namespace entity
     * @param namespacePermissions the list of namespace permissions
     *
     * @return the newly created user namespace authorization entity
     */
    private UserNamespaceAuthorizationEntity createUserNamespaceAuthorizationEntity(String userId, NamespaceEntity namespaceEntity,
        List<NamespacePermissionEnum> namespacePermissions)
    {
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = new UserNamespaceAuthorizationEntity();

        userNamespaceAuthorizationEntity.setUserId(userId);
        userNamespaceAuthorizationEntity.setNamespace(namespaceEntity);
        updateNamespacePermissions(userNamespaceAuthorizationEntity, namespacePermissions);

        return userNamespaceAuthorizationDao.saveAndRefresh(userNamespaceAuthorizationEntity);
    }

    /**
     * Sets relative flags on the user namespace authorization entity as per specified list of namespace permissions.
     *
     * @param userNamespaceAuthorizationEntity the user namespace authorization entity
     * @param namespacePermissions the list of namespace permissions
     */
    private void updateNamespacePermissions(UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity,
        List<NamespacePermissionEnum> namespacePermissions)
    {
        userNamespaceAuthorizationEntity.setReadPermission(namespacePermissions.contains(NamespacePermissionEnum.READ));
        userNamespaceAuthorizationEntity.setWritePermission(namespacePermissions.contains(NamespacePermissionEnum.WRITE));
        userNamespaceAuthorizationEntity.setExecutePermission(namespacePermissions.contains(NamespacePermissionEnum.EXECUTE));
        userNamespaceAuthorizationEntity.setGrantPermission(namespacePermissions.contains(NamespacePermissionEnum.GRANT));
        userNamespaceAuthorizationEntity.setWriteDescriptiveContentPermission(namespacePermissions.contains(NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT));
    }

    /**
     * Creates a list of user namespace authorizations from the list of persisted entities.
     *
     * @param userNamespaceAuthorizationEntities the list of user namespace authorization entities
     *
     * @return the list of user namespace authorizations
     */
    private List<UserNamespaceAuthorization> createUserNamespaceAuthorizationsFromEntities(
        List<UserNamespaceAuthorizationEntity> userNamespaceAuthorizationEntities)
    {
        List<UserNamespaceAuthorization> userNamespaceAuthorizations = new ArrayList<>();

        for (UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity : userNamespaceAuthorizationEntities)
        {
            userNamespaceAuthorizations.add(createUserNamespaceAuthorizationFromEntity(userNamespaceAuthorizationEntity));
        }

        return userNamespaceAuthorizations;
    }

    /**
     * Creates the user namespace authorization from the persisted entity.
     *
     * @param userNamespaceAuthorizationEntity the user namespace authorization entity
     *
     * @return the user namespace authorization
     */
    private UserNamespaceAuthorization createUserNamespaceAuthorizationFromEntity(UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity)
    {
        UserNamespaceAuthorization userNamespaceAuthorization = new UserNamespaceAuthorization();

        userNamespaceAuthorization.setId(userNamespaceAuthorizationEntity.getId());

        UserNamespaceAuthorizationKey userNamespaceAuthorizationKey = new UserNamespaceAuthorizationKey();
        userNamespaceAuthorization.setUserNamespaceAuthorizationKey(userNamespaceAuthorizationKey);
        userNamespaceAuthorizationKey.setUserId(userNamespaceAuthorizationEntity.getUserId());
        userNamespaceAuthorizationKey.setNamespace(userNamespaceAuthorizationEntity.getNamespace().getCode());

        userNamespaceAuthorization.setNamespacePermissions(userNamespaceAuthorizationHelper.getNamespacePermissions(userNamespaceAuthorizationEntity));

        return userNamespaceAuthorization;
    }

    /**
     * Gets a storage entity based on the storage name and makes sure that it exists.
     *
     * @param key the user namespace authorization key (case insensitive)
     *
     * @return the user namespace authorization entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the user namespace authorization entity doesn't exist
     */
    private UserNamespaceAuthorizationEntity getUserNamespaceAuthorizationEntity(UserNamespaceAuthorizationKey key) throws ObjectNotFoundException
    {
        UserNamespaceAuthorizationEntity userNamespaceAuthorizationEntity = userNamespaceAuthorizationDao.getUserNamespaceAuthorizationByKey(key);

        if (userNamespaceAuthorizationEntity == null)
        {
            throw new ObjectNotFoundException(
                String.format("User namespace authorization with user id \"%s\" and namespace \"%s\" doesn't exist.", key.getUserId(), key.getNamespace()));
        }

        return userNamespaceAuthorizationEntity;
    }
}
