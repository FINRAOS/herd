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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.TagTypeDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.TagTypeEntity;

/**
 * Helper for tag type related operations which require DAO.
 */
@Component
public class TagTypeDaoHelper
{
    @Autowired
    private TagTypeDao tagTypeDao;

    /**
     * Gets a tag type entity and ensure it exists.
     *
     * @param tagTypeCode the tag type (case insensitive)
     *
     * @return the tag type entity
     * @throws ObjectNotFoundException if the tag type entity doesn't exist
     */
    public TagTypeEntity getTagTypeEntity(String tagTypeCode) throws ObjectNotFoundException
    {
        TagTypeEntity tagTypeEntity = tagTypeDao.getTagTypeByCd(tagTypeCode);

        if (tagTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Tag type with code \"%s\" doesn't exist.", tagTypeCode));
        }

        return tagTypeEntity;
    }

    /**
     * Gets a tag type entity by its display name and ensure it doesn't already exist.
     *
     * @param displayName the tag display name (case insensitive)
     *
     * @throws AlreadyExistsException if the tag type entity already exists
     */
    public void assertTagTypeDisplayNameDoesNotExist(String displayName) throws AlreadyExistsException
    {
        TagTypeEntity tagTypeEntity = tagTypeDao.getTagTypeByDisplayName(displayName);

        if (tagTypeEntity != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create tag type with display name \"%s\" because it already exists.", displayName));
        }
    }
}
