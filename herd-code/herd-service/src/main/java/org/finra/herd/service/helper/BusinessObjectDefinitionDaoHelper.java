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

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSampleDataFileEntity;
import org.finra.herd.model.jpa.StorageEntity;

/**
 * Helper for data provider related operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionDaoHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;
    
    @Autowired
    private StorageDaoHelper storageDaoHelper;
    
    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;
    
    /**
     * Retrieves a business object definition entity by it's key and ensure it exists.
     *
     * @param businessObjectDefinitionKey the business object definition name (case-insensitive)
     *
     * @return the business object definition entity
     * @throws ObjectNotFoundException if the business object definition entity doesn't exist
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionEntity(BusinessObjectDefinitionKey businessObjectDefinitionKey)
        throws ObjectNotFoundException
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);

        if (businessObjectDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));
        }

        return businessObjectDefinitionEntity;
    }
    
    /**
     * Update business object definition sample files
     * @param businessObjectDefinitionKey business object definition key
     * @param fileName new file name
     * @param fileSize file size
     * @throws ObjectNotFoundException objection not found exception
     */
    @Transactional
    public void updatedBusinessObjectDefinitionEntitySampleFiles(BusinessObjectDefinitionKey businessObjectDefinitionKey, String fileName, long fileSize)
            throws ObjectNotFoundException
      {
        //save path from the input parameter
        String path = businessObjectDefinitionKey.getNamespace() + "/" + businessObjectDefinitionKey.getBusinessObjectDefinitionName() + "/";
        //validate business object key
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);
        //validate file name
        Assert.hasText(fileName, "A file name must be specified.");
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        Collection<BusinessObjectDefinitionSampleDataFileEntity> sampleFiles = businessObjectDefinitionEntity.getSampleDataFiles();
        boolean found = false;
        for (BusinessObjectDefinitionSampleDataFileEntity sampleDataFieEntity: sampleFiles)
        {
           if (sampleDataFieEntity.getFileName().equals(fileName))
           {
               found = true;
               //update the file size if they are different
               if (sampleDataFieEntity.getFileSizeBytes() != fileSize)
               {
                   sampleDataFieEntity.setFileSizeBytes(fileSize);
                   businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
               }              
               break;
           }
        }      
        //create a new entity when not found
        if (!found)
        {
            StorageEntity storageEntity = storageDaoHelper.getStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE);            
            BusinessObjectDefinitionSampleDataFileEntity sampleDataFileEntity = new BusinessObjectDefinitionSampleDataFileEntity();
            sampleDataFileEntity.setStorage(storageEntity);
            sampleDataFileEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
            sampleDataFileEntity.setDirectoryPath(path);
            sampleDataFileEntity.setFileName(fileName);
            sampleDataFileEntity.setFileSizeBytes(fileSize);
            businessObjectDefinitionEntity.getSampleDataFiles().add(sampleDataFileEntity);
            businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
        }   
      }
}
