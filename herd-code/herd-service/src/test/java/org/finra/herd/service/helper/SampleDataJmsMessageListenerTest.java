package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.BusinessObjectDefinitionService;

public class SampleDataJmsMessageListenerTest extends AbstractServiceTest
{
    @Autowired
    SampleDataJmsMessageListener sampleDataJmsMessageListener;

    @Autowired
    JsonHelper jsonHelper;

    @Autowired
    BusinessObjectDefinitionService businessObjectDefinitionService;


    @Test
    public void testS3Message() throws Exception
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME)));
        
        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        businessObjectDefinitionService.createBusinessObjectDefinition(request);
        String fileName = "test1.csv";
        String filePath = NAMESPACE + "/" + BDEF_NAME + "/" + fileName;
        long fizeSize = 1024L;
        S3Entity s3Entity = new S3Entity(null, null, new S3ObjectEntity(filePath, fizeSize, null, null), null);

        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null));

        S3EventNotification s3EventNotification = new S3EventNotification(records);

        sampleDataJmsMessageListener.processMessage(jsonHelper.objectToJson(s3EventNotification), null);
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey);
        
        List<SampleDataFile> samplDataFiles = Arrays.asList(new SampleDataFile(NAMESPACE + "/" + BDEF_NAME + "/", fileName));
        
        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(updatedBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
            samplDataFiles), updatedBusinessObjectDefinition);
    }
}
