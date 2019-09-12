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
package org.finra.herd.tools.access.validator;

import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_REGION_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_ROLE_ARN_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.AWS_SQS_QUEUE_URL_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_BASE_URL_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_PASSWORD_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.HERD_USERNAME_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.NAMESPACE_PROPERTY;
import static org.finra.herd.tools.access.validator.PropertiesHelper.PRIMARY_PARTITION_VALUE_PROPERTY;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.sdk.api.ApplicationApi;
import org.finra.herd.sdk.api.BusinessObjectDataApi;
import org.finra.herd.sdk.api.CurrentUserApi;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.model.BuildInformation;
import org.finra.herd.sdk.model.BusinessObjectData;
import org.finra.herd.sdk.model.UserAuthorizations;

/**
 * A helper class that wraps calls to Herd API clients.
 */
@Component
@EnableRetry
class HerdApiClientOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HerdApiClientOperations.class);
    private static final String SEARCH_KEYWORD_1 = "businessObjectDataKey";
    private static final String SEARCH_KEYWORD_2 = "BUS_OBJCT_DATA_STTS_CHG";
    private static final String JSON_KEY = "Message";
    private static final int MAX_NUM_MESSAGES = 10;
    private static final int WAIT_TIME_SECS = 1;
    private static final int VISIBILITY_TIMEOUT_SECS = 0;

    /**
     * Gets the build information.
     *
     * @param applicationApi the application API client
     *
     * @return BuildInformation
     * @throws ApiException if fails to make API call
     */
    BuildInformation applicationGetBuildInfo(ApplicationApi applicationApi) throws ApiException
    {
        return applicationApi.applicationGetBuildInfo();
    }

    /**
     * Retrieves existing business object data entry information.
     *
     * @param businessObjectDataApi the business object data API client
     * @param namespace the namespace (required)
     * @param businessObjectDefinitionName the business object definition name (required)
     * @param businessObjectFormatUsage the business object format usage (required)
     * @param businessObjectFormatFileType the business object format file type (required)
     * @param partitionKey the partition key of the business object format. When specified, the partition key is validated against the partition key associated
     * with the relative business object format (optional)
     * @param partitionValue the partition value of the business object data (optional)
     * @param subPartitionValues the list of sub-partition values delimited by \&quot;|\&quot; (delimiter can be escaped by \&quot;\\\&quot;) (optional)
     * @param businessObjectFormatVersion the version of the business object format. When the business object format version is not specified, the business
     * object data with the latest business format version available for the specified partition values is returned (optional)
     * @param businessObjectDataVersion the version of the business object data. When business object data version is not specified, the latest version of
     * business object data of the specified business object data status is returned (optional)
     * @param businessObjectDataStatus the status of the business object data. When business object data version is specified, this parameter is ignored.
     * Default value is \&quot;VALID\&quot; (optional)
     * @param includeBusinessObjectDataStatusHistory specifies to include business object data status history in the response (optional)
     * @param includeStorageUnitStatusHistory specifies to include storage unit status history for each storage unit in the response (optional)
     *
     * @return BusinessObjectData
     * @throws ApiException if fails to make API call
     */
    BusinessObjectData businessObjectDataGetBusinessObjectData(BusinessObjectDataApi businessObjectDataApi, String namespace,
        String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType, String partitionKey, String partitionValue,
        String subPartitionValues, Integer businessObjectFormatVersion, Integer businessObjectDataVersion, String businessObjectDataStatus,
        Boolean includeBusinessObjectDataStatusHistory, Boolean includeStorageUnitStatusHistory) throws ApiException
    {
        return businessObjectDataApi
            .businessObjectDataGetBusinessObjectData(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                partitionKey, partitionValue, subPartitionValues, businessObjectFormatVersion, businessObjectDataVersion, businessObjectDataStatus,
                includeBusinessObjectDataStatusHistory, includeStorageUnitStatusHistory);
    }

    /**
     * Gets BusinessObjectDataKey from SQS message
     *
     * @param client the AWS SQS client
     * @param queueUrl the AWS SQS queue url
     *
     * @return BusinessObjectDataKey
     * @throws IOException if fails to retrieve BusinessObjectDataKey from SQS message
     * @throws ApiException if fails to make API call
     */
    @Retryable(value = ApiException.class, backoff = @Backoff(delay = 2000, multiplier = 2))
    BusinessObjectDataKey getBdataKeySqs(AmazonSQS client, String queueUrl) throws IOException, ApiException
    {
        ReceiveMessageRequest receiveMessageRequest =
            new ReceiveMessageRequest().withMaxNumberOfMessages(MAX_NUM_MESSAGES).withQueueUrl(queueUrl).withWaitTimeSeconds(WAIT_TIME_SECS)
                .withVisibilityTimeout(VISIBILITY_TIMEOUT_SECS);

        LOGGER.info("Checking queue");
        ReceiveMessageResult receiveMessageResult = client.receiveMessage(receiveMessageRequest);
        if (receiveMessageResult != null && receiveMessageResult.getMessages() != null && receiveMessageResult.getMessages().size() > 0)
        {
            List<Message> sqsMessageList = receiveMessageResult.getMessages();

            LOGGER.info("Scanning {} messages for {} and {}", sqsMessageList.size(), SEARCH_KEYWORD_1, SEARCH_KEYWORD_2);
            // Get message type BUS_OBJCT_DATA_STTS_CHG
            for (Message sqsMessage : sqsMessageList)
            {
                String receivedMessageBody = sqsMessage.getBody();
                if (receivedMessageBody.contains(SEARCH_KEYWORD_1) && receivedMessageBody.contains(SEARCH_KEYWORD_2))
                {
                    LOGGER.info("Received Message: {}", receivedMessageBody);
                    return mapJsontoBdataKey(receivedMessageBody).getBusinessObjectDataKey();
                }
            }
        }
        throw new ApiException("No SQS message found in queue: " + queueUrl);
    }

    /**
     * Maps SQS message to JsonSqsMessageBody.class
     *
     * @param messageBody the SQS message body
     *
     * @return BusinessObjectDataKey
     * @throws IOException if fails to map message to JsonSqsMessageBody.class
     */
    JsonSqsMessageBody mapJsontoBdataKey(String messageBody) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        return mapper.readValue(new JSONObject(messageBody).getString(JSON_KEY), JsonSqsMessageBody.class);
    }

    /**
     * Checks the property file for missing properties
     *
     * @param propertiesHelper the loaded properties file
     * @param messageFlag the message flag to read SQS message
     *
     * @throws ApiException if a required parameter is missing
     */
    void checkPropertiesFile(PropertiesHelper propertiesHelper, Boolean messageFlag) throws ApiException
    {
        List<String> requiredProperties =
            Arrays.asList(HERD_BASE_URL_PROPERTY, HERD_USERNAME_PROPERTY, HERD_PASSWORD_PROPERTY, AWS_ROLE_ARN_PROPERTY, AWS_REGION_PROPERTY);
        Boolean missingProperty = false;
        StringBuilder errorMessage = new StringBuilder();

        for (String property : requiredProperties)
        {
            if (propertiesHelper.isBlankOrNull(property))
            {
                missingProperty = true;
                errorMessage.append(property);
                errorMessage.append('\n');
            }
        }

        if (messageFlag)
        {
            if (propertiesHelper.isBlankOrNull(AWS_SQS_QUEUE_URL_PROPERTY))
            {
                missingProperty = true;
                errorMessage.append(AWS_SQS_QUEUE_URL_PROPERTY);
                errorMessage.append('\n');
            }
        }
        else
        {
            requiredProperties = Arrays.asList(NAMESPACE_PROPERTY, BUSINESS_OBJECT_DEFINITION_NAME_PROPERTY, BUSINESS_OBJECT_FORMAT_USAGE_PROPERTY,
                BUSINESS_OBJECT_FORMAT_FILE_TYPE_PROPERTY, PRIMARY_PARTITION_VALUE_PROPERTY);

            for (String property : requiredProperties)
            {
                if (propertiesHelper.isBlankOrNull(property))
                {
                    missingProperty = true;
                    errorMessage.append(property);
                    errorMessage.append('\n');
                }
            }
        }

        if (missingProperty)
        {
            throw new ApiException("The following properties are missing. Please double check case and spelling:\n" + errorMessage.toString());
        }
    }

    /**
     * Gets all authorizations for the current user.
     *
     * @param currentUserApi the CurrentUser API client
     *
     * @return UserAuthorizations
     * @throws ApiException if fails to make API call
     */
    UserAuthorizations currentUserGetCurrentUser(CurrentUserApi currentUserApi) throws ApiException
    {
        return currentUserApi.currentUserGetCurrentUser();
    }
}
