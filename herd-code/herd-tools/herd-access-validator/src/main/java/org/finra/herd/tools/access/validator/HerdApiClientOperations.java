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

import org.springframework.stereotype.Component;

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
class HerdApiClientOperations
{
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
