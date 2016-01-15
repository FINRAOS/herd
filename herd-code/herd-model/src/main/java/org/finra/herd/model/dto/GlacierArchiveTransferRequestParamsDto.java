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
package org.finra.herd.model.dto;

/**
 * A DTO that holds various parameters for making a Glacier archive transfer request.
 * <p/>
 * Consider using the builder to make constructing this class easier. For example:
 * <p/>
 * <pre>
 * GlacierArchiveTransferRequestParamsDto params = GlacierFileTransferRequestParamsDto
 *     .builder().vaultName(&quot;myVault&quot;).glacierArchiveId(&quot;myGlacierArchiveId&quot;).build();
 * </pre>
 */
public class GlacierArchiveTransferRequestParamsDto extends AwsParamsDto
{
    /**
     * The optional AWS Glacier endpoint to use when making AWS Glacier service calls.
     */
    private String glacierEndpoint;

    /**
     * The Glacier vault name.
     */
    private String vaultName;

    /**
     * The local file path.
     */
    private String localFilePath;

    public String getGlacierEndpoint()
    {
        return glacierEndpoint;
    }

    public void setGlacierEndpoint(String glacierEndpoint)
    {
        this.glacierEndpoint = glacierEndpoint;
    }

    public String getVaultName()
    {
        return vaultName;
    }

    public void setVaultName(String vaultName)
    {
        this.vaultName = vaultName;
    }

    public String getLocalFilePath()
    {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath)
    {
        this.localFilePath = localFilePath;
    }

    /**
     * Returns a builder that can easily build this DTO.
     *
     * @return the builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A builder that makes it easier to construct this DTO.
     */
    public static class Builder
    {
        private GlacierArchiveTransferRequestParamsDto params = new GlacierArchiveTransferRequestParamsDto();

        public Builder glacierEndpoint(String glacierEndpoint)
        {
            params.setGlacierEndpoint(glacierEndpoint);
            return this;
        }

        public Builder vaultName(String vaultName)
        {
            params.setVaultName(vaultName);
            return this;
        }

        public Builder localFilePath(String localFilePath)
        {
            params.setLocalFilePath(localFilePath);
            return this;
        }

        public GlacierArchiveTransferRequestParamsDto build()
        {
            return params;
        }
    }
}
