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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * A DTO that holds various parameters for making an S3 file/directory transfer request.
 * <p/>
 * Consider using the builder to make constructing this class easier. For example:
 * <p/>
 * <pre>
 * S3FileTransferRequestParamsDto params = S3FileTransferRequestParamsDto
 *     .builder().s3BucketName(&quot;myBucket&quot;).s3KeyPrefix(&quot;myS3KeyPrefix&quot;).build();
 * </pre>
 */
/*
 * Need to suppress this warning because PMD complains there are too many public methods in this class.
 * In reality, they are just getters and setters, and there is a similar builder inner class which contributes to the public count.
 * These public methods do not contribute to the complexity of this class.
 */
@SuppressWarnings("PMD.ExcessivePublicCount")
public class S3FileTransferRequestParamsDto extends AwsParamsDto
{
    /**
     * The optional S3 endpoint to use when making S3 service calls.
     */
    private String s3Endpoint;

    /**
     * The S3 bucket name.
     */
    private String s3BucketName;

    /**
     * The S3 key prefix for copying to or from.
     */
    private String s3KeyPrefix;

    /**
     * The local file path (file or directory as appropriate).
     */
    private String localPath;

    /**
     * A list of files to upload relative to the local path for upload or S3 key prefix for download. In any case, when we specify the file list, the local path
     * should be a directory.
     */
    private List<File> files;

    /**
     * For directory copies, this determines if the copy will recurse into subdirectories.
     */
    private Boolean isRecursive;

    /**
     * This flag determines if S3 reduced redundancy storage will be used when copying to S3 (when supported).
     */
    private Boolean useRrs;

    /**
     * The S3 access key used for S3 authentication.
     */
    private String s3AccessKey;

    /**
     * The S3 secret key used for S3 authentication.
     */
    private String s3SecretKey;

    /**
     * The maximum number of threads to use for file copying.
     */
    private Integer maxThreads;

    /**
     * The KMS id to use for server side encryption.
     */
    private String kmsKeyId;

    /**
     * The socket timeout in milliseconds. 0 means no timeout.
     */
    private Integer socketTimeout;

    /**
     * Any additional AWS credentials providers the S3 operation should use to get credentials.
     */
    private List<HerdAWSCredentialsProvider> additionalAwsCredentialsProviders = new ArrayList<>();

    public String getS3Endpoint()
    {
        return s3Endpoint;
    }

    public void setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = s3Endpoint;
    }

    public String getS3BucketName()
    {
        return s3BucketName;
    }

    public void setS3BucketName(String s3BucketName)
    {
        this.s3BucketName = s3BucketName;
    }

    public String getS3KeyPrefix()
    {
        return s3KeyPrefix;
    }

    public void setS3KeyPrefix(String s3KeyPrefix)
    {
        this.s3KeyPrefix = s3KeyPrefix;
    }

    public String getLocalPath()
    {
        return localPath;
    }

    public void setLocalPath(String localPath)
    {
        this.localPath = localPath;
    }

    public List<File> getFiles()
    {
        return this.files;
    }

    public void setFiles(List<File> files)
    {
        this.files = files;
    }

    public Boolean getRecursive()
    {
        return isRecursive;
    }

    public void setRecursive(Boolean recursive)
    {
        isRecursive = recursive;
    }

    public Boolean getUseRrs()
    {
        return useRrs;
    }

    public void setUseRrs(Boolean useRrs)
    {
        this.useRrs = useRrs;
    }

    public String getS3AccessKey()
    {
        return s3AccessKey;
    }

    public void setS3AccessKey(String s3AccessKey)
    {
        this.s3AccessKey = s3AccessKey;
    }

    public String getS3SecretKey()
    {
        return s3SecretKey;
    }

    public void setS3SecretKey(String s3SecretKey)
    {
        this.s3SecretKey = s3SecretKey;
    }

    public Integer getMaxThreads()
    {
        return maxThreads;
    }

    public void setMaxThreads(Integer maxThreads)
    {
        this.maxThreads = maxThreads;
    }

    public String getKmsKeyId()
    {
        return kmsKeyId;
    }

    public void setKmsKeyId(String kmsKeyId)
    {
        this.kmsKeyId = kmsKeyId;
    }

    public Integer getSocketTimeout()
    {
        return socketTimeout;
    }

    public void setSocketTimeout(Integer socketTimeout)
    {
        this.socketTimeout = socketTimeout;
    }

    public List<HerdAWSCredentialsProvider> getAdditionalAwsCredentialsProviders()
    {
        return additionalAwsCredentialsProviders;
    }

    public void setAdditionalAwsCredentialsProviders(List<HerdAWSCredentialsProvider> additionalAwsCredentialsProviders)
    {
        this.additionalAwsCredentialsProviders = additionalAwsCredentialsProviders;
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
        private S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();

        public Builder s3Endpoint(String s3Endpoint)
        {
            params.setS3Endpoint(s3Endpoint);
            return this;
        }

        public Builder s3BucketName(String s3BucketName)
        {
            params.setS3BucketName(s3BucketName);
            return this;
        }

        public Builder s3KeyPrefix(String s3KeyPrefix)
        {
            params.setS3KeyPrefix(s3KeyPrefix);
            return this;
        }

        public Builder localPath(String localPath)
        {
            params.setLocalPath(localPath);
            return this;
        }

        public Builder files(List<File> files)
        {
            params.setFiles(files);
            return this;
        }

        public Builder recursive(Boolean recursive)
        {
            params.setRecursive(recursive);
            return this;
        }

        public Builder useRrs(Boolean useRrs)
        {
            params.setUseRrs(useRrs);
            return this;
        }

        public Builder s3AccessKey(String s3AccessKey)
        {
            params.setS3AccessKey(s3AccessKey);
            return this;
        }

        public Builder s3SecretKey(String s3SecretKey)
        {
            params.setS3SecretKey(s3SecretKey);
            return this;
        }

        public Builder maxThreads(Integer maxThreads)
        {
            params.setMaxThreads(maxThreads);
            return this;
        }

        public Builder httpProxyHost(String httpProxyHost)
        {
            params.setHttpProxyHost(httpProxyHost);
            return this;
        }

        public Builder httpProxyPort(Integer httpProxyPort)
        {
            params.setHttpProxyPort(httpProxyPort);
            return this;
        }

        public Builder kmsKeyId(String kmsKeyId)
        {
            params.setKmsKeyId(kmsKeyId);
            return this;
        }

        public Builder socketTimeout(Integer socketTimeout)
        {
            params.setSocketTimeout(socketTimeout);
            return this;
        }

        public Builder additionalAwsCredentialsProviders(List<HerdAWSCredentialsProvider> additionalAwsCredentialsProviders)
        {
            params.setAdditionalAwsCredentialsProviders(additionalAwsCredentialsProviders);
            return this;
        }

        public S3FileTransferRequestParamsDto build()
        {
            return params;
        }
    }
    
    @Override
    public int hashCode()
    {
        int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((additionalAwsCredentialsProviders == null) ? 0 : additionalAwsCredentialsProviders.hashCode());
        result = prime * result + ((files == null) ? 0 : files.hashCode());
        result = prime * result + ((isRecursive == null) ? 0 : isRecursive.hashCode());
        result = prime * result + ((kmsKeyId == null) ? 0 : kmsKeyId.hashCode());
        result = prime * result + ((localPath == null) ? 0 : localPath.hashCode());
        result = prime * result + ((maxThreads == null) ? 0 : maxThreads.hashCode());
        result = prime * result + ((s3AccessKey == null) ? 0 : s3AccessKey.hashCode());
        result = prime * result + ((s3BucketName == null) ? 0 : s3BucketName.hashCode());
        result = prime * result + ((s3Endpoint == null) ? 0 : s3Endpoint.hashCode());
        result = prime * result + ((s3KeyPrefix == null) ? 0 : s3KeyPrefix.hashCode());
        result = prime * result + ((s3SecretKey == null) ? 0 : s3SecretKey.hashCode());
        result = prime * result + ((socketTimeout == null) ? 0 : socketTimeout.hashCode());
        result = prime * result + ((useRrs == null) ? 0 : useRrs.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!super.equals(obj))
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
           
        S3FileTransferRequestParamsDto other = (S3FileTransferRequestParamsDto) obj;
        
        return new EqualsBuilder()
        .appendSuper(super.equals(obj))
        .append(additionalAwsCredentialsProviders, other.additionalAwsCredentialsProviders)
        .append(files, other.files)
        .append(localPath, other.localPath)
        .append(maxThreads, other.maxThreads)
        .append(s3AccessKey, other.s3AccessKey)
        .append(s3BucketName, other.s3BucketName)
        .append(s3Endpoint, other.s3Endpoint)
        .append(s3KeyPrefix, other.s3KeyPrefix)
        .append(s3SecretKey, other.s3SecretKey)
        .append(socketTimeout, other.socketTimeout)
        .append(useRrs, other.useRrs)
        .isEquals();
    }

}