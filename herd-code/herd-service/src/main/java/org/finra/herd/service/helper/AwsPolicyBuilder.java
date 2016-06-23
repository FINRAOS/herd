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

import java.util.ArrayList;
import java.util.Arrays;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.S3Actions;

/**
 * A builder for AWS policy objects.
 */
public class AwsPolicyBuilder
{
    private Policy policy;

    public AwsPolicyBuilder()
    {
        policy = new Policy(null, new ArrayList<>());
    }

    /**
     * Returns the policy object.
     *
     * @return The policy
     */
    public Policy build()
    {
        return policy;
    }

    /**
     * Adds a permission to allow the specified actions to the given KMS key id.
     *
     * @param kmsKeyId Full ARN to the kms key
     * @param actions List of actions
     *
     * @return This builder
     */
    @SuppressWarnings("PMD.CloseResource")
    public AwsPolicyBuilder withKms(String kmsKeyId, KmsActions... actions)
    {
        Statement statement = new Statement(Effect.Allow);
        statement.setActions(Arrays.asList(actions));
        statement.setResources(Arrays.asList(new Resource(kmsKeyId)));
        policy.getStatements().add(statement);
        return this;
    }

    /**
     * Adds a permission to allow the specified actions to the given bucket and s3 object key. The permission will allow the given actions only to the specified
     * object key. If object key is null, the permission is applied to the bucket itself.
     *
     * @param bucketName S3 bucket name
     * @param objectKey S3 object key
     * @param actions List of actions to allow
     *
     * @return This builder
     */
    @SuppressWarnings("PMD.CloseResource")
    public AwsPolicyBuilder withS3(String bucketName, String objectKey, S3Actions... actions)
    {
        Statement statement = new Statement(Effect.Allow);
        statement.setActions(Arrays.asList(actions));
        String resource = "arn:aws:s3:::" + bucketName;
        if (objectKey != null)
        {
            resource += "/" + objectKey;
        }
        statement.setResources(Arrays.asList(new Resource(resource)));
        policy.getStatements().add(statement);
        return this;
    }

    /**
     * Adds a permission to allow the specified actions to the given bucket and s3 key prefix. The permissions will allow the given actions to all objects with
     * the given prefix.
     *
     * @param bucketName S3 Bucket name
     * @param prefix S3 Object key prefix
     * @param actions List of actions to allow
     *
     * @return This builder
     */
    public AwsPolicyBuilder withS3Prefix(String bucketName, String prefix, S3Actions... actions)
    {
        return withS3(bucketName, prefix + "/*", actions);
    }
}
