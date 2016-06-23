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
package org.finra.herd.model.jpa;

import java.util.ArrayList;
import java.util.Collection;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * An instance of job definition.
 */
@Table(name = JobDefinitionEntity.TABLE_NAME)
@Entity
public class JobDefinitionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "job_dfntn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    /**
     * The parameter name column.
     */
    @Column(name = "name_tx", nullable = false)
    private String name;

    /**
     * The parameter description column.
     */
    @Column(name = "desc_tx", length = 500)
    private String description;

    /**
     * The parameter activiti id column.
     */
    @Column(name = "activiti_id", nullable = false)
    private String activitiId;

    @ManyToOne
    @JoinColumn(name = "name_space_cd", referencedColumnName = "name_space_cd", nullable = false)
    private NamespaceEntity namespace;

    @OneToMany(mappedBy = "jobDefinition", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("name")
    private Collection<JobDefinitionParameterEntity> parameters;

    @Column(name = "s3_prpty_buckt_nm", length = 500, nullable = true)
    private String s3BucketName;

    @Column(name = "s3_prpty_objct_key", length = 500, nullable = true)
    private String s3ObjectKey;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public NamespaceEntity getNamespace()
    {
        return namespace;
    }

    public void setNamespace(NamespaceEntity namespace)
    {
        this.namespace = namespace;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getActivitiId()
    {
        return activitiId;
    }

    public void setActivitiId(String activitiId)
    {
        this.activitiId = activitiId;
    }

    public Collection<JobDefinitionParameterEntity> getParameters()
    {
        return parameters;
    }

    public String getS3BucketName()
    {
        return s3BucketName;
    }

    public void setS3BucketName(String s3BucketName)
    {
        this.s3BucketName = s3BucketName;
    }

    public String getS3ObjectKey()
    {
        return s3ObjectKey;
    }

    public void setS3ObjectKey(String s3ObjectKey)
    {
        this.s3ObjectKey = s3ObjectKey;
    }

    /**
     * Sets the parameters. The specified data will be overlaid on to of any existing parameters with the same name. If a parameter with the same name doesn't
     * exist, a new one will exist. Any existing parameters that aren't specified in the new collection will be deleted.
     *
     * @param parameters the parameters.
     */
    public void setParameters(Collection<JobDefinitionParameterEntity> parameters)
    {
        // Create a new holding list that contains all the parameters we want to persist.
        Collection<JobDefinitionParameterEntity> newParameters = new ArrayList<>();

        // Loop through the new set of parameters.
        for (JobDefinitionParameterEntity parameter : parameters)
        {
            // Add the updated/merged parameter to the new list.
            newParameters.add(updateParameter(parameter));
        }

        // If this is a new set of parameters, just use the newly created list.
        // Otherwise, if an existing set of parameters already exists, clear it out and add the elements of the new holding list to it.
        // If an existing list exists, we need to re-use it (i.e. clear and addAll) since Hibernate already has it associated with the session.
        // If we were to set a new list, Hibernate would throw an exception when trying to persist the new collection.
        if (this.parameters == null)
        {
            this.parameters = newParameters;
        }
        else
        {
            this.parameters.clear();
            this.parameters.addAll(newParameters);
        }
    }

    /**
     * Updates the existing parameter (if any) with the specified parameter data. If an existing parameter exists, it will be updated and returned. If an
     * existing parameter doesn't exist, the specified parameter will be updated and returned.
     *
     * @param parameter the newly specified parameter data to use.
     *
     * @return the overlaid or newly created parameter data with all updates applied.
     */
    private JobDefinitionParameterEntity updateParameter(JobDefinitionParameterEntity parameter)
    {
        // Initialize the parameter entity we want to update to null.
        JobDefinitionParameterEntity parameterToUpdate = null;

        // If we have any existing parameters present, loop through them.
        if (parameters != null)
        {
            for (JobDefinitionParameterEntity existingParameter : parameters)
            {
                // If we have an existing parameter that has the same name as the specified parameter, then use it as the one to update (i.e. we'll overlay
                // the new data on top of it).
                if (existingParameter.getName().equalsIgnoreCase(parameter.getName()))
                {
                    parameterToUpdate = existingParameter;
                    break;
                }
            }
        }

        // If no existing parameter was found to overlay data, then just use the specified parameter which should be newly created by the caller.
        // Also, update the parent job definition to this object to set the back reference.
        if (parameterToUpdate == null)
        {
            parameterToUpdate = parameter;
            parameterToUpdate.setJobDefinition(this);
        }

        // Overlay the data on top of the parameter to update.
        parameterToUpdate.setName(parameter.getName());
        parameterToUpdate.setValue(parameter.getValue());

        // Return the parameter.
        return parameterToUpdate;
    }
}
