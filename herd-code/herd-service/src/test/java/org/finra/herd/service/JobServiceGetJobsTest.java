package org.finra.herd.service;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.impl.persistence.entity.HistoricProcessInstanceEntity;
import org.activiti.engine.repository.ProcessDefinition;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.api.xml.JobSummary;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.helper.JobDefinitionHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceSecurityHelper;
import org.finra.herd.service.helper.S3PropertiesLocationHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.impl.JobServiceImpl;

public class JobServiceGetJobsTest extends AbstractServiceTest
{
    @Mock
    private ActivitiService activitiService;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Mock
    private JavaPropertiesHelper javaPropertiesHelper;

    @Mock
    private JobDefinitionDao jobDefinitionDao;

    @Mock
    private JobDefinitionHelper jobDefinitionHelper;

    @InjectMocks
    private JobServiceImpl jobServiceImpl;

    @Mock
    private NamespaceDao namespaceDao;

    @Mock
    private NamespaceDaoHelper namespaceDaoHelper;

    @Mock
    private NamespaceSecurityHelper namespaceSecurityHelper;

    @Mock
    private S3Dao s3Dao;

    @Mock
    private S3PropertiesLocationHelper s3PropertiesLocationHelper;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    /**
     * Returns an argument matcher proxy which matches arguments that matches EqualsCollection
     *
     * @param other The collection to match
     *
     * @return The proxy
     */
    private static <T> Collection<T> equalsCollection(Collection<T> other)
    {
        return argThat(new EqualsCollection<>(other));
    }

    @Before
    public void before()
    {
        initMocks(this);

        when(herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.JOBS_QUERY_MAX_RESULTS)).thenReturn(1);

        when(jobDefinitionHelper.getActivitiJobDefinitionTemplate()).thenReturn("~namespace~.~jobName~");
        when(jobDefinitionHelper.getNamespaceToken()).thenReturn("~namespace~");
        when(jobDefinitionHelper.getJobNameToken()).thenReturn("~jobName~");
    }

    @Test
    public void testGetJobsAssertJobNameTrimmed() throws Exception
    {
        String namespace = "namespace";
        String jobName = StringUtils.wrap("jobName", BLANK_TEXT);
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList("namespace"));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        verify(jobDefinitionDao).getJobDefinitionsByFilter(eq(authorizedNamespaces), eq(jobName.trim()));
    }

    /**
     * Tests case where multiple process instances were created, but the job definition entities were deleted at some point without removing the old historic
     * instances, then a new process instance is created with the same namespace and job name pair. When a ListJobs is called, it should not return the old
     * instance information. <p/> This case was added as a verification to the bug raised by test automation.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsGivenMultipleHistoricProcessInstanceWithSamKeyJobAssertReturnCompletedJob() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList(namespace));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        List<JobDefinitionEntity> jobDefinitionEntities = new ArrayList<>();
        JobDefinitionEntity jobDefinitionEntity1 = new JobDefinitionEntity();
        jobDefinitionEntity1.setActivitiId(namespace + "." + jobName + ":1" + ":1");
        jobDefinitionEntities.add(jobDefinitionEntity1);
        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(jobDefinitionEntities);

        ProcessDefinition processDefinition = mock(ProcessDefinition.class);
        when(processDefinition.getId()).thenReturn("a.b:1:1");
        when(processDefinition.getKey()).thenReturn("a.b");
        when(activitiService.getProcessDefinitionsByIds(any())).thenReturn(asList(processDefinition));

        when(activitiService.getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(any(), any(), any(), any())).thenReturn(1l);
        List<HistoricProcessInstance> historicProcessInstances = new ArrayList<>();
        {
            HistoricProcessInstanceEntity historicProcessInstanceEntity1 = new HistoricProcessInstanceEntity();
            historicProcessInstanceEntity1.setId("historicProcessInstanceEntity1.id");
            historicProcessInstanceEntity1.setProcessDefinitionId("a.b:1:1");
            historicProcessInstanceEntity1.setStartTime(new Date(1234));
            historicProcessInstanceEntity1.setEndTime(new Date(2345));
            historicProcessInstances.add(historicProcessInstanceEntity1);
        }
        {
            HistoricProcessInstanceEntity historicProcessInstanceEntity1 = new HistoricProcessInstanceEntity();
            historicProcessInstanceEntity1.setId("historicProcessInstanceEntity2.id");
            historicProcessInstanceEntity1.setProcessDefinitionId("a.b:1:2");
            historicProcessInstanceEntity1.setStartTime(new Date(1234));
            historicProcessInstanceEntity1.setEndTime(new Date(2345));
            historicProcessInstances.add(historicProcessInstanceEntity1);
        }
        when(activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(any(), any(), any(), any())).thenReturn(historicProcessInstances);

        JobSummaries getJobsResult = jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        List<JobSummary> jobSummaries = getJobsResult.getJobSummaries();
        assertEquals(1, jobSummaries.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsGivenOneCompletedJobAndPassingStartAndEndTimeAssertReturnCompletedJob() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        DateTime startTime = new DateTime(new Date(0123));
        DateTime endTime = new DateTime(new Date(3456));
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList(namespace));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        List<JobDefinitionEntity> jobDefinitionEntities = new ArrayList<>();
        JobDefinitionEntity jobDefinitionEntity1 = new JobDefinitionEntity();
        jobDefinitionEntity1.setActivitiId(namespace + "." + jobName + ":1" + ":1");
        jobDefinitionEntities.add(jobDefinitionEntity1);
        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(jobDefinitionEntities);

        ProcessDefinition processDefinition = mock(ProcessDefinition.class);
        when(processDefinition.getId()).thenReturn("a.b:1:1");
        when(processDefinition.getKey()).thenReturn("a.b");
        when(activitiService.getProcessDefinitionsByIds(any())).thenReturn(asList(processDefinition));

        when(activitiService.getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(any(), any(), any(), any())).thenReturn(1l);
        List<HistoricProcessInstance> historicProcessInstances = new ArrayList<>();
        HistoricProcessInstanceEntity historicProcessInstanceEntity1 = new HistoricProcessInstanceEntity();
        historicProcessInstanceEntity1.setId("historicProcessInstanceEntity1.id");
        historicProcessInstanceEntity1.setProcessDefinitionId("a.b:1:1");
        historicProcessInstanceEntity1.setStartTime(new Date(1234));
        historicProcessInstanceEntity1.setEndTime(new Date(2345));
        historicProcessInstances.add(historicProcessInstanceEntity1);
        when(activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(any(), any(), any(), any())).thenReturn(historicProcessInstances);

        JobSummaries getJobsResult = jobServiceImpl.getJobs(namespace, jobName, jobStatus, startTime, endTime);

        List<JobSummary> jobSummaries = getJobsResult.getJobSummaries();
        assertEquals(1, jobSummaries.size());
        JobSummary jobSummary = jobSummaries.get(0);
        assertEquals(historicProcessInstanceEntity1.getId(), jobSummary.getId());
        assertEquals("a", jobSummary.getNamespace());
        assertEquals("b", jobSummary.getJobName());
        assertEquals(JobStatusEnum.COMPLETED, jobSummary.getStatus());
        assertEquals(historicProcessInstanceEntity1.getStartTime().getTime(), jobSummary.getStartTime().toGregorianCalendar().getTimeInMillis());
        assertEquals(historicProcessInstanceEntity1.getEndTime().getTime(), jobSummary.getEndTime().toGregorianCalendar().getTimeInMillis());
        assertEquals(0, jobSummary.getTotalExceptions());

        verify(activitiService)
            .getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(eq(JobStatusEnum.COMPLETED), any(), eq(startTime), eq(endTime));
        verify(activitiService).getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(eq(JobStatusEnum.COMPLETED), any(), eq(startTime), eq(endTime));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsGivenOneRunningJobAssertReturnCompletedJob() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        JobStatusEnum jobStatus = JobStatusEnum.RUNNING;
        long expectedNumberOfExceptions = 1234l;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList(namespace));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        List<JobDefinitionEntity> jobDefinitionEntities = new ArrayList<>();
        JobDefinitionEntity jobDefinitionEntity1 = new JobDefinitionEntity();
        jobDefinitionEntity1.setActivitiId(namespace + "." + jobName + ":1" + ":1");
        jobDefinitionEntities.add(jobDefinitionEntity1);
        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(jobDefinitionEntities);

        ProcessDefinition processDefinition = mock(ProcessDefinition.class);
        when(processDefinition.getId()).thenReturn("a.b:1:1");
        when(processDefinition.getKey()).thenReturn("a.b");
        when(activitiService.getProcessDefinitionsByIds(any())).thenReturn(asList(processDefinition));

        when(activitiService.getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(any(), any(), any(), any())).thenReturn(1l);
        List<HistoricProcessInstance> historicProcessInstances = new ArrayList<>();
        HistoricProcessInstanceEntity historicProcessInstanceEntity1 = new HistoricProcessInstanceEntity();
        historicProcessInstanceEntity1.setId("historicProcessInstanceEntity1.id");
        historicProcessInstanceEntity1.setProcessDefinitionId("a.b:1:1");
        historicProcessInstanceEntity1.setStartTime(new Date(1234));
        historicProcessInstances.add(historicProcessInstanceEntity1);
        when(activitiService.getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(any(), any(), any(), any())).thenReturn(historicProcessInstances);

        when(activitiService.getJobsWithExceptionCountByProcessInstanceId(any())).thenReturn(expectedNumberOfExceptions);

        JobSummaries getJobsResult = jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        List<JobSummary> jobSummaries = getJobsResult.getJobSummaries();
        assertEquals(1, jobSummaries.size());
        JobSummary jobSummary = jobSummaries.get(0);
        assertEquals(historicProcessInstanceEntity1.getId(), jobSummary.getId());
        assertEquals("a", jobSummary.getNamespace());
        assertEquals("b", jobSummary.getJobName());
        assertEquals(JobStatusEnum.RUNNING, jobSummary.getStatus());
        assertEquals(historicProcessInstanceEntity1.getStartTime().getTime(), jobSummary.getStartTime().toGregorianCalendar().getTimeInMillis());
        assertNull(jobSummary.getEndTime());
        assertEquals(expectedNumberOfExceptions, jobSummary.getTotalExceptions());

        verify(activitiService).getJobsWithExceptionCountByProcessInstanceId(historicProcessInstanceEntity1.getId());
        verify(activitiService).getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(eq(JobStatusEnum.RUNNING), any(), any(), any());
        verify(activitiService).getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(eq(JobStatusEnum.RUNNING), any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsWhenActivitiIdIsNotExpectedFormatAssertSuccess() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        String processDefinitionKey = String.format("%s.%s", namespace, jobName);

        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(new HashSet<>(asList(namespace)));

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        JobDefinitionEntity jobDefinitionEntity = new JobDefinitionEntity();
        jobDefinitionEntity.setNamespace(namespaceEntity);
        jobDefinitionEntity.setName(jobName);
        jobDefinitionEntity.setActivitiId("123456");
        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(asList(jobDefinitionEntity));

        ProcessDefinition processDefinition = mock(ProcessDefinition.class);
        when(processDefinition.getKey()).thenReturn(processDefinitionKey);
        when(activitiService.getProcessDefinitionsByIds(any())).thenReturn(asList(processDefinition));

        jobServiceImpl.getJobs(namespace, jobName, AbstractServiceTest.NO_ACTIVITI_JOB_STATUS, NO_START_TIME, NO_END_TIME);

        verify(activitiService)
            .getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(any(), equalsCollection(asList(processDefinitionKey)), any(), any());
        verify(activitiService)
            .getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(any(), equalsCollection(asList(processDefinitionKey)), any(), any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsWhenJobDaoReturnEmptyAssertReturnEmpty() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList(namespace));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(new ArrayList<>());

        JobSummaries getJobsResult = jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        List<JobSummary> jobSummaries = getJobsResult.getJobSummaries();
        assertEquals(0, jobSummaries.size());
    }

    @Test
    public void testGetJobsWhenJobNameNullAssertQueryByNull() throws Exception
    {
        String namespace = "namespace";
        String jobName = null;
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList("namespace"));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        verify(jobDefinitionDao).getJobDefinitionsByFilter(eq(authorizedNamespaces), isNull(String.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsWhenJobStatusNullQueryIgnoreStatus() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        JobStatusEnum jobStatus = null;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList(namespace));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        List<JobDefinitionEntity> jobDefinitionEntities = new ArrayList<>();
        JobDefinitionEntity jobDefinitionEntity1 = new JobDefinitionEntity();
        jobDefinitionEntity1.setActivitiId(namespace + "." + jobName + ":1" + ":1");
        jobDefinitionEntities.add(jobDefinitionEntity1);
        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(jobDefinitionEntities);

        when(activitiService.getProcessDefinitionsByIds(any())).thenReturn(asList());

        jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        // Assert neither status filter was called on the query
        verify(activitiService).getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(isNull(JobStatusEnum.class), any(), any(), any());
        verify(activitiService).getHistoricProcessInstancesByStatusAndProcessDefinitionKeys(isNull(JobStatusEnum.class), any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsWhenNamespaceNotSpecifiedAssertQueryByAllAuthorizedNamespaces() throws Exception
    {
        String namespace = null;
        String jobName = "jobName";
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList("a", "b"));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(new ArrayList<>());

        jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        verify(jobDefinitionDao).getJobDefinitionsByFilter(eq(authorizedNamespaces), eq(jobName));
    }

    @Test
    public void testGetJobsWhenNamespaceSpecifiedButDoesNotExistAssertResultEmpty() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList(namespace));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        when(namespaceDao.getNamespaceByCd(any())).thenReturn(null);

        JobSummaries getJobsResult = jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        List<JobSummary> jobSummaries = getJobsResult.getJobSummaries();
        assertEquals(0, jobSummaries.size());
    }

    @Test
    public void testGetJobsWhenNamespaceSpecifiedButNotAuthorizedAssertNoQuery() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList("a", "b"));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);

        verify(jobDefinitionDao, times(0)).getJobDefinitionsByFilter(eq(authorizedNamespaces), eq(jobName));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetJobsWhenResultTooLargeAssertThrowException() throws Exception
    {
        String namespace = "namespace";
        String jobName = "jobName";
        JobStatusEnum jobStatus = JobStatusEnum.COMPLETED;

        Set<String> authorizedNamespaces = new HashSet<>(Arrays.asList(namespace));
        when(namespaceSecurityHelper.getAuthorizedNamespaces(any())).thenReturn(authorizedNamespaces);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        when(namespaceDao.getNamespaceByCd(any())).thenReturn(namespaceEntity);

        List<JobDefinitionEntity> jobDefinitionEntities = new ArrayList<>();
        JobDefinitionEntity jobDefinitionEntity1 = new JobDefinitionEntity();
        jobDefinitionEntity1.setActivitiId(namespace + "." + jobName + ":1" + ":1");
        jobDefinitionEntities.add(jobDefinitionEntity1);
        when(jobDefinitionDao.getJobDefinitionsByFilter(any(Collection.class), any())).thenReturn(jobDefinitionEntities);

        when(activitiService.getProcessDefinitionsByIds(any())).thenReturn(asList());

        when(activitiService.getHistoricProcessInstancesCountByStatusAndProcessDefinitionKeys(any(), any(), any(), any())).thenReturn(1000l);

        try
        {
            jobServiceImpl.getJobs(namespace, jobName, jobStatus, NO_START_TIME, NO_END_TIME);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Too many jobs found for the specified filter parameters. The maximum number of results allowed is 1 and the number of results " +
                "returned was 1000.", e.getMessage());
        }
    }

    /**
     * An argument matches which matches when two collections' contents are equal regardless of the type of collection used.
     *
     * @param <T> The type of the collection element
     */
    private static class EqualsCollection<T> extends ArgumentMatcher<Collection<T>>
    {
        private Collection<T> other;

        public EqualsCollection(Collection<T> other)
        {
            this.other = other;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean matches(Object argument)
        {
            Collection<T> collection = (Collection<T>) argument;
            return collection.size() == other.size() && collection.containsAll(other);
        }
    }
}
