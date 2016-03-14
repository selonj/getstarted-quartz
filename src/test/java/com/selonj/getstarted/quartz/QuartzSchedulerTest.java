package com.selonj.getstarted.quartz;

import com.selonj.getstarted.quartz.supports.QuartzJobMonitor;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.impl.StdSchedulerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.hasEntry;
import static org.quartz.DateBuilder.IntervalUnit.MILLISECOND;
import static org.quartz.DateBuilder.futureDate;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Created by Administrator on 2016-03-11.
 */
public class QuartzSchedulerTest {

  private Scheduler quartz;
  private QuartzJobMonitor monitor = new QuartzJobMonitor();

  @Before
  public void start() throws Exception {
    quartz = StdSchedulerFactory.getDefaultScheduler();
    quartz.setJobFactory(monitor);
    quartz.start();
  }

  @After
  public void shutdown() throws Exception {
    quartz.shutdown();
  }

  @Test
  public void createNewJobInstanceEveryTime() throws Exception {

    monitor.assertCountOfCreatedJobInstancesEqual(0);

    quartz.scheduleJob(monitor.aJob().build(), newTrigger().build());
    monitor.assertCountOfCreatedJobInstancesEqual(1);

    quartz.scheduleJob(monitor.aJob().build(), newTrigger().build());
    monitor.assertCountOfCreatedJobInstancesEqual(2);
  }

  @Test
  public void executeJobRightNow() throws Exception {
    quartz.scheduleJob(monitor.aJob().build(), newTrigger().build());

    monitor.assertJobExecutedMatchingUtil(100, MILLISECONDS);
  }

  @Test
  public void executeJobLater() throws Exception {
    Date now = new Date();
    Date start = futureDate(500, MILLISECOND);
    long duration = start.getTime() - now.getTime();

    quartz.scheduleJob(monitor.aJob().build(), newTrigger().startAt(start).build());

    monitor.assertJobNotExecutedAfter(duration - 200, MILLISECONDS);
    monitor.assertJobExecutedMatchingUtil(201, MILLISECONDS);
  }

  @Test
  public void executeIntervalJobRepeat() throws Exception {
    long intervalMillis = 100;
    long timeout = intervalMillis + 50;
    int repeatCount = 2;

    SimpleScheduleBuilder scheduler =
        simpleSchedule().withIntervalInMilliseconds(intervalMillis).withRepeatCount(
            repeatCount);

    quartz.scheduleJob(monitor.aJob().build(), newTrigger().withSchedule(scheduler).build());

    monitor.assertJobExecutedMatchingUtil(timeout, MILLISECONDS);
    //repeat 2 times
    monitor.assertJobExecutedMatchingUtil(timeout, MILLISECONDS);
    monitor.assertJobExecutedMatchingUtil(timeout, MILLISECONDS);
    monitor.assertJobNotExecutedAfter(timeout, MILLISECONDS);
  }

  @Test
  public void triggerAddedJobToExecute() throws Exception {
    JobDetail job = monitor.aJob().build();
    quartz.addJob(job, true, true);

    quartz.scheduleJob(newTrigger().forJob(job).build());

    monitor.assertJobExecutedMatchingUtil(1, SECONDS);
  }

  @Test
  public void triggerAddedJobToExecuteByJobName() throws Exception {
    JobDetail job = monitor.aJob().withIdentity("foo").build();
    quartz.addJob(job, true, true);

    quartz.scheduleJob(newTrigger().forJob("foo").build());
    monitor.assertJobExecutedMatchingUtil(100, MILLISECONDS);
  }

  @Test(expected = JobPersistenceException.class)
  public void triggerJobNameThrowsExceptionIfJobNotExists() throws Exception {
    quartz.scheduleJob(newTrigger().forJob("foo").build());
  }

  @Test
  public void executeJobWithinDataProvidedByTrigger() throws Exception {
    JobDataMap data = new JobDataMap() {{
      put("foo", "bar");
    }};

    quartz.scheduleJob(monitor.aJob().build(), newTrigger().usingJobData(data).build());

    monitor.assertJobExecutedMatchingUtil(1, TimeUnit.SECONDS,
        Matchers.<String, Object>hasEntry("foo", "bar"));
  }

  @Test
  public void executeJobWithinDataProvidedByJobDetail() throws Exception {
    JobDataMap data = new JobDataMap() {{
      put("foo", "bar");
    }};

    quartz.scheduleJob(monitor.aJob().setJobData(data).build(),
        newTrigger().build());

    monitor.assertJobExecutedMatchingUtil(1, TimeUnit.SECONDS,
        Matchers.<String, Object>hasEntry("foo", "bar"));
  }
}
