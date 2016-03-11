package com.selonj.getstarted.quartz.supports;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.quartz.JobBuilder.newJob;

/**
 * Created by Administrator on 2016-03-11.
 */
public class QuartzJobMonitor implements JobFactory {
  private volatile AtomicInteger countOfCreatedInstances = new AtomicInteger(0);
  private BlockingQueue<Object> jobExecutionBlocking = new ArrayBlockingQueue<>(1);

  @Override public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler)
      throws SchedulerException {
    return new JobStub();
  }

  public JobBuilder aJob() {
    return JobBuilder.newJob(JobStub.class);
  }

  public void assertJobExecuted(long timeout, TimeUnit seconds) throws InterruptedException {
    assertTrue(waitingJobDoneUtil(timeout, seconds));
  }

  public void assertJobNotExecutedAfter(long timeout, TimeUnit unit) throws InterruptedException {
    assertFalse(waitingJobDoneUtil(timeout, unit));
  }

  private boolean waitingJobDoneUtil(long timeout, TimeUnit seconds) throws InterruptedException {
    return jobExecutionBlocking.poll(timeout, seconds) != null;
  }

  public void assertCountOfCreatedJobInstancesEqual(int count) throws InterruptedException {
    waitingJobDoneUtil(1, TimeUnit.SECONDS);
    assertThat(countOfCreatedInstances.get(), equalTo(count));
  }

  private class JobStub implements Job {
    public JobStub() {
      countOfCreatedInstances.addAndGet(1);
    }

    @Override public void execute(JobExecutionContext context) throws JobExecutionException {
      jobExecutionBlocking.add("");
    }
  }
}
