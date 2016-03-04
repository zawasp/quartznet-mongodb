using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quartz.MongoDB.Tests.Models;
using System.Threading;
using System.Configuration;
using System.Diagnostics;
using Quartz.Impl;

namespace Quartz.MongoDB.Tests
{
    /// <summary>
    /// This test class describes how it(JobStore) works in real usage
    /// </summary>
    [TestClass]
    public class RealUsageTests
    {
        private IJobDetail _job1;
        private IJobDetail _job2;
        private ITrigger _trigger1;
        private ITrigger _trigger2;

        /// <summary>
        /// Ctor
        /// </summary>
        public RealUsageTests()
        {
            // Initialize jobs and triggers

            _job1 = JobBuilder.Create<Job1>()
                    .WithIdentity("job1", "group1")
                    .Build();

            _job2 = JobBuilder.Create<Job2>()
                    .WithIdentity("job2", "group1")
                    .Build();

            _trigger1 = TriggerBuilder.Create()
                    .WithIdentity("trigger1", "group1")
                    .UsingJobData("key1", "value1")
                    .StartNow()
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(3)
                        .RepeatForever()
                        .WithMisfireHandlingInstructionIgnoreMisfires())
                    .Build();

            _trigger2 = TriggerBuilder.Create()
                    .WithIdentity("trigger2", "group1")
                    .WithCronSchedule("0/3 * * * * ?", x => x.WithMisfireHandlingInstructionIgnoreMisfires())
                    .UsingJobData("parameters", "{\"Name\": \"Man\"}")
                    .Build();
        }

        /// <summary>
        /// 
        /// </summary>
        private void ClearDb()
        {
            var dbContext = new DbContext(ConfigurationManager.ConnectionStrings["quartz-db"].ConnectionString, "qrtz.");
            dbContext.ClearAllCollections();
        }

        /// <summary>
        /// This test method describes auto resume and storing jobs and triggers
        /// </summary>
        [TestMethod]
        public void Usage1()
        {
            // Clear database
            ClearDb();

            var scheduler = StdSchedulerFactory.GetDefaultScheduler();
            var waiter = new ManualResetEvent(false);

            scheduler.Start();

            // schedule jobs
            scheduler.ScheduleJob(_job1, _trigger1);
            scheduler.ScheduleJob(_job2, _trigger2);

            // wait 5 seconds for job execution
            waiter.WaitOne(TimeSpan.FromSeconds(5));

            // job should be executed at least 1 time
            Assert.IsTrue(Job1.Count > 0);
            // shutdown scheduler, and jobStore should store all data
            scheduler.Shutdown();

            Thread.Sleep(1000);

            // Next time

            waiter.Reset();
            Job1.Count = 0;
            scheduler = StdSchedulerFactory.GetDefaultScheduler();

            scheduler.Start();
            // wait 5 seconds for job execution
            waiter.WaitOne(TimeSpan.FromSeconds(5));

            // if all data stored in last time, following conditions should be true
            Assert.AreEqual(_job1, scheduler.GetJobDetail(_job1.Key));
            Assert.AreEqual(_job2, scheduler.GetJobDetail(_job2.Key));
            Assert.AreEqual(_trigger1, scheduler.GetTrigger(_trigger1.Key));
            Assert.AreEqual(_trigger2, scheduler.GetTrigger(_trigger2.Key));

            // and scheduler should resume triggers
            Assert.IsTrue(Job1.Count > 0);

            scheduler.Shutdown();
        }

        /// <summary>
        /// This test method describes pausing and resuming process
        /// </summary>
        [TestMethod]
        public void Usage2()
        {
            // Clear database
            ClearDb();

            var job = _job2;
            var trigger = _trigger2;

            // initializing scheduler
            var scheduler = StdSchedulerFactory.GetDefaultScheduler();
            var waiter = new ManualResetEvent(false);

            scheduler.Start();

            // schedule job
            scheduler.ScheduleJob(job, trigger);

            // and wait one
            waiter.WaitOne(TimeSpan.FromSeconds(5));

            // job should be executed at least 1 time
            Assert.IsTrue(Job2.Count > 0);

            // then we pause trigger
            scheduler.PauseTrigger(trigger.Key);

            // state of trigger should be "Paused"
            Assert.AreEqual(TriggerState.Paused, scheduler.GetTriggerState(trigger.Key));
            Job2.Count = 0;
            waiter.Reset();
            // wait one
            waiter.WaitOne(TimeSpan.FromSeconds(5));
            // trigger paused, therefore job should not be executed
            Assert.IsTrue(Job2.Count == 0);

            Job2.Count = 0;
            // then resume trigger
            scheduler.ResumeTrigger(trigger.Key);
            // state of trigger should be "Normal"
            Assert.AreEqual(TriggerState.Normal, scheduler.GetTriggerState(trigger.Key));
            waiter.Reset();
            //wait one
            waiter.WaitOne(TimeSpan.FromSeconds(5));

            // job should be executed at least 1 time
            Assert.IsTrue(Job2.Count > 0);

            scheduler.Shutdown();
        }

        /// <summary>
        /// This test method describes rescheduling and removing processes
        /// </summary>
        [TestMethod]
        public void Usage3()
        {
            // Clear database
            ClearDb();

            var job = _job2;
            var trigger = _trigger2;

            var scheduler = StdSchedulerFactory.GetDefaultScheduler();
            var waiter = new ManualResetEvent(false);

            scheduler.Start();
            
            // schedule job
            scheduler.ScheduleJob(job, trigger);
            // wait one
            waiter.WaitOne(TimeSpan.FromSeconds(5));
            // job should be executed at least 1 time
            Assert.IsTrue(Job2.Count > 0);

            // rescheduler job
            scheduler.RescheduleJob(trigger.Key, trigger);

            // state of trigger should be "Normal"
            Assert.AreEqual(TriggerState.Normal, scheduler.GetTriggerState(trigger.Key));

            // delete job
            scheduler.DeleteJob(job.Key);

            // and job should be removed from store
            Assert.IsTrue(scheduler.GetTrigger(trigger.Key) == null);

            scheduler.Shutdown();
        }
    }
}
