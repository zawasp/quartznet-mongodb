using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quartz.Impl.Matchers;
using Quartz.MongoDB.Tests.Models;
using Quartz.Spi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.MongoDB.Tests
{
    /// <summary>
    /// Test class for JobStore
    /// </summary>
    [TestClass]
    public class JobStoreTests
    {
        private JobStore _jobStore;
        private IJobDetail _job1;
        private IOperableTrigger _cronTrigger;

        public JobStoreTests()
        {
            _jobStore = new JobStore();
            _jobStore.InstanceId = "JobStoreTests";
            _jobStore.TablePrefix = "jobStoreTest.";
            _jobStore.ConnectionStringName = "quartz-db";
            _jobStore.Initialize(null, null);
            Initialize();
        }

        /// <summary>
        /// Initialize
        /// </summary>
        private void Initialize()
        {
            // generate jobs and triggers

            _job1 = JobBuilder.Create<Job1>()
                .WithIdentity("job1", "group1")
                .WithDescription("Description of job")
                .StoreDurably()
                .UsingJobData("key1", "value1")
                .Build();

            _cronTrigger = TriggerBuilder.Create()
                .WithIdentity("cronTrigger1", "group1")
                .WithDescription("Sample cron trigger")
                .ForJob(_job1)
                .WithCronSchedule("0/5 * * * * ?", x => x.WithMisfireHandlingInstructionDoNothing())
                .UsingJobData("key1", null)
                .Build() as IOperableTrigger;
        }

        /// <summary>
        /// Describes job storing process
        /// </summary>
        [TestMethod]
        public void CreateJob()
        {
            var job = _job1;

            try
            {
                // store job
                _jobStore.StoreJob(job, false);
            }
            catch (ObjectAlreadyExistsException)
            {
                // if job already exists, we can change some property to test update process
                job.JobDataMap.Add("key2", "value2");

                // store job (actually update process)
                _jobStore.StoreJob(job, true);
            }

            // job should be exists
            Assert.IsTrue(_jobStore.CheckExists(job.Key));

            // and instances should be equal
            Assert.AreEqual(job, _jobStore.RetrieveJob(job.Key));

            var keys = _jobStore.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(job.Key.Group));
            Assert.IsTrue(keys.Any(x => x.Equals(job.Key)));

            var groups = _jobStore.GetJobGroupNames();
            Assert.IsTrue(groups.Contains(job.Key.Group));
        }

        /// <summary>
        /// Describes trigger storing process
        /// </summary>
        [TestMethod]
        public void CreateTrigger()
        {
            var trigger = _cronTrigger;

            try
            {
                // store trigger
                _jobStore.StoreTrigger(trigger, false);
            }
            catch (ObjectAlreadyExistsException)
            {
                // if trigger already exists, we can change some property to test update process
                trigger.JobDataMap.Add("key2", "value2");

                // store trigger (actually update process)
                _jobStore.StoreTrigger(trigger, true);
            }

            // trigger should be exists
            Assert.IsTrue(_jobStore.CheckExists(trigger.Key));

            // and instances should be equal
            Assert.AreEqual(trigger, _jobStore.RetrieveTrigger(trigger.Key));

            var keys = _jobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(trigger.Key.Group));
            Assert.IsTrue(keys.Any(x => x.Equals(trigger.Key)));

            var groups = _jobStore.GetTriggerGroupNames();
            Assert.IsTrue(groups.Contains(trigger.Key.Group));
        }

        /// <summary>
        /// Describes calendar storing process
        /// </summary>
        [TestMethod]
        public void CreateCalendar()
        {
            var cal = new Impl.Calendar.HolidayCalendar();
            cal.Description = "Custom calendar 1";
            cal.AddExcludedDate(new DateTime(2016, 1, 1));
            cal.TimeZone = TimeZoneInfo.Utc;

            try
            {
                // store calendar
                _jobStore.StoreCalendar("cal1", cal, false, false);
            }
            catch (ObjectAlreadyExistsException)
            {
                // if trigger already exists, we can change some property to test update process
                // and store calendar (actually update process)
                _jobStore.StoreCalendar("cal1", cal, true, false);
            }

            // calendar should be exists
            Assert.IsTrue(_jobStore.CalendarExists("cal1"));

            // and instances should be equal
            var actual = _jobStore.RetrieveCalendar("cal1") as Impl.Calendar.HolidayCalendar;

            Assert.AreEqual(cal.Description, actual.Description);
            Assert.AreEqual(cal.TimeZone, actual.TimeZone);
            Assert.AreEqual(cal.ExcludedDates.Count, actual.ExcludedDates.Count);
            Assert.AreEqual(cal.ExcludedDates.FirstOrDefault(), actual.ExcludedDates.FirstOrDefault());
        }
    }
}
