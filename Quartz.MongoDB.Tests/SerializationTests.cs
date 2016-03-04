using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using Quartz.MongoDB.Serialization;
using MongoDB.Bson.Serialization;
using Quartz.MongoDB.Tests.Models;
using Quartz.Impl;

namespace Quartz.MongoDB.Tests
{
    /// <summary>
    /// This test class describes bson serialization process
    /// </summary>
    [TestClass]
    public class SerializationTests
    {
        public SerializationTests()
        {
            // regiter serializers before starting tests
            SerializationProvider.RegisterSerializers();
        }

        /// <summary>
        /// Describes serialization of TriggerKey class
        /// </summary>
        [TestMethod]
        public void TriggerKey()
        {
            var expected = new TriggerKey("trigger1", "group1");

            var bsonDoc = expected.ToBsonDocument();

            var actual = BsonSerializer.Deserialize<TriggerKey>(bsonDoc);

            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Describes serialization of JobKey class
        /// </summary>
        [TestMethod]
        public void JobKey()
        {
            var expected = new JobKey("job1", "group1");

            var bsonDoc = expected.ToBsonDocument();

            var actual = BsonSerializer.Deserialize<JobKey>(bsonDoc);

            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Describes serialization of JobDataMap class
        /// </summary>
        [TestMethod]
        public void JobDataMap()
        {
            var expected = new JobDataMap();
            expected.Add("key1", "value1");
            expected.Add("key2", 234.5);
            expected.Add("key3", true);
            expected.Add("key4", "{ \"name\": \"Jim\" }");

            var bsonDoc = expected.ToBsonDocument();

            var actual = BsonSerializer.Deserialize<JobDataMap>(bsonDoc);

            foreach (var key in expected.Keys)
            {
                Assert.AreEqual(expected[key], actual[key]);
            }
        }

        /// <summary>
        /// Describes serialization of TimeZoneInfo class
        /// </summary>
        [TestMethod]
        public void TimeZoneInfoTest()
        {
            var expected = TimeZoneInfo.Utc;

            var wrapper = new TimeZoneInfoWrapper()
            {
                Value = expected
            };

            var bsonDoc = wrapper.ToBsonDocument();

            var actual = BsonSerializer.Deserialize<TimeZoneInfoWrapper>(bsonDoc).Value;

            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Describes serialization of TimeOfDay class
        /// </summary>
        [TestMethod]
        public void TimeOfDayTest()
        {
            var expected = new TimeOfDay(15, 35, 49);

            var wrapper = new TimeOfDayWrapper()
            {
                Value = expected
            };

            var bsonDoc = wrapper.ToBsonDocument();

            var actual = BsonSerializer.Deserialize<TimeOfDayWrapper>(bsonDoc).Value;

            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Describes serialization of JobDetailImpl class
        /// </summary>
        [TestMethod]
        public void JobDetailImplTest()
        {
            var jobDataMap = new JobDataMap();
            jobDataMap.Add("key1", "value1");
            jobDataMap.Add("key2", true);

            var expected = (JobDetailImpl)JobBuilder.Create<Job1>()
                .WithIdentity("job1", "group1")
                .WithDescription("Email sender job")
                .StoreDurably()
                .UsingJobData(jobDataMap)
                .Build();

            var bsonDoc = expected.ToBsonDocument();

            var actual = BsonSerializer.Deserialize<JobDetailImpl>(bsonDoc);

            Assert.AreEqual(expected, actual);
        }
    }
}
