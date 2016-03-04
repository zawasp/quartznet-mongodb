using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using System.Configuration;

namespace Quartz.MongoDB.Tests
{
    /// <summary>
    /// DbContext tests
    /// </summary>
    [TestClass]
    public class DbContextTests
    {
        /// <summary>
        /// Describes connection to db
        /// </summary>
        [TestMethod]
        public void TestConnection()
        {
            try
            {
                var db = new DbContext(ConfigurationManager.ConnectionStrings["quartz-db"].ConnectionString, "dbContext.");
                var result = db.Ping();

                Assert.AreEqual(result["ok"].AsDouble, 1.0d);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }
    }
}
