using MongoDB.Bson;
using MongoDB.Driver;
using Quartz.MongoDB.Serialization;

namespace Quartz.MongoDB
{
    /// <summary>
    /// DbContext to store all scheduling data
    /// </summary>
    internal class DbContext
    {
        private IMongoClient _client;
        private IMongoDatabase _database;
        private string _prefix;

        static DbContext()
        {
            // register all serializers
            SerializationProvider.RegisterSerializers();
        }

        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="url">Mongo url</param>
        /// <param name="prefix">Prefix for collection names. It inserts start of collection name. Default value is "qrtz."</param>
        public DbContext(string url, string prefix = Constants.DEFAULT_PREFIX)
        {
            var mongoUrl = new MongoUrl(url);
            _client = new MongoClient(mongoUrl);
            _database = _client.GetDatabase(mongoUrl.DatabaseName);

            _prefix = prefix;

            InitializeCollections();
        }

        // Collections

        internal IMongoCollection<BsonDocument> Schedulers { get; private set; }
        internal IMongoCollection<BsonDocument> Calendars { get; private set; }
        internal IMongoCollection<BsonDocument> Triggers { get; private set; }
        internal IMongoCollection<BsonDocument> Jobs { get; private set; }
        internal IMongoCollection<BsonDocument> PausedTriggerGroups { get; private set; }
        internal IMongoCollection<BsonDocument> PausedJobGroups { get; private set; }
        internal IMongoCollection<BsonDocument> BlockedJobs { get; private set; }

        /// <summary>
        /// Initialize collections
        /// </summary>
        private void InitializeCollections()
        {
            Schedulers = _database.GetCollection<BsonDocument>(GenerateCollectionName("schedulers"));
            Calendars = _database.GetCollection<BsonDocument>(GenerateCollectionName("calendars"));
            Triggers = _database.GetCollection<BsonDocument>(GenerateCollectionName("triggers"));
            PausedTriggerGroups = _database.GetCollection<BsonDocument>(GenerateCollectionName("pausedTriggerGroups"));
            Jobs = _database.GetCollection<BsonDocument>(GenerateCollectionName("jobs"));
            PausedJobGroups = _database.GetCollection<BsonDocument>(GenerateCollectionName("pausedJobGroups"));
            BlockedJobs = _database.GetCollection<BsonDocument>(GenerateCollectionName("blockedJobs"));
        }

        /// <summary>
        /// Generate collection name
        /// </summary>
        /// <param name="name">name of collection</param>
        /// <returns></returns>
        private string GenerateCollectionName(string name)
        {
            return _prefix + name;
        }

        /// <summary>
        /// Test for database connectivity
        /// </summary>
        /// <returns></returns>
        internal BsonDocument Ping()
        {
            return _database.RunCommand((Command<BsonDocument>)"{ping:1}");
        }

        /// <summary>
        /// Clear all collections
        /// </summary>
        internal void ClearAllCollections()
        {
            var filter = new BsonDocument();

            Schedulers.DeleteMany(filter);
            Calendars.DeleteMany(filter);
            Triggers.DeleteMany(filter);
            Jobs.DeleteMany(filter);
            PausedTriggerGroups.DeleteMany(filter);
            PausedJobGroups.DeleteMany(filter);
            BlockedJobs.DeleteMany(filter);
        }
    }
}
