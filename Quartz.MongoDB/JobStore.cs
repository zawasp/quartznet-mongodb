using Quartz.Spi;
using System;
using System.Collections.Generic;
using System.Linq;
using Quartz.Impl.Matchers;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using MongoDB.Bson.Serialization;
using Quartz.MongoDB.Models;
using Quartz.Impl;
using System.Threading;
using System.Globalization;
using System.Configuration;
using Common.Logging;

namespace Quartz.MongoDB
{
    /// <summary>
    /// Mongo job store for Quartz
    /// </summary>
    public class JobStore : Constants, IJobStore
    {
        private readonly ILog Log = LogManager.GetLogger<JobStore>();
        private readonly object lockObject = new object();
        private string _instanceId;
        private string _instanceName;
        private int _threadPoolSize;
        private DbContext _db;
        private TimeSpan misfireThreshold = TimeSpan.FromSeconds(5);
        private ISchedulerSignaler signaler;

        /// <summary>
        /// Ctor
        /// </summary>
        public JobStore()
        {
        }

        /// <summary>
        /// Quartz automatically loads if "quartz.jobStore.connectionString" config set
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Quartz automatically loads if "quartz.jobStore.connectionStringName" config set
        /// </summary>
        public string ConnectionStringName { get; set; }

        public bool Clustered
        {
            get
            {
                return false;   // not tested yet
            }
        }

        public long EstimatedTimeToReleaseAndAcquireTrigger
        {
            get
            {
                return 100;
            }
        }

        /// <summary>
        /// Quartz automatically loads if "quartz.scheduler.instanceId" config set
        /// </summary>
        public string InstanceId
        {
            set
            {
                _instanceId = value;
            }
        }

        /// <summary>
        /// Quartz automatically loads if "quartz.scheduler.instanceName" config set
        /// </summary>
        public string InstanceName
        {
            set
            {
                _instanceName = value;
            }
        }

        public bool SupportsPersistence
        {
            get
            {
                return true;
            }
        }

        public int ThreadPoolSize
        {
            set
            {
                _threadPoolSize = value;
            }
        }

        /// <summary>
        /// Quartz automatically loads if "quartz.jobStore.misfireThreshold" config set
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan MisfireThreshold
        {
            get { return misfireThreshold; }
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("Misfirethreashold must be larger than 0");
                }
                misfireThreshold = value;
            }
        }

        /// <summary>
        /// Quartz automatically loads if "quartz.jobStore.tablePrefix" config set
        /// </summary>
        public string TablePrefix { get; set; }

        /// <summary>
        /// Initializing job store
        /// </summary>
        /// <param name="loadHelper"></param>
        /// <param name="signaler"></param>
        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
        {
            lock (lockObject)
            {
                if (string.IsNullOrWhiteSpace(ConnectionStringName))
                    ConnectionStringName = DEFAULT_CONNECTION_STRING_NAME;

                _db = new DbContext(ConfigurationManager.ConnectionStrings[ConnectionStringName] == null ?
                    ConnectionString : ConfigurationManager.ConnectionStrings[ConnectionStringName].ConnectionString, TablePrefix);
                this.signaler = signaler;
            }
        }

        /// <summary>
        /// Get a handle to the next trigger to be fired, and mark it as 'reserved'
        /// by the calling scheduler.
        /// </summary>
        public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            lock (lockObject)
            {
                var Query = Builders<BsonDocument>.Filter;
                var Update = Builders<BsonDocument>.Update;

                // multiple instances management
                var doc = new BsonDocument()
                    .SetElement(new BsonElement("_id", _instanceId))
                    .SetElement(new BsonElement("Expires", (SystemTime.Now() + new TimeSpan(0, 10, 0)).UtcDateTime))
                    .SetElement(new BsonElement("State", "Running"));
                _db.Schedulers.ReplaceOne(
                    Builders<BsonDocument>.Filter.Eq("_id", _instanceId),
                    doc, new UpdateOptions { IsUpsert = true });

                _db.Schedulers.DeleteMany(
                    Query.Lt("Expires", SystemTime.Now().UtcDateTime));

                IEnumerable<BsonValue> activeInstances = _db.Schedulers.Distinct(x => x["_id"], Query.Empty).ToEnumerable();

                _db.Triggers.UpdateMany(
                    Query.And(
                        Query.Nin("SchedulerInstanceId", activeInstances),
                            Query.Nin("State", new string[]
                            { TriggerStates.PAUSED, TriggerStates.PAUSED_AND_BLOCKED, TriggerStates.COMPLETE })),
                    Update.Unset("SchedulerInstanceId")
                        .Set("State", TriggerStates.WAITING));

                List<IOperableTrigger> result = new List<IOperableTrigger>();
                Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
                DateTimeOffset? firstAcquiredTriggerFireTime = null;

                var candidates = _db.Triggers.Find(
                    Query.And(
                        Query.Eq("State", TriggerStates.WAITING),
                        Query.Lte("nextFireTimeUtc", noLaterThan + timeWindow)))
                    .As<IOperableTrigger>()
                    .ToEnumerable()
                    .OrderBy(t => t.GetNextFireTimeUtc()).ThenByDescending(t => t.Priority);

                foreach (IOperableTrigger trigger in candidates)
                {
                    if (trigger.GetNextFireTimeUtc() == null)
                    {
                        continue;
                    }

                    // it's possible that we've selected triggers way outside of the max fire ahead time for batches 
                    // (up to idleWaitTime + fireAheadTime) so we need to make sure not to include such triggers.  
                    // So we select from the first next trigger to fire up until the max fire ahead time after that...
                    // which will perfectly honor the fireAheadTime window because the no firing will occur until
                    // the first acquired trigger's fire time arrives.
                    if (firstAcquiredTriggerFireTime != null
                        && trigger.GetNextFireTimeUtc() > (firstAcquiredTriggerFireTime.Value + timeWindow))
                    {
                        break;
                    }

                    if (ApplyMisfire(trigger))
                    {
                        if (trigger.GetNextFireTimeUtc() == null
                            || trigger.GetNextFireTimeUtc() > noLaterThan + timeWindow)
                        {
                            continue;
                        }
                    }

                    if (trigger.GetNextFireTimeUtc() > noLaterThan + timeWindow)
                        continue;

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // put it back into the timeTriggers set and continue to search for next trigger.
                    JobKey jobKey = trigger.JobKey;
                    IJobDetail job = _db.Jobs.Find(Query.Eq("_id", jobKey.ToBsonDocument()))
                        .As<JobDetailImpl>()
                        .FirstOrDefault();

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            continue; // go to next trigger in store.
                        }
                        else
                        {
                            acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                        }
                    }

                    trigger.FireInstanceId = this.GetFiredTriggerRecordId();
                    var acquired = _db.Triggers.FindOneAndUpdate(
                        Query.And(
                            Query.Eq("_id", trigger.Key),
                            Query.Eq("State", TriggerStates.WAITING)),
                        Update.Set("State", TriggerStates.ACQUIRED)
                            .Set("SchedulerInstanceId", _instanceId)
                            .Set("FireInstanceId", trigger.FireInstanceId));

                    if (acquired != null)
                    {
                        result.Add(trigger);

                        if (firstAcquiredTriggerFireTime == null)
                        {
                            firstAcquiredTriggerFireTime = trigger.GetNextFireTimeUtc();
                        }
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }

                return result;
            }
        }

        private static long ftrCtr = SystemTime.UtcNow().Ticks;
        protected virtual string GetFiredTriggerRecordId()
        {
            long value = Interlocked.Increment(ref ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        public bool CalendarExists(string calName)
        {
            lock (lockObject)
            {
                return _db.Calendars
                              .Find(x => x["_id"].Equals(calName))
                              .Any();
            }
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                return _db.Triggers
                      .Find(triggerKey.ToBsonDocument())
                      .Any();
            }
        }

        public bool CheckExists(JobKey jobKey)
        {
            lock (lockObject)
            {
                return _db.Jobs
                      .Find(jobKey.ToBsonDocument())
                      .Any();
            }
        }

        public void ClearAllSchedulingData()
        {
            lock (lockObject)
            {
                _db.ClearAllCollections();
            }
        }

        public IList<string> GetCalendarNames()
        {
            lock (lockObject)
            {
                return _db.Calendars
                      .AsQueryable()
                      .Select(x => x["_id"])
                      .ToEnumerable()
                      .Select(x => x.AsString)
                      .ToList();
            }
        }

        public IList<string> GetJobGroupNames()
        {
            lock (lockObject)
            {
                return _db.Jobs
                      .AsQueryable()
                      .Select(x => x["Group"])
                      .ToEnumerable()
                      .Select(x => x.AsString)
                      .ToList();
            }
        }

        public Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            lock (lockObject)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Empty;

                if (matcher.CompareWithOperator == StringOperator.Contains)
                    filter = filterBuilder.Regex("Group", new BsonRegularExpression(matcher.CompareToValue));
                else if (matcher.CompareWithOperator == StringOperator.StartsWith)
                    filter = filterBuilder.Regex("Group", new BsonRegularExpression("^" + matcher.CompareToValue));
                else if (matcher.CompareWithOperator == StringOperator.EndsWith)
                    filter = filterBuilder.Regex("Group", new BsonRegularExpression(matcher.CompareToValue + "$"));

                var keys = _db.Jobs
                          .Find(filter)
                          .ToEnumerable()
                          .Select(x => x["_id"])
                          .Select(x => BsonSerializer.Deserialize<JobKey>(x.AsBsonDocument));

                return new Collection.HashSet<JobKey>(keys);
            }
        }

        public int GetNumberOfCalendars()
        {
            lock (lockObject)
            {
                return (int)_db.Calendars.Count(x => true);
            }
        }

        public int GetNumberOfJobs()
        {
            lock (lockObject)
            {
                return (int)_db.Jobs.Count(x => true);
            }
        }

        public int GetNumberOfTriggers()
        {
            lock (lockObject)
            {
                return (int)_db.Triggers.Count(x => true);
            }
        }

        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            lock (lockObject)
            {
                return new Collection.HashSet<string>(
                _db.PausedTriggerGroups
                   .AsQueryable()
                   .Select(x => x["Name"])
                   .ToEnumerable()
                   .Select(x => x.AsString));
            }
        }

        public IList<string> GetTriggerGroupNames()
        {
            lock (lockObject)
            {
                return _db.Triggers
                   .AsQueryable()
                   .Select(x => x["Group"])
                   .ToEnumerable()
                   .Select(x => x.AsString)
                   .ToList();
            }
        }

        public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            lock (lockObject)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Empty;

                if (matcher.CompareWithOperator == StringOperator.Contains)
                    filter = filterBuilder.Regex("Group", new BsonRegularExpression(matcher.CompareToValue));
                else if (matcher.CompareWithOperator == StringOperator.StartsWith)
                    filter = filterBuilder.Regex("Group", new BsonRegularExpression("^" + matcher.CompareToValue));
                else if (matcher.CompareWithOperator == StringOperator.EndsWith)
                    filter = filterBuilder.Regex("Group", new BsonRegularExpression(matcher.CompareToValue + "$"));

                var keys = _db.Triggers
                          .Find(filter)
                          .ToEnumerable()
                          .Select(x => x["_id"])
                          .Select(x => BsonSerializer.Deserialize<TriggerKey>(x.AsBsonDocument));

                return new Collection.HashSet<TriggerKey>(keys);
            }
        }

        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                return _db.Triggers
                    .Find(Builders<BsonDocument>.Filter.Eq("JobKey", jobKey))
                    .As<IOperableTrigger>()
                    .ToList();
            }
        }

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                BsonDocument triggerState = _db.Triggers.Find(triggerKey.ToBsonDocument()).FirstOrDefault();

                if (triggerState.IsBsonNull)
                {
                    return TriggerState.None;
                }
                if (triggerState["State"] == TriggerStates.COMPLETE)
                {
                    return TriggerState.Complete;
                }
                if (triggerState["State"] == TriggerStates.PAUSED)
                {
                    return TriggerState.Paused;
                }
                if (triggerState["State"] == TriggerStates.PAUSED_AND_BLOCKED)
                {
                    return TriggerState.Paused;
                }
                if (triggerState["State"] == TriggerStates.BLOCKED)
                {
                    return TriggerState.Blocked;
                }
                if (triggerState["State"] == TriggerStates.ERROR)
                {
                    return TriggerState.Error;
                }

                return TriggerState.Normal;
            }
        }

        public bool IsJobGroupPaused(string groupName)
        {
            lock (lockObject)
            {
                return _db.PausedJobGroups.Find(groupName).Any();
            }
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            lock (lockObject)
            {
                return _db.PausedTriggerGroups.Find(groupName).Any();
            }
        }

        public void PauseAll()
        {
            lock (lockObject)
            {
                IList<string> triggerGroupNames = GetTriggerGroupNames();

                foreach (string groupName in triggerGroupNames)
                {
                    PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
        }

        public void PauseJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    PauseTrigger(trigger.Key);
                }
            }
        }

        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            List<string> pausedGroups = new List<string>();
            lock (lockObject)
            {
                StringOperator op = matcher.CompareWithOperator;
                if (op == StringOperator.Equality)
                {
                    var doc = new BsonDocument();
                    doc.Add(new BsonElement("_id", matcher.CompareToValue));
                    _db.PausedJobGroups.UpdateOne(doc, doc, new UpdateOptions { IsUpsert = true });

                    pausedGroups.Add(matcher.CompareToValue);
                }
                else
                {
                    IList<string> groups = GetJobGroupNames();

                    foreach (string group in groups)
                    {
                        if (op.Evaluate(group, matcher.CompareToValue))
                        {
                            var doc = new BsonDocument();
                            doc.Add(new BsonElement("_id", matcher.CompareToValue));
                            _db.PausedJobGroups.UpdateOne(doc, doc, new UpdateOptions { IsUpsert = true });

                            pausedGroups.Add(matcher.CompareToValue);
                        }
                    }
                }

                foreach (string groupName in pausedGroups)
                {
                    foreach (JobKey jobKey in GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName)))
                    {
                        IList<IOperableTrigger> triggers = this.GetTriggersForJob(jobKey);
                        foreach (IOperableTrigger trigger in triggers)
                        {
                            PauseTrigger(trigger.Key);
                        }
                    }
                }
            }

            return pausedGroups;
        }

        public void PauseTrigger(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                var filter = Builders<BsonDocument>.Filter;
                var update = Builders<BsonDocument>.Update;

                _db.Triggers
                    .UpdateOne(
                        filter.And(
                            filter.Eq("_id", triggerKey),
                            filter.Eq("State", TriggerStates.BLOCKED)),
                        update.Set("State", TriggerStates.PAUSED_AND_BLOCKED));

                _db.Triggers
                    .UpdateOne(
                        filter.And(
                            filter.Eq("_id", triggerKey),
                            filter.Ne("State", TriggerStates.BLOCKED)),
                        update.Set("State", TriggerStates.PAUSED));
            }
        }

        public Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            IList<string> pausedGroups;

            lock (lockObject)
            {
                pausedGroups = new List<string>();

                StringOperator op = matcher.CompareWithOperator;
                if (op == StringOperator.Equality)
                {
                    var doc = new BsonDocument();
                    doc.Add(new BsonElement("_id", matcher.CompareToValue));
                    _db.PausedTriggerGroups.UpdateOne(doc, doc, new UpdateOptions { IsUpsert = true });

                    pausedGroups.Add(matcher.CompareToValue);
                }
                else
                {
                    IList<string> groups = GetTriggerGroupNames();

                    foreach (string group in groups)
                    {
                        if (op.Evaluate(group, matcher.CompareToValue))
                        {
                            var doc = new BsonDocument();
                            doc.Add(new BsonElement("_id", matcher.CompareToValue));
                            _db.PausedTriggerGroups.UpdateOne(doc, doc, new UpdateOptions { IsUpsert = true });

                            pausedGroups.Add(matcher.CompareToValue);
                        }
                    }
                }

                foreach (string pausedGroup in pausedGroups)
                {
                    Collection.ISet<TriggerKey> keys = this.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(pausedGroup));

                    foreach (TriggerKey key in keys)
                    {
                        this.PauseTrigger(key);
                    }
                }
            }
            return new Collection.HashSet<string>(pausedGroups);
        }

        public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            lock (lockObject)
            {
                var filter = Builders<BsonDocument>.Filter;
                _db.Triggers.UpdateOne(
                    filter.And(
                        filter.Eq("_id", trigger.Key),
                        filter.Eq("State", TriggerStates.ACQUIRED)),
                    Builders<BsonDocument>.Update
                        .Unset("SchedulerInstanceId")
                        .Set("State", TriggerStates.WAITING));
            }
        }

        public bool RemoveCalendar(string calName)
        {
            lock (lockObject)
            {
                if (_db.Triggers.Find(x => x["CalendarName"].Equals(calName)).Any())
                {
                    throw new JobPersistenceException("Calender cannot be removed if it is referenced by a Trigger!");
                }

                _db.Calendars.DeleteOne(x => x["_id"].Equals(calName));

                return true;
            }
        }

        public bool RemoveJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                bool found;

                IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    RemoveTrigger(trigger.Key);
                }

                found = CheckExists(jobKey);

                if (found)
                {
                    _db.Jobs.DeleteOne(jobKey.ToBsonDocument());

                    _db.BlockedJobs.DeleteOne(jobKey.ToBsonDocument());

                    var others = _db.Jobs.Find(x => x["Group"].Equals(jobKey.Group));

                    if (others.Count() == 0)
                    {
                        _db.PausedJobGroups.DeleteOne(x => x["_id"].Equals(jobKey.Group));
                    }
                }

                return found;
            }
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            lock (lockObject)
            {
                bool allFound = true;

                foreach (JobKey key in jobKeys)
                {
                    allFound = RemoveJob(key) && allFound;
                }

                return allFound;
            }
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            return RemoveTrigger(triggerKey, true);
        }

        /// <summary>
        /// Remove Trigger
        /// </summary>
        /// <param name="triggerKey"></param>
        /// <param name="tryToRemoveJob">Try to remove job, if there is not exists trigger for this job</param>
        /// <returns></returns>
        protected bool RemoveTrigger(TriggerKey triggerKey, bool tryToRemoveJob)
        {
            lock (lockObject)
            {
                bool found;

                var trigger = RetrieveTrigger(triggerKey);
                found = trigger != null;

                if (found)
                {
                    _db.Triggers.DeleteOne(trigger.Key.ToBsonDocument());

                    if (tryToRemoveJob)
                    {
                        IJobDetail jobDetail = RetrieveJob(trigger.JobKey);
                        if (jobDetail != null)
                        {
                            IList<IOperableTrigger> trigs = GetTriggersForJob(jobDetail.Key);
                            if ((trigs == null
                                || trigs.Count == 0)
                                && !jobDetail.Durable)
                            {
                                if (RemoveJob(jobDetail.Key))
                                {
                                    signaler.NotifySchedulerListenersJobDeleted(jobDetail.Key);
                                }
                            }
                        }
                    }
                }

                return found;
            }
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            lock (lockObject)
            {
                bool allFound = true;

                foreach (TriggerKey key in triggerKeys)
                {
                    allFound = RemoveTrigger(key) && allFound;
                }

                return allFound;
            }
        }

        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            bool found;

            lock (lockObject)
            {
                IOperableTrigger oldTrigger = _db.Triggers
                    .Find(triggerKey.ToBsonDocument())
                    .As<IOperableTrigger>()
                    .FirstOrDefault();

                found = oldTrigger != null;

                if (found)
                {
                    if (!oldTrigger.JobKey.Equals(newTrigger.JobKey))
                    {
                        throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                    }

                    RemoveTrigger(triggerKey, false);

                    try
                    {
                        StoreTrigger(newTrigger, false);
                    }
                    catch (JobPersistenceException)
                    {
                        StoreTrigger(oldTrigger, false); // put previous trigger back...
                        throw;
                    }
                }
            }

            return found;
        }

        public void ResumeAll()
        {
            lock (lockObject)
            {
                // TODO need a match all here!
                _db.PausedJobGroups.DeleteMany(x => true);
                IList<string> triggerGroupNames = GetTriggerGroupNames();

                foreach (string groupName in triggerGroupNames)
                {
                    ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
            }
        }

        public void ResumeJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
                foreach (IOperableTrigger trigger in triggersForJob)
                {
                    ResumeTrigger(trigger.Key);
                }
            }
        }

        public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            Collection.ISet<string> resumedGroups = new Collection.HashSet<string>();
            lock (lockObject)
            {
                Collection.ISet<JobKey> keys = GetJobKeys(matcher);

                foreach (string pausedJobGroup in _db.PausedJobGroups.Find(x => true).As<string>().ToEnumerable())
                {
                    if (matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue))
                    {
                        resumedGroups.Add(pausedJobGroup);
                    }
                }

                _db.PausedTriggerGroups.DeleteMany(
                        Builders<BsonDocument>.Filter.All("_id", new BsonArray(resumedGroups)));

                foreach (JobKey key in keys)
                {
                    IList<IOperableTrigger> triggers = GetTriggersForJob(key);
                    foreach (IOperableTrigger trigger in triggers)
                    {
                        ResumeTrigger(trigger.Key);
                    }
                }
            }

            return resumedGroups;
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                IOperableTrigger trigger = _db.Triggers
                    .Find(Builders<BsonDocument>.Filter.Eq("_id", triggerKey))
                    .As<IOperableTrigger>()
                    .FirstOrDefault();

                // does the trigger exist?
                if (trigger == null)
                {
                    return;
                }

                BsonDocument triggerState = _db.Triggers
                    .Find(Builders<BsonDocument>.Filter.Eq("_id", triggerKey))
                    .FirstOrDefault();

                // if the trigger is not paused resuming it does not make sense...
                if (triggerState["State"] != TriggerStates.PAUSED &&
                    triggerState["State"] != TriggerStates.PAUSED_AND_BLOCKED)
                {
                    return;
                }

                if (_db.BlockedJobs.Find(trigger.JobKey.ToBsonDocument()).Any())
                {
                    triggerState["State"] = TriggerStates.BLOCKED;
                }
                else
                {
                    triggerState["State"] = TriggerStates.WAITING;
                }

                ApplyMisfire(trigger);

                _db.Triggers.ReplaceOne(triggerState["_id"].AsBsonDocument, triggerState);
            }
        }

        protected virtual bool ApplyMisfire(IOperableTrigger trigger)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = trigger.GetNextFireTimeUtc();
            if (!tnft.HasValue || tnft.Value > misfireTime
                || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = RetrieveCalendar(trigger.CalendarName);
            }

            signaler.NotifyTriggerListenersMisfired(trigger);

            trigger.UpdateAfterMisfire(cal);
            StoreTrigger(trigger, true);

            if (!trigger.GetNextFireTimeUtc().HasValue)
            {
                _db.Triggers.UpdateOne(trigger.Key.ToBsonDocument(),
                    Builders<BsonDocument>.Update.Set("State", TriggerStates.COMPLETE));

                signaler.NotifySchedulerListenersFinalized(trigger);
            }
            else if (tnft.Equals(trigger.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            Collection.ISet<string> groups = new Collection.HashSet<string>();
            lock (lockObject)
            {
                Collection.ISet<TriggerKey> keys = this.GetTriggerKeys(matcher);

                foreach (TriggerKey triggerKey in keys)
                {
                    groups.Add(triggerKey.Group);
                    IOperableTrigger trigger = _db.Triggers
                        .Find(triggerKey.ToBsonDocument())
                        .As<IOperableTrigger>()
                        .FirstOrDefault();

                    var hasPausedJobGroup = _db.PausedJobGroups
                        .Find(trigger.JobKey.Group)
                        .As<string>()
                        .Any();

                    if (hasPausedJobGroup)
                        continue;

                    ResumeTrigger(triggerKey);
                }

                _db.PausedTriggerGroups.DeleteMany(Builders<BsonDocument>.Filter.In("_id", groups));
            }

            return new List<string>(groups);
        }

        public ICalendar RetrieveCalendar(string calName)
        {
            lock (lockObject)
            {
                var cal = _db.Calendars.Find(x => x["_id"].Equals(calName))
                    .As<CalendarModel>()
                    .FirstOrDefault();
                if (cal == null)
                    return null;
                return cal.Value;
            }
        }

        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            lock (lockObject)
            {
                return _db.Jobs
                   .Find(jobKey.ToBsonDocument())
                   .As<JobDetailImpl>()
                   .FirstOrDefault();
            }
        }

        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            lock (lockObject)
            {
                return _db.Triggers
                   .Find(triggerKey.ToBsonDocument())
                   .As<IOperableTrigger>()
                   .FirstOrDefault();
            }
        }

        public void SchedulerPaused()
        {
            lock (lockObject)
            {
                _db.Schedulers.UpdateOne(x => x["_id"].Equals(_instanceId),
                Builders<BsonDocument>.Update.Set("State", "Paused"), new UpdateOptions { IsUpsert = true });
            }
        }

        public void SchedulerResumed()
        {
            lock (lockObject)
            {
                _db.Schedulers.UpdateOne(x => x["_id"].Equals(_instanceId),
                Builders<BsonDocument>.Update.Set("State", "Resuming"), new UpdateOptions { IsUpsert = true });
            }
        }

        public void SchedulerStarted()
        {
        }

        public void Shutdown()
        {
            lock (lockObject)
            {
                _db.Schedulers.DeleteOne(x => x["_id"].Equals(_instanceId));

                _db.Triggers.UpdateOne(
                    x => x["SchedulerInstanceId"].Equals(_instanceId),
                    Builders<BsonDocument>.Update
                    .Unset("SchedulerInstanceId"));
            }
        }

        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            lock (lockObject)
            {
                var model = new CalendarModel()
                {
                    Name = name,
                    Value = calendar
                };

                if (CalendarExists(name))
                {
                    if (!replaceExisting)
                        throw new ObjectAlreadyExistsException("Calendar with name '" + name + "' already exists.");

                    _db.Calendars.ReplaceOne(Builders<BsonDocument>.Filter.Eq("_id", name), model.ToBsonDocument());
                }
                else
                {
                    _db.Calendars.InsertOne(model.ToBsonDocument());
                }

                if (updateTriggers)
                {
                    var triggers = _db.Triggers
                        .Find(x => x["CalendarName"].Equals(name))
                        .As<IOperableTrigger>()
                        .ToList();
                    foreach (var trigger in triggers)
                    {
                        trigger.UpdateWithNewCalendar(calendar, MisfireThreshold);
                        _db.Triggers.ReplaceOne(x => x["_id"].Equals(trigger.Key), trigger.ToBsonDocument());
                    }
                }
            }
        }

        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            lock (lockObject)
            {
                if (CheckExists(newJob.Key))
                {
                    if (!replaceExisting)
                    {
                        throw new ObjectAlreadyExistsException(newJob);
                    }

                    _db.Jobs.ReplaceOne(
                        Builders<BsonDocument>.Filter.Eq("_id", newJob.Key),
                        ((JobDetailImpl)newJob).ToBsonDocument());
                }
                else
                {
                    _db.Jobs.InsertOne(((JobDetailImpl)newJob).ToBsonDocument());
                }
            }
        }

        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            StoreJob(newJob, false);
            StoreTrigger(newTrigger, false);
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs, bool replace)
        {
            if (!replace)
            {
                foreach (IJobDetail job in triggersAndJobs.Keys)
                {
                    if (CheckExists(job.Key))
                    {
                        throw new ObjectAlreadyExistsException(job);
                    }
                    foreach (ITrigger trigger in triggersAndJobs[job])
                    {
                        if (CheckExists(trigger.Key))
                        {
                            throw new ObjectAlreadyExistsException(trigger);
                        }
                    }
                }
            }

            foreach (IJobDetail job in triggersAndJobs.Keys)
            {
                StoreJob(job, true);
                foreach (ITrigger trigger in triggersAndJobs[job])
                {
                    StoreTrigger((IOperableTrigger)trigger, true);
                }
            }
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            lock (lockObject)
            {
                if (CheckExists(newTrigger.Key))
                {
                    if (!replaceExisting)
                    {
                        throw new ObjectAlreadyExistsException(newTrigger);
                    }

                    _db.Triggers.ReplaceOne(
                        Builders<BsonDocument>.Filter.Eq("_id", newTrigger.Key),
                        newTrigger.ToBsonDocument());
                }
                else
                {
                    var state = TriggerStates.WAITING;

                    if (_db.PausedTriggerGroups.Find(x => x["_id"].Equals(newTrigger.Key.Group)).Any()
                        || _db.PausedJobGroups.Find(x => x["_id"].Equals(newTrigger.JobKey.Group)).Any())
                    {
                        state = TriggerStates.PAUSED;
                        if (_db.BlockedJobs.Find(Builders<BsonDocument>.Filter.Eq("_id", newTrigger.JobKey)).Any())
                        {
                            state = TriggerStates.PAUSED_AND_BLOCKED;
                        }
                    }
                    else if (_db.BlockedJobs.Find(Builders<BsonDocument>.Filter.Eq("_id", newTrigger.JobKey)).Any())
                    {
                        state = TriggerStates.BLOCKED;
                    }

                    var doc = newTrigger.ToBsonDocument();
                    doc.Add("State", state);

                    _db.Triggers.InsertOne(doc);
                }
            }
        }

        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            lock (lockObject)
            {
                ReleaseAcquiredTrigger(trigger);

                // It's possible that the job is null if:
                //   1- it was deleted during execution
                //   2- RAMJobStore is being used only for volatile jobs / triggers
                //      from the JDBC job store

                if (jobDetail.PersistJobDataAfterExecution)
                {
                    _db.Jobs.UpdateOne(jobDetail.Key.ToBsonDocument(),
                        Builders<BsonDocument>.Update.Set("JobDataMap", jobDetail.JobDataMap));
                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {
                    IList<IOperableTrigger> jobTriggers = GetTriggersForJob(jobDetail.Key);
                    IEnumerable<BsonDocument> triggerKeys = jobTriggers.Select(t => t.Key.ToBsonDocument());

                    var filter = Builders<BsonDocument>.Filter;
                    var update = Builders<BsonDocument>.Update;

                    _db.Triggers.UpdateMany(
                        filter.And(filter.In("_id", triggerKeys), filter.Eq("State", TriggerStates.BLOCKED)),
                        update.Set("State", TriggerStates.WAITING));


                    _db.Triggers.UpdateMany(
                        filter.And(
                            filter.In("_id", triggerKeys),
                            filter.Eq("State", TriggerStates.PAUSED_AND_BLOCKED)),
                        update.Set("State", TriggerStates.PAUSED));

                    signaler.SignalSchedulingChange(null);
                }

                // even if it was deleted, there may be cleanup to do
                _db.BlockedJobs.DeleteOne(jobDetail.Key.ToBsonDocument());

                // check for trigger deleted during execution...
                if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                {
                    DateTimeOffset? d = trigger.GetNextFireTimeUtc();
                    if (!d.HasValue)
                    {
                        // double check for possible reschedule within job 
                        // execution, which would cancel the need to delete...
                        d = trigger.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            RemoveTrigger(trigger.Key);
                        }
                        else
                        {
                            // log.Debug("Deleting cancelled - trigger still active");
                        }
                    }
                    else
                    {
                        RemoveTrigger(trigger.Key);
                        signaler.SignalSchedulingChange(null);
                    }
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                {
                    _db.Triggers.UpdateOne(trigger.Key.ToBsonDocument(),
                        Builders<BsonDocument>.Update.Set("State", TriggerStates.COMPLETE));

                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                {
                    Log.Info(string.Format(CultureInfo.InvariantCulture, "Trigger {0} set to ERROR state.", trigger.Key));
                    _db.Triggers.UpdateOne(trigger.Key.ToBsonDocument(),
                        Builders<BsonDocument>.Update.Set("State", TriggerStates.ERROR));

                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                {
                    Log.Info(string.Format(CultureInfo.InvariantCulture, "All triggers of Job {0} set to ERROR state.", trigger.JobKey));
                    IList<IOperableTrigger> jobTriggers = GetTriggersForJob(jobDetail.Key);
                    IEnumerable<BsonDocument> triggerKeys = jobTriggers.Select(t => t.Key.ToBsonDocument());
                    _db.Triggers.UpdateMany(
                        Builders<BsonDocument>.Filter.In("_id", triggerKeys),
                        Builders<BsonDocument>.Update.Set("State", TriggerStates.ERROR));

                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                {
                    IList<IOperableTrigger> jobTriggers = GetTriggersForJob(jobDetail.Key);
                    IEnumerable<BsonDocument> triggerKeys = jobTriggers.Select(t => t.Key.ToBsonDocument());
                    _db.Triggers.UpdateMany(
                        Builders<BsonDocument>.Filter.In("_id", triggerKeys),
                        Builders<BsonDocument>.Update.Set("State", TriggerStates.COMPLETE));

                    signaler.SignalSchedulingChange(null);
                }
            }
        }

        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            lock (lockObject)
            {
                List<TriggerFiredResult> results = new List<TriggerFiredResult>();

                foreach (IOperableTrigger trigger in triggers)
                {
                    // was the trigger deleted since being acquired?
                    if (!_db.Triggers.Find(trigger.Key.ToBsonDocument()).Any())
                    {
                        continue;
                    }
                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    BsonDocument triggerState = _db.Triggers.Find(trigger.Key.ToBsonDocument()).FirstOrDefault();
                    if (triggerState["State"] != TriggerStates.ACQUIRED)
                    {
                        continue;
                    }

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = RetrieveCalendar(trigger.CalendarName);
                        if (cal == null)
                        {
                            continue;
                        }
                    }

                    DateTimeOffset? prevFireTime = trigger.GetPreviousFireTimeUtc();

                    // call triggered on our copy, and the scheduler's copy
                    trigger.Triggered(cal);

                    var document = trigger.ToBsonDocument();
                    document.Add("State", TriggerStates.WAITING);
                    _db.Triggers.ReplaceOne(trigger.Key.ToBsonDocument(), document);

                    TriggerFiredBundle bndle = new TriggerFiredBundle(RetrieveJob(trigger.JobKey),
                                                                      trigger,
                                                                      cal,
                                                                      false, SystemTime.UtcNow(),
                                                                      trigger.GetPreviousFireTimeUtc(), prevFireTime,
                                                                      trigger.GetNextFireTimeUtc());

                    IJobDetail job = bndle.JobDetail;

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        var jobTriggers = GetTriggersForJob(job.Key);
                        IEnumerable<BsonDocument> triggerKeys = jobTriggers
                            .Where(t => !t.Key.Equals(trigger.Key))
                            .Select(t => t.Key.ToBsonDocument());

                        var Query = Builders<BsonDocument>.Filter;
                        var Update = Builders<BsonDocument>.Update;

                        _db.Triggers.UpdateMany(
                            Query.And(
                                Query.In("_id", triggerKeys),
                                Query.Eq("State", TriggerStates.WAITING)),
                            Update.Set("State", TriggerStates.BLOCKED));

                        _db.Triggers.UpdateMany(
                            Query.And(
                                Query.In("_id", triggerKeys),
                                Query.Eq("State", TriggerStates.PAUSED)),
                            Update.Set("State", TriggerStates.PAUSED_AND_BLOCKED));

                        var doc = new BsonDocument(
                                new BsonElement("_id", job.Key.ToBsonDocument()));

                        _db.BlockedJobs.ReplaceOne(doc, doc, new UpdateOptions { IsUpsert = true });
                    }

                    results.Add(new TriggerFiredResult(bndle));
                }
                return results;
            }
        }
    }
}
