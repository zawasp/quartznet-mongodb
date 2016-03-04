using Common.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.MongoDB.Tests.Models
{
    public abstract class BaseJob : IJob, IDisposable
    {
        private ILog _log = null;

        public BaseJob()
        {
        }

        protected IJobExecutionContext ExecutionContext { get; set; }

        protected ILog Log
        {
            get { return _log ?? (_log = ResolveLogger()); }
        }

        public void Execute(IJobExecutionContext context)
        {
            ExecutionContext = context;

            // ignore if this execution is missed fire
            if (IsMissedfireExecution())
            {
                Log.Warn("Missed execution");
                return;
            }

            try
            {
                LoadProperties();

                OnInitialize();

                BeforeExecuting();

                OnExecute();

                AfterExecuted();
            }
            catch (Exception ex)
            {
                OnException(ex);
            }
            finally
            {
                SaveProperties();

                OnFinalize();
            }
        }

        protected virtual void OnInitialize()
        {
            Log.Info("Initializing");
        }
        protected virtual void BeforeExecuting()
        {
            Log.Info("Executing");
        }
        protected virtual void AfterExecuted()
        {
            Log.Info("Executed");
        }
        protected virtual void OnException(Exception ex)
        {
            Log.Error(ex);
        }
        protected virtual void OnExecute() { }
        protected virtual void OnFinalize()
        {
            Log.Info("Finalizing");
        }
        protected virtual ILog ResolveLogger()
        {
            if (ExecutionContext != null)
                return LogManager.GetLogger(ExecutionContext.Trigger.Key.ToString());
            return LogManager.GetLogger(GetType());
        }

        protected bool IsMissedfireExecution()
        {
            if (ExecutionContext != null && ExecutionContext.FireTimeUtc.HasValue && ExecutionContext.ScheduledFireTimeUtc.HasValue)
                return (ExecutionContext.FireTimeUtc.Value - ExecutionContext.ScheduledFireTimeUtc.Value).TotalSeconds >= 3;
            return false;
        }

        private void LoadProperties()
        {
            if (ExecutionContext == null)
                return;

            var jobData = ExecutionContext.JobDetail.JobDataMap;
            var props = GetType().GetProperties();
            foreach (var p in props)
            {
                var attr = p.GetCustomAttribute<PersistDataAttribute>();
                if (attr == null)
                    continue;
                LoadPropertyFromJobData(p, attr.Key ?? p.Name, jobData);
            }
        }
        private void SaveProperties()
        {
            if (ExecutionContext == null)
                return;

            var jobData = ExecutionContext.JobDetail.JobDataMap;
            var props = GetType().GetProperties();
            foreach (var p in props)
            {
                var attr = p.GetCustomAttribute<PersistDataAttribute>();
                if (attr == null)
                    continue;
                SavePropertyToJobData(p, attr.Key ?? p.Name, jobData);
            }
        }

        protected void LoadPropertyFromJobData(PropertyInfo propertyInfo, string key, JobDataMap jobData = null)
        {
            if (jobData == null)
            {
                if (ExecutionContext == null)
                    return;
                jobData = ExecutionContext.JobDetail.JobDataMap;
            }

            key = key ?? propertyInfo.Name;

            if (jobData.ContainsKey(key))
            {
                var o = jobData.Get(key);
                if (o != null)
                {
                    var value = JsonConvert.DeserializeObject(o.ToString(), propertyInfo.PropertyType);
                    propertyInfo.SetValue(this, value, null);
                }
            }
        }
        protected void SavePropertyToJobData(PropertyInfo propertyInfo, string key, JobDataMap jobData = null)
        {
            if (jobData == null)
            {
                if (ExecutionContext == null)
                    return;
                jobData = ExecutionContext.JobDetail.JobDataMap;
            }

            key = key ?? propertyInfo.Name;

            var value = JsonConvert.SerializeObject(propertyInfo.GetValue(this));

            if (jobData.ContainsKey(key))
                jobData[key] = value;
            else
                jobData.Add(key, value);
        }

        #region IDisposable members

        private bool _disposed = false;
        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void OnDispose() { }

        private void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                try
                {
                    OnDispose();
                }
                catch { }

                GC.SuppressFinalize(this);

                _disposed = true;
            }
        }

        ~BaseJob()
        {
            Dispose(false);
        }
        #endregion
    }

    public abstract class BaseJob<TParam> : BaseJob
        where TParam : class, new()
    {
        protected TParam Parameters { get; set; }

        protected override void OnInitialize()
        {
            base.OnInitialize();

            LoadPropertyFromJobData(
                GetType().GetProperty("Parameters", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static),
                "parameters", ExecutionContext.Trigger.JobDataMap);
        }
    }
}
