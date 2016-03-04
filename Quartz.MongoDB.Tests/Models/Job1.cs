using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.MongoDB.Tests.Models
{
    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    class Job1 : IJob
    {
        public static int Count = 0;

        public IJobExecutionContext ExecutionContext { get; set; }
        public void Execute(IJobExecutionContext context)
        {
            ExecutionContext = context;

            if (IsMissedfireExecution())
                return;

            Count++;
        }

        protected bool IsMissedfireExecution()
        {
            if (ExecutionContext != null && ExecutionContext.FireTimeUtc.HasValue && ExecutionContext.ScheduledFireTimeUtc.HasValue)
                return (ExecutionContext.FireTimeUtc.Value - ExecutionContext.ScheduledFireTimeUtc.Value).TotalSeconds >= 3;
            return false;
        }

    }
}
