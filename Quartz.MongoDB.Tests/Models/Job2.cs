using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.MongoDB.Tests.Models
{
    class Job2 : BaseJob<Job2Param>
    {
        public static int Count = 0;

        [PersistData]
        public State State { get; set; }

        protected override void OnExecute()
        {
            Count++;
            Log.InfoFormat("Parameter.Name: {0}", Parameters.Name);
        }
    }


    class Job2Param
    {
        public string Name { get; set; }
    }

    class State
    {
        public int Id { get; set; }
    }
}
