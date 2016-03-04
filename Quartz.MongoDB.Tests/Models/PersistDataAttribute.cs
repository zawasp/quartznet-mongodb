using System;

namespace Quartz.MongoDB.Tests.Models
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public class PersistDataAttribute : Attribute
    {
        public string Key { get; set; }
    }
}
