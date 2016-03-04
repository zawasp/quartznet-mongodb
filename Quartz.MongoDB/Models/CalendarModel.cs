using System;

namespace Quartz.MongoDB.Models
{
    /// <summary>
    /// Wrapper for ICalendar 
    /// </summary>
    internal class CalendarModel
    {
        public string Name { get; set; }

        public ICalendar Value { get; set; }
    }
}
