namespace Quartz.MongoDB
{
    public class Constants
    {
        internal const string DEFAULT_PREFIX = "qrtz.";

        internal const string DEFAULT_CONNECTION_STRING_NAME = "quartznet-mongodb";

        /// <summary>
        /// Trigger States
        /// </summary>
        internal class TriggerStates
        {
            internal const string NORMAL = "Normal";
            internal const string WAITING = "Waiting";
            internal const string PAUSED = "Paused";
            internal const string BLOCKED = "Blocked";
            internal const string PAUSED_AND_BLOCKED = "PausedAndBlocked";
            internal const string ACQUIRED = "Acquired";
            internal const string COMPLETE = "Complete";
            internal const string ERROR = "Error";
        }
    }
}
