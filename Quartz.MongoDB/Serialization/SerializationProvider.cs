using MongoDB.Bson.Serialization;
using Quartz.Impl.Triggers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.MongoDB.Serialization
{
    internal class SerializationProvider
    {
        private static bool _registered = false;

        internal static void RegisterSerializers()
        {
            if (_registered)
                return;
            _registered = true;

            BsonSerializer.RegisterSerializer(new TriggerKeySerializer());
            BsonSerializer.RegisterSerializer(new JobKeySerializer());
            BsonSerializer.RegisterSerializer(new JobDataMapSerializer());
            BsonSerializer.RegisterSerializer(new TimeZoneInfoSerializer());
            BsonSerializer.RegisterSerializer(new TimeOfDaySerializer());
            BsonSerializer.RegisterSerializer(new TypeSerializer());
            BsonSerializer.RegisterSerializer(new JobDetailImplSerializer());
            BsonSerializer.RegisterSerializer(new CalendarModelSerializer());

            BsonClassMap.RegisterClassMap<AbstractTrigger>(m =>
            {
                m.AutoMap();

                m.MapIdProperty(x => x.Key);
                m.MapField(x => x.Name);
                m.MapField(x => x.Group);
                m.MapField(x => x.JobName);
                m.MapField(x => x.JobGroup);
                m.MapField(x => x.JobKey);
                m.MapField(x => x.Name);
                m.MapField(x => x.Group);
                m.MapField(x => x.Description);
                m.MapField(x => x.CalendarName);
                m.MapField(x => x.JobDataMap);
                m.MapField(x => x.MisfireInstruction);
                m.MapField(x => x.FireInstanceId);
                m.MapField(x => x.EndTimeUtc);
                m.MapField(x => x.StartTimeUtc);
                m.MapField(x => x.Priority);

                m.SetIsRootClass(true);
            });

            BsonClassMap.RegisterClassMap<CalendarIntervalTriggerImpl>(m =>
            {
                m.AutoMap();
                m.MapField("nextFireTimeUtc");
                m.MapField("previousFireTimeUtc");
                m.SetIgnoreExtraElements(true);
            });

            BsonClassMap.RegisterClassMap<CronTriggerImpl>(m =>
            {
                m.AutoMap();

                m.MapField(x => x.CronExpressionString);
                m.MapField(x => x.TimeZone);
                m.MapField("nextFireTimeUtc");
                m.MapField("previousFireTimeUtc");
                m.MapField(x => x.TimeZone);
                m.SetIgnoreExtraElements(true);
            });

            BsonClassMap.RegisterClassMap<DailyTimeIntervalTriggerImpl>(m =>
            {
                m.AutoMap();
                m.MapField("complete");
                m.MapField("nextFireTimeUtc");
                m.MapField("previousFireTimeUtc");
                m.MapField(x => x.TimeZone);
                m.SetIgnoreExtraElements(true);
            });

            BsonClassMap.RegisterClassMap<SimpleTriggerImpl>(m =>
            {
                m.AutoMap();
                m.MapField("nextFireTimeUtc");
                m.MapField("previousFireTimeUtc");
                m.SetIgnoreExtraElements(true);
            });
        }
    }
}
