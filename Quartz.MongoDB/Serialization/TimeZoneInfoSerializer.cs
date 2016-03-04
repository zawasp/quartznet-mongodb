using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Quartz.MongoDB.Serialization
{
    internal class TimeZoneInfoSerializer : SerializerBase<TimeZoneInfo>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, TimeZoneInfo value)
        {
            context.Writer.WriteString(value.Id);
        }

        public override TimeZoneInfo Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            return TimeZoneInfo.FindSystemTimeZoneById(context.Reader.ReadString());
        }
    }
}
