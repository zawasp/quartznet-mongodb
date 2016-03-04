using MongoDB.Bson.Serialization.Serializers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;

namespace Quartz.MongoDB.Serialization
{
    internal class TimeOfDaySerializer : SerializerBase<TimeOfDay>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, TimeOfDay value)
        {
            context.Writer.WriteStartDocument();

            context.Writer.WriteName("Hour");
            context.Writer.WriteInt32(value.Hour);

            context.Writer.WriteName("Minute");
            context.Writer.WriteInt32(value.Minute);

            context.Writer.WriteName("Second");
            context.Writer.WriteInt32(value.Second);

            context.Writer.WriteEndDocument();
        }

        public override TimeOfDay Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            context.Reader.ReadStartDocument();

            var value = new TimeOfDay(
                context.Reader.ReadInt32(),
                context.Reader.ReadInt32(),
                context.Reader.ReadInt32());

            context.Reader.ReadEndDocument();

            return value;
        }
    }
}
