using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Quartz.MongoDB.Serialization
{
    internal class JobKeySerializer : SerializerBase<JobKey>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, JobKey value)
        {
            context.Writer.WriteStartDocument();
            context.Writer.WriteName("Name");
            context.Writer.WriteString(value.Name);
            context.Writer.WriteName("Group");
            context.Writer.WriteString(value.Group);
            context.Writer.WriteEndDocument();
        }

        public override JobKey Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            context.Reader.ReadStartDocument();
            var key = new JobKey(context.Reader.ReadString(), context.Reader.ReadString());
            context.Reader.ReadEndDocument();

            return key;
        }
    }
}
