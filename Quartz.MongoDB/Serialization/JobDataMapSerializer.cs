using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson;
using MongoDB.Bson.IO;

namespace Quartz.MongoDB.Serialization
{
    internal class JobDataMapSerializer : SerializerBase<JobDataMap>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, JobDataMap value)
        {
            context.Writer.WriteStartDocument();

            if (value != null)
            {
                foreach (var item in value)
                {
                    context.Writer.WriteName(item.Key);
                    BsonSerializer.Serialize(context.Writer, item.Value);
                }
            }

            context.Writer.WriteEndDocument();
        }

        public override JobDataMap Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var jobDataMap = new JobDataMap();

            context.Reader.ReadStartDocument();

            while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
                jobDataMap.Add(
                    context.Reader.ReadName(Utf8NameDecoder.Instance), 
                    BsonSerializer.Deserialize<object>(context.Reader));

            context.Reader.ReadEndDocument();

            return jobDataMap;
        }
    }
}
