using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using System;
using MongoDB.Bson.IO;
using Quartz.Impl;

namespace Quartz.MongoDB.Serialization
{
    internal class JobDetailImplSerializer : SerializerBase<JobDetailImpl>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, JobDetailImpl value)
        {
            var w = context.Writer;

            w.WriteStartDocument();

            w.WriteName("_id");
            BsonSerializer.Serialize(w, value.Key);
            w.WriteName("JobType");
            BsonSerializer.Serialize(w, value.JobType);
            w.WriteName("Name");
            w.WriteString(value.Name);
            w.WriteName("Group");
            w.WriteString(value.Group);
            w.WriteName("RequestsRecovery");
            w.WriteBoolean(value.RequestsRecovery);
            w.WriteName("Durable");
            w.WriteBoolean(value.Durable);
            w.WriteName("Description");
            if (value.Description == null)
                w.WriteNull();
            else
                w.WriteString(value.Description);
            w.WriteName("JobDataMap");
            BsonSerializer.Serialize(w, value.JobDataMap);

            w.WriteEndDocument();
        }

        public override JobDetailImpl Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var r = context.Reader;

            r.ReadStartDocument();

            var jobDetail = new JobDetailImpl();
            r.ReadBsonType();
            jobDetail.Key = BsonSerializer.Deserialize<JobKey>(r);
            r.ReadBsonType();
            jobDetail.JobType = BsonSerializer.Deserialize<Type>(r);
            jobDetail.Name = r.ReadString();
            jobDetail.Group = r.ReadString();
            jobDetail.RequestsRecovery = r.ReadBoolean();
            jobDetail.Durable = r.ReadBoolean();
            r.ReadName(Utf8NameDecoder.Instance);
            if (r.CurrentBsonType != BsonType.Null)
                jobDetail.Description = r.ReadString();
            else
                r.ReadNull();
            r.ReadName(Utf8NameDecoder.Instance);
            if (r.CurrentBsonType != BsonType.Null)
                jobDetail.JobDataMap = BsonSerializer.Deserialize<JobDataMap>(r);
            else
                r.ReadNull();

            r.ReadEndDocument();

            return jobDetail;
        }
    }
}
