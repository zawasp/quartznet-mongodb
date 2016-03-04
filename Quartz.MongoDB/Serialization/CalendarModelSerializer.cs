using MongoDB.Bson.Serialization.Serializers;
using Quartz.MongoDB.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using MongoDB.Bson;

namespace Quartz.MongoDB.Serialization
{
    internal class CalendarModelSerializer : SerializerBase<CalendarModel>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, CalendarModel value)
        {
            context.Writer.WriteStartDocument();
            context.Writer.WriteName("_id");
            context.Writer.WriteString(value.Name);
            using (var stream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(stream, value.Value);
                context.Writer.WriteName("ContentStream");
                context.Writer.WriteBinaryData(
                    new BsonBinaryData(stream.ToArray(), BsonBinarySubType.Binary));
            }
            context.Writer.WriteEndDocument();
        }

        public override CalendarModel Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var item = new CalendarModel();

            context.Reader.ReadStartDocument();
            item.Name = context.Reader.ReadString();
            var binaryData = context.Reader.ReadBinaryData();
            using (var ms = new MemoryStream(binaryData.Bytes))
            {
                item.Value = (ICalendar)new BinaryFormatter().Deserialize(ms);
            }
            context.Reader.ReadEndDocument();

            return item;
        }
    }
}
