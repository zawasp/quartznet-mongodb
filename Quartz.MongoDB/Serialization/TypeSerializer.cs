using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.MongoDB.Serialization
{
    internal class TypeSerializer : SerializerBase<Type>
    {
        private static Assembly[] Assemblies = AppDomain.CurrentDomain.GetAssemblies().OrderBy(x => x.FullName).ToArray();
        private static char[] commaChar = new char[] { ',' };

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Type value)
        {
            if (value == null)
                context.Writer.WriteNull();
            else
            {
                context.Writer.WriteStartDocument();

                context.Writer.WriteName("FullName");
                context.Writer.WriteString(value.FullName);

                context.Writer.WriteName("Assembly");
                context.Writer.WriteString(value.Assembly.FullName.Split(commaChar, 2).FirstOrDefault());

                context.Writer.WriteEndDocument();
            }
        }

        public override Type Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {

            Type type = null;

            if (context.Reader.CurrentBsonType != BsonType.Null)
            {
                context.Reader.ReadStartDocument();
                var fullName = context.Reader.ReadString();
                var assemblyFullName = context.Reader.ReadString();

                var asm = Assemblies.FirstOrDefault(x => x.FullName.StartsWith(assemblyFullName));
                if (asm != null)
                    type = asm.GetType(fullName);
                context.Reader.ReadEndDocument();
            }


            return type;
        }
    }
}
