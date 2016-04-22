using System;
using System.Data.Common;
using Akka.Actor;
using Akka.Persistence.Sql.Common.Journal;

namespace Akka.Persistence.SqlServer
{
    /// <summary>
    /// Default implementation of <see cref="IJournalQueryMapper"/> used for mapping data 
    /// returned from ADO.NET data readers back to <see cref="IPersistentRepresentation"/> messages.
    /// </summary>
    internal class SqlServerJournalQueryMapper : IJournalQueryMapper
    {
        public const int PersistenceIdIndex = 0;
        public const int SequenceNrIndex = 1;
        public const int IsDeletedIndex = 2;
        public const int ManifestIndex = 3;
        public const int PayloadIndex = 4;
        public const int TimestampIndex = 5;
        public const int GlobalSequenceNrIndex = 6;
        
        private readonly Akka.Serialization.Serialization _serialization;

        public SqlServerJournalQueryMapper(Akka.Serialization.Serialization serialization)
        {
            _serialization = serialization;
        }

        public IPersistentRepresentation Map(DbDataReader reader, IActorRef sender = null)
        {
            var persistenceId = reader.GetString(PersistenceIdIndex);
            var sequenceNr = reader.GetInt64(SequenceNrIndex);
            var isDeleted = reader.GetBoolean(IsDeletedIndex);
            var manifest = reader.GetString(ManifestIndex);

            // timestamp is SQL-journal specific field, but it's useful :D
            var globalSequenceNr = reader.GetInt64(GlobalSequenceNrIndex);
            var timestamp = reader.GetDateTime(TimestampIndex);

            var payload = GetPayload(reader, manifest);

            return new SqlServerPersistent(payload, timestamp, globalSequenceNr, sequenceNr, manifest, persistenceId, isDeleted, sender);
        }

        private object GetPayload(DbDataReader reader, string manifest)
        {
            var type = Type.GetType(manifest, true);
            var binary = (byte[])reader[PayloadIndex];

            var serializer = _serialization.FindSerializerForType(type);
            return serializer.FromBinary(binary, type);
        }
    }
}