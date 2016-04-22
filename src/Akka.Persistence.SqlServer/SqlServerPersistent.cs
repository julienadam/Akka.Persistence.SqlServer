using System;
using Akka.Actor;

namespace Akka.Persistence.SqlServer
{
    [Serializable]
    public class SqlServerPersistent : IPersistentRepresentation, IEquatable<IPersistentRepresentation>
    {
        public static readonly string Undefined = string.Empty;

        public object Payload { get; }

        public string Manifest { get; }

        public string PersistenceId { get; }

        public long SequenceNr { get; }

        public bool IsDeleted { get; }

        public IActorRef Sender { get; }

        public string WriterGuid { get; }

        public long GlobalSequenceNr { get; }

        public DateTime? Timestamp { get; }

        public SqlServerPersistent(object payload, DateTime? timestamp, long globalSequenceNr = 0, long sequenceNr = 0, string persistenceId = null, string manifest = null, bool isDeleted = false, IActorRef sender = null, string writerGuid = null)
        {
            Payload = payload;
            SequenceNr = sequenceNr;
            IsDeleted = isDeleted;
            Manifest = manifest ?? Persistent.Undefined;
            PersistenceId = persistenceId ?? Persistent.Undefined;
            Sender = sender;
            WriterGuid = writerGuid ?? Persistent.Undefined;
            GlobalSequenceNr = globalSequenceNr;
            Timestamp = timestamp;
        }

        public IPersistentRepresentation WithPayload(object payload)
        {
            return new SqlServerPersistent(payload, Timestamp, GlobalSequenceNr, SequenceNr, PersistenceId, Manifest, IsDeleted, Sender, WriterGuid);
        }

        public IPersistentRepresentation WithManifest(string manifest)
        {
            return Manifest != manifest ? new SqlServerPersistent(Payload, Timestamp, GlobalSequenceNr, SequenceNr, PersistenceId, manifest, IsDeleted, Sender, WriterGuid) : this;
        }

        public IPersistentRepresentation Update(long sequenceNr, string persistenceId, bool isDeleted, IActorRef sender, string writerGuid)
        {
            return new SqlServerPersistent(Payload, Timestamp, GlobalSequenceNr, sequenceNr, persistenceId, Manifest, isDeleted, sender, writerGuid);
        }

        public bool Equals(IPersistentRepresentation other)
        {
            if (other == null)
                return false;
            if (this == other)
                return true;
            if (object.Equals(Payload, other.Payload) && string.Equals(Manifest, other.Manifest) && (string.Equals(PersistenceId, other.PersistenceId) && SequenceNr == other.SequenceNr) && this.IsDeleted == other.IsDeleted && object.Equals((object)Sender, (object)other.Sender))
                return string.Equals(this.WriterGuid, other.WriterGuid);
            return false;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as IPersistentRepresentation);
        }

        public bool Equals(SqlServerPersistent other)
        {
            if (object.Equals(Payload, other.Payload) && string.Equals(Manifest, other.Manifest) && (string.Equals(PersistenceId, other.PersistenceId) && SequenceNr == other.SequenceNr) && this.IsDeleted == other.IsDeleted && object.Equals((object)Sender, (object)other.Sender))
                return string.Equals(WriterGuid, other.WriterGuid);
            return false;
        }

        public override int GetHashCode()
        {
            return ((((((Payload?.GetHashCode() ?? 0) * 397 ^ (Manifest?.GetHashCode() ?? 0)) * 397 ^ (PersistenceId?.GetHashCode() ?? 0)) * 397 ^ SequenceNr.GetHashCode()) * 397 ^ IsDeleted.GetHashCode()) * 397 ^ (Sender?.GetHashCode() ?? 0)) * 397 ^ (WriterGuid?.GetHashCode() ?? 0);
        }

        public override string ToString()
        {
            return $"Persistent<pid: {PersistenceId}, seqNr: {SequenceNr}, deleted: {IsDeleted}, manifest: {Manifest}, sender: {Sender}, payload: {Payload}, writerGuid: {WriterGuid}>";
        }
    }
}