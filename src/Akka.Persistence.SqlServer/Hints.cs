using Akka.Persistence.Sql.Common.Queries;

namespace Akka.Persistence.SqlServer
{
    public static class Hints
    {
        public static IHint FromGlobalSequenceNumber(long globalSequenceNr)
        {
            return new FromGlobalSequenceNumberHint(globalSequenceNr);
        }
    }

    public class FromGlobalSequenceNumberHint : IHint
    {
        public long GlobalSequenceNr { get; }

        public FromGlobalSequenceNumberHint(long globalSequenceNr)
        {
            GlobalSequenceNr = globalSequenceNr;
        }
    }
}
