using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace PipeStreamProvider
{
    [Serializable]
    internal class PlainBatchContainer : IBatchContainer
    {
        public Guid StreamGuid { get; private set; }
        public string StreamNamespace { get; private set; }
        public StreamSequenceToken SequenceToken { get { return RealToken; } }
        public SimpleSequenceToken RealToken { get; set; }
        public List<object> Payload { get; private set; }

        public PlainBatchContainer(Guid streamGuid, string streamNamespace, List<object> payload)//, SimpleSequenceToken token)
        {
            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            this.Payload = payload;
            //this.RealToken = token;
        }

        public PlainBatchContainer(Guid streamGuid, string streamNamespace, List<object> payload, SimpleSequenceToken token) : this (streamGuid, streamNamespace, payload)
        {
            this.RealToken = token;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return Payload.OfType<T>().Select((e, i) => Tuple.Create(e, (StreamSequenceToken)RealToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ImportRequestContext()
        {
            return false;
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            return true;
        }

        public override string ToString()
        {
            return $"[{nameof(PlainBatchContainer)}:Stream= {StreamGuid}, Payload= {Payload}]";
        }
    }
}
