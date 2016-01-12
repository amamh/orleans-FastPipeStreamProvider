using System;
using System.Collections.Concurrent;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace PipeStreamProvider.MemoryCache
{
    class MySimpleQueueAdapterCache : IQueueAdapterCache
    {
        private readonly Logger _logger;
        private IObjectPool<FixedSizeBuffer> bufferPool;

        public MySimpleQueueAdapterCache(IQueueAdapterFactory factory, Logger logger)
        {
            _logger = logger;
            // 10 meg buffer pool.  10 1 meg blocks
            bufferPool = new FixedSizeObjectPool<FixedSizeBuffer>(10, pool => new FixedSizeBuffer(1 << 20, pool));
        }

        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return new FastPipeQueueCache(bufferPool, queueId);
        }

        public int Size => 0; // FIXME
    }
}