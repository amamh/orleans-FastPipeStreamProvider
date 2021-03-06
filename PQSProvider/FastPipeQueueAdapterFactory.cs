﻿using System;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using StackExchange.Redis;

namespace PipeStreamProvider
{
    public class FastPipeQueueAdapterFactory : IQueueAdapterFactory
    {
        //private const string CacheSizeParam = "CacheSize";
        //private const int DefaultCacheSize = 4096;
        //private int _cacheSize;

        private const string NumQueuesParam = "NumQueues";
        private const int DefaultNumQueues = 8; // keep as power of 2.
        private int _numQueues;

        private const string ServerParam = "Server";
        private const string DefaultServer = "localhost:6379";
        private string _server;

        private const string RedisDbParam = "RedisDb";
        private const int DefaultRedisDb = -1;
        private int _databaseNum;

        // TODO: This should be an enum to choose which physical queue to use
        private const string UseRedisForQueueParam = "UseRedisForQueue";
        private const bool DefaultUseRedisForQueue = false;
        private bool _useRedisForQueue;

        private string _providerName;
        private Logger _logger;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        private ConnectionMultiplexer _redisConn; // TODO: Dispose
        private IDatabase _redisDb;
        private IProviderConfiguration _config;
        private IServiceProvider _serviceProvider;
        private IStreamFailureHandler _failureHandler;

        public void Init(IProviderConfiguration config, string providerName, Logger logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _providerName = providerName;
            _config = config;
            _serviceProvider = serviceProvider;

            // Cache size
            //string cacheSizeString;
            //_cacheSize = DefaultCacheSize;
            //if (config.Properties.TryGetValue(CacheSizeParam, out cacheSizeString))
            //{
            //    if (!int.TryParse(cacheSizeString, out _cacheSize))
            //        throw new ArgumentException($"{CacheSizeParam} invalid.  Must be int");
            //}

            // # queues
            string numQueuesString;
            _numQueues = DefaultNumQueues;
            if (config.Properties.TryGetValue(NumQueuesParam, out numQueuesString))
            {
                if (!int.TryParse(numQueuesString, out _numQueues))
                    throw new ArgumentException($"{NumQueuesParam} invalid.  Must be int");
            }

            // Use Redis for queue?
            string useRedis;
            _useRedisForQueue = DefaultUseRedisForQueue;
            if (config.Properties.TryGetValue(UseRedisForQueueParam, out useRedis))
            {
                if (!bool.TryParse(useRedis, out _useRedisForQueue))
                    throw new ArgumentException($"{UseRedisForQueueParam} invalid value {useRedis}");
            }

            if (_useRedisForQueue)
                ReadRedisConnectionParams(config);

            _streamQueueMapper = new HashRingBasedStreamQueueMapper(_numQueues, providerName);
        }

        private void ReadRedisConnectionParams(IProviderConfiguration config)
        {
            // server
            string server;
            _server = DefaultServer;
            if (config.Properties.TryGetValue(ServerParam, out server))
            {
                if (server == "")
                    throw new ArgumentException($"{DefaultServer} invalid. Must not be empty");
                _server = server;
            }

            // db
            string dbNum;
            _databaseNum = DefaultRedisDb;
            if (config.Properties.TryGetValue(RedisDbParam, out dbNum))
            {
                if (!int.TryParse(dbNum, out _databaseNum))
                    throw new ArgumentException($"{RedisDbParam} invalid.  Must be int");
                if (_databaseNum > 15 || _databaseNum < 0)
                    throw new ArgumentException($"{RedisDbParam} invalid.  Must be from 0 to 15");
            }
        }

        public async Task<IQueueAdapter> CreateAdapter()
        {
            // In AzureQueueAdapterFactory an adapter is made per call, so we do the same
            IQueueAdapter adapter;
            if (_useRedisForQueue)
            {
                //adapter = new PhysicalQueues.Redis.RedisQueueAdapter(_logger, GetStreamQueueMapper(), _providerName, _server, _databaseNum);
                var redisQueueProvider = new PhysicalQueues.Redis.RedisQueueProvider();
                var redisAdapter = new PhysicalQueues.GenericQueueAdapter(_logger, GetStreamQueueMapper(), _providerName, _config, redisQueueProvider, _numQueues);
                await redisAdapter.Init();
                adapter = redisAdapter;
            }
            else
            {
                var memoryQueueProvider = new PhysicalQueues.Memory.MemoryQueueProvider();
                adapter = new PhysicalQueues.GenericQueueAdapter(_logger, GetStreamQueueMapper(), _providerName, _config, memoryQueueProvider, _numQueues);
            }

            return adapter;
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache ?? (_adapterCache = new MemoryCache.MySimpleQueueAdapterCache(this, _logger));
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _streamQueueMapper;
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult<IStreamFailureHandler>(
                _failureHandler ?? (_failureHandler = new LoggerStreamFailureHandler(_logger))
                );
        }

        //private void MakeSureRedisConnected()
        //{
        //    if (_redisConn?.IsConnected == true)
        //        return;

        //    // Note: using non-async Connect doesn't work
        //    _redisConn = ConnectionMultiplexer.ConnectAsync(_server).Result;
        //    _redisDb = _redisConn.GetDatabase(_databaseNum);
        //}
    }
}