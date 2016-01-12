using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Client
{
    public class TestObserver : IAsyncObserver<int>
    {
        public async Task Subscribe()
        {
            var providerName = "PQSProvider";
            var streamId = new Guid("00000000-0000-0000-0000-000000000000");

            var provider = GrainClient.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(streamId, "GlobalNamespace");
            await stream.SubscribeAsync(this);
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            Console.WriteLine($"{item}");
            return TaskDone.Done;
        }

        public Task OnCompletedAsync()
        {
            throw new NotImplementedException();
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }
    }
}