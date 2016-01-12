using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            while (true)
            {
                try
                {
                    GrainClient.Initialize();
                    break;
                }
                catch (Exception)
                {
                    Task.Delay(100).Wait();
                }
            }

            var providerName = "PQSProvider";
            var streamId = new Guid("00000000-0000-0000-0000-000000000000");

            var provider = GrainClient.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(streamId, "GlobalNamespace");
            for (int i = 0; i < 1000; i++)
            {
                Task.Delay(500).Wait();
                stream.OnNextAsync(i);
                Console.WriteLine($"Writing....: {i}\t\t{DateTime.UtcNow.Millisecond}");
            }
        }
    }
}
