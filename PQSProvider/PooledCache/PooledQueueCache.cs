
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Common
{
    public class PooledQueueCache<TQueueMessage, TCachedMessage>
        where TQueueMessage : class
        where TCachedMessage : struct
    {
        // linked list of message bocks.  First is newest.
        private readonly LinkedList<CachedMessageBlock<TQueueMessage, TCachedMessage>> messageBlocks;
        private readonly ConcurrentQueue<IDisposable> purgeQueue;
        private readonly CachedMessagePool<TQueueMessage, TCachedMessage> pool;
        private readonly ICacheDataAdapter<TQueueMessage, TCachedMessage> cacheDataAdapter;

        private class Cursor
        {
            public readonly Guid StreamGuid;
            public readonly string StreamNamespace;

            public Cursor(Guid streamGuid, string streamNamespace)
            {
                StreamGuid = streamGuid;
                StreamNamespace = streamNamespace;
                State = States.Unset;
            }

            public enum States
            {
                Unset, // Not yet set, or points to some data in the future.
                Set, // Points to a message in the cache
                Idle, // Has ittereted over all relivant events in the cache and is waiting for more data on the stream.
            }
            public States State;

            // current sequence token
            public StreamSequenceToken SequenceToken;

            // reference in cache
            public LinkedListNode<CachedMessageBlock<TQueueMessage, TCachedMessage>> CurrentBlock;
            public int Index;

            // utilities
            public bool IsNewestInBlock { get { return Index == CurrentBlock.Value.NewestMessageIndex; } }
            public TCachedMessage Message { get { return CurrentBlock.Value[Index]; } }
        }

        public PooledQueueCache(ICacheDataAdapter<TQueueMessage, TCachedMessage> cacheDataAdapter)
        {
            if (cacheDataAdapter == null)
            {
                throw new ArgumentNullException("cacheDataAdapter");
            }
            this.cacheDataAdapter = cacheDataAdapter;
            pool = new CachedMessagePool<TQueueMessage, TCachedMessage>(cacheDataAdapter);
            purgeQueue = new ConcurrentQueue<IDisposable>();
            messageBlocks = new LinkedList<CachedMessageBlock<TQueueMessage, TCachedMessage>>();
        }

        public bool IsEmpty
        {
            get
            {
                return messageBlocks.Count == 0 || (messageBlocks.Count == 1 && messageBlocks.First.Value.IsEmpty);
            }
        }

        /// <summary>
        /// Acquires a cursor to enumerate through the messages in the cache at the provided sequenceToken, 
        ///   filtered on the specified stream.
        /// </summary>
        /// <param name="streamGuid">Key to filter to</param>
        /// <param name="streamNamespace">Namespace to filter to</param>
        /// <param name="sequenceToken"></param>
        /// <returns></returns>
        public object GetCursor(Guid streamGuid, string streamNamespace, StreamSequenceToken sequenceToken)
        {
            var cursor = new Cursor(streamGuid, streamNamespace);
            SetCursor(cursor, sequenceToken);
            return cursor;
        }

        private void SetCursor(Cursor cursor, StreamSequenceToken sequenceToken)
        {
            // If nothing in cache, unset token, and wait for more data.
            if (messageBlocks.Count == 0)
            {
                cursor.State = Cursor.States.Unset;
                cursor.SequenceToken = sequenceToken;
                return;
            }

            LinkedListNode<CachedMessageBlock<TQueueMessage, TCachedMessage>> newestBlock = messageBlocks.First;

            // if sequenceToken is null, iterate from newest message in cache
            if (sequenceToken == null)
            {
                cursor.State = Cursor.States.Idle;
                cursor.CurrentBlock = newestBlock;
                cursor.Index = newestBlock.Value.NewestMessageIndex;
                cursor.SequenceToken = newestBlock.Value.NewestSequenceToken;
                return;
            }

            // If sequenceToken is too new to be in cache, unset token, and wait for more data.
            TCachedMessage newestMessage = newestBlock.Value.NewestMessage;
            if (cacheDataAdapter.CompareCachedMessageToSequenceToken(ref newestMessage, sequenceToken) < 0) 
            {
                cursor.State = Cursor.States.Unset;
                cursor.SequenceToken = sequenceToken;
                return;
            }

            // Check to see if sequenceToken is too old to be in cache
            TCachedMessage oldestMessage = messageBlocks.Last.Value.OldestMessage;
            if (cacheDataAdapter.CompareCachedMessageToSequenceToken(ref oldestMessage, sequenceToken) > 0)
            {
                // throw cache miss exception
                throw new QueueCacheMissException(sequenceToken, messageBlocks.Last.Value.OldestSequenceToken, messageBlocks.First.Value.NewestSequenceToken);
            }

            // Find block containing sequence number, starting from the newest and working back to oldest
            LinkedListNode<CachedMessageBlock<TQueueMessage, TCachedMessage>> node = messageBlocks.First;
            while (true)
            {
                TCachedMessage oldestMessageInBlock = node.Value.OldestMessage;
                if (cacheDataAdapter.CompareCachedMessageToSequenceToken(ref oldestMessageInBlock, sequenceToken) <= 0)
                {
                    break;
                }
                node = node.Next;
            }

            // return cursor from start.
            cursor.CurrentBlock = node;
            cursor.Index = node.Value.GetIndexOfFirstMessageLessThanOrEqualTo(sequenceToken);
            // if cursor has been idle, move to next message after message specified by sequenceToken  
            if(cursor.State == Cursor.States.Idle)
            {
                // if there are more messages in this block, move to next message
                if (!cursor.IsNewestInBlock)
                {
                    cursor.Index++;
                }
                // if this is the newest message in this block, move to oldest message in newer block
                else if (node.Previous != null)
                {
                    cursor.CurrentBlock = node.Previous;
                    cursor.Index = cursor.CurrentBlock.Value.OldestMessageIndex;
                }
                else
                {
                    cursor.State = Cursor.States.Idle;
                    return;
                }
            }
            cursor.SequenceToken = cursor.CurrentBlock.Value.GetSequenceToken(cursor.Index);
            cursor.State = Cursor.States.Set;
        }

        /// <summary>
        /// Acquires the next message in the cache at the provided cursor
        /// </summary>
        /// <param name="cursorObj"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public bool TryGetNextMessage(object cursorObj, out IBatchContainer message)
        {
            message = null;

            if (cursorObj == null)
            {
                throw new ArgumentNullException("cursorObj");
            }

            var cursor = cursorObj as Cursor;
            if (cursor == null)
            {
                throw new ArgumentOutOfRangeException("cursorObj", "Cursor is bad");
            }

            if (cursor.State != Cursor.States.Set)
            {
                SetCursor(cursor, cursor.SequenceToken);
                if (cursor.State != Cursor.States.Set)
                {
                    return false;
                }
            }

            // has this message been purged
            TCachedMessage oldestMessage = messageBlocks.Last.Value.OldestMessage;
            if (cacheDataAdapter.CompareCachedMessageToSequenceToken(ref oldestMessage, cursor.SequenceToken) > 0)
            {
                throw new QueueCacheMissException(cursor.SequenceToken, messageBlocks.Last.Value.OldestSequenceToken, messageBlocks.First.Value.NewestSequenceToken);
            }

            // Iterate forward (in time) in the cache until we find a message on the stream or run out of cached messages.
            // Note that we get the message from the current cursor location, then move it forward.  This means that if we return true, the cursor
            //   will point to the next message after the one we're returning.
            while (cursor.State == Cursor.States.Set)
            {
                TCachedMessage currentMessage = cursor.Message;

                // Have we caught up to the newest event, if so set cursor to idle.
                if (cursor.CurrentBlock == messageBlocks.First && cursor.IsNewestInBlock)
                {
                    cursor.State = Cursor.States.Idle;
                    cursor.SequenceToken = messageBlocks.First.Value.NewestSequenceToken;
                }
                else // move to next
                {
                    int index;
                    if (cursor.IsNewestInBlock)
                    {
                        cursor.CurrentBlock = cursor.CurrentBlock.Previous;
                        cursor.CurrentBlock.Value.TryFindFirstMessage(cursor.StreamGuid, cursor.StreamNamespace, out index);
                    }
                    else
                    {
                        cursor.CurrentBlock.Value.TryFindNextMessage(cursor.Index + 1, cursor.StreamGuid, cursor.StreamNamespace, out index);
                    }
                    cursor.Index = index;
                }

                // check if this message is in the cursor's stream
                if (cacheDataAdapter.IsInStream(ref currentMessage, cursor.StreamGuid, cursor.StreamNamespace))
                {
                    message = cacheDataAdapter.GetBatchContainer(ref currentMessage);
                    cursor.SequenceToken = cursor.CurrentBlock.Value.GetSequenceToken(cursor.Index);
                    return true;
                }
            }

            return false;
        }

        public void Add(TQueueMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            PerformPendingPurges();

            // allocate message from pool
            CachedMessageBlock<TQueueMessage, TCachedMessage> block = pool.AllocateMessage(message);

            // If new block, add message block to linked list
            if (block != messageBlocks.FirstOrDefault())
                messageBlocks.AddFirst(block.Node);

            PerformPendingPurges();
        }

        /// <summary>
        /// Record that we need to purge all messages associated with purge request in the next purge cycle
        /// </summary>
        /// <param name="purgeRequest"></param>
        public void Purge(IDisposable purgeRequest)
        {
            purgeQueue.Enqueue(purgeRequest);
        }

        private void PerformPendingPurges()
        {
            uint purgeCount = 0;

            // If there are purge requests, perform purges
            if (!purgeQueue.IsEmpty)
            {
                IDisposable purgeRequest;
                while (purgeQueue.TryDequeue(out purgeRequest))
                {
                    purgeCount += PerformBlockPurge(purgeRequest);
                }
            }
        }

        private uint PerformBlockPurge(IDisposable purgeRequest)
        {
            uint purgeCount = 0;
            while (!IsEmpty &&
                   cacheDataAdapter.ShouldPurge(messageBlocks.Last.Value[messageBlocks.Last.Value.OldestMessageIndex], purgeRequest)) // value has the same resource blockId as the one we are purging
            {
                messageBlocks.Last.Value.Remove();
                // if block is currently empty, but all capacity has been exausted, remove
                if (messageBlocks.Last.Value.IsEmpty && !messageBlocks.Last.Value.HasCapacity)
                {
                    messageBlocks.Last.Value.Dispose();
                    messageBlocks.RemoveLast();
                }
                purgeCount++;
            }

            purgeRequest.Dispose();

            return purgeCount;
        }
    }
}
