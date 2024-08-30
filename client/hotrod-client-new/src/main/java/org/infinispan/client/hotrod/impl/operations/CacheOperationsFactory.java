package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.commons.util.IntSet;

public class CacheOperationsFactory {
   private final InternalRemoteCache<?, ?> remoteCache;

   public CacheOperationsFactory(InternalRemoteCache<?, ?> remoteCache) {
      this.remoteCache = Objects.requireNonNull(remoteCache);
   }

   public InternalRemoteCache<?, ?> getRemoteCache() {
      return remoteCache;
   }

   public <V> HotRodOperation<V> newGetOperation(Object key) {
      // TODO: need to support when storage is object based
      return new GetOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key));
   }

   public HotRodOperation<PingResponse> newPingOperation() {
      return new CachePingOperation(remoteCache.getName());
   }

   public <T> HotRodOperation<T> executeOperation(String taskName, Map<String, byte[]> marshalledParams, Object key) {
      return new CacheExecuteOperation<>(remoteCache, taskName, marshalledParams, key);
   }

   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                                     List<Modification> modifications,
                                                                     boolean recoverable, long timeoutMs) {
      return new PrepareTransactionOperation(remoteCache, xid, onePhaseCommit, modifications, recoverable, timeoutMs);
   }

   public HotRodOperation<Void> newRemoveClientListenerOperation(Object listener) {
      ClientListenerNotifier cln = remoteCache.getDispatcher().getClientListenerNotifier();
      byte[] listenerId = cln.findListenerId(listener);
      if (listenerId == null) {
         return NoHotRodOperation.instance();
      }
      return new RemoveClientListenerOperation(remoteCache, cln, listenerId);
   }

   public HotRodOperation<IterationStartResponse> newIterationStartOperation(String filterConverterFactory, byte[][] filterParams,
                                                                             IntSet segments, int batchSize, boolean metadata) {
      return new IterationStartOperation(remoteCache, filterConverterFactory, filterParams, segments, batchSize,
            metadata);
   }

   public <K, E> HotRodOperation<IterationNextResponse<K, E>> newIterationNextOperation(byte[] iterationId, KeyTracker segmentKeyTracker) {
      return new IterationNextOperation<>(remoteCache, iterationId, segmentKeyTracker);
   }

   public HotRodOperation<IterationEndResponse> newIterationEndOperation(byte[] iterationId) {
      return new IterationEndOperation(remoteCache, iterationId);
   }

   public ClearOperation newClearOperation() {
      return new ClearOperation(remoteCache);
   }

   public <V, K> HotRodOperation<V> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      // TODO: need to support when storage is object based
      return new PutOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public <V> HotRodOperation<V> newRemoveOperation(Object key) {
      return new RemoveOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key));
   }

   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key) {
      return new ContainsKeyOperation(remoteCache, remoteCache.getDataFormat().keyToBytes(key));
   }

   public <V, K> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit,
                                                         long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new ReplaceOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(key), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                                 long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new PutIfAbsentOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                                 long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return new PutIfAbsentOperation<>(remoteCache.withFlags(flags), remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public HotRodOperation<ServerStatistics> newStatsOperation() {
      return new StatsOperation(remoteCache);
   }

   public SizeOperation newSizeOperation() {
      return new SizeOperation(remoteCache);
   }

   public PutAllOperation newPutAllOperation(Map<byte[],byte[]> byteMap, long lifespan, TimeUnit lifespanUnit,
                                                     long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new PutAllOperation(remoteCache, byteMap, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public <V, K> GetWithMetadataOperation<V> newGetWithMetadataOperation(K key, SocketAddress preferredAddres) {
      return new GetWithMetadataOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key), preferredAddres);
   }

   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan,
                                                                                             TimeUnit lifespanTimeUnit, long maxIdle,
                                                                                             TimeUnit maxIdleTimeUnit, long version) {
      return new ReplaceIfUnmodifiedOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key),
            remoteCache.getDataFormat().valueToBytes(key), lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
   }

   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return new RemoveIfUnmodifiedOperation<>(remoteCache, remoteCache.getDataFormat().keyToBytes(key), version);
   }

   public <K, V> HotRodOperation<Map<K,V>> newGetAllOperation(Set<byte[]> byteKeys) {
      return new GetAllOperation<>(remoteCache, byteKeys);
   }

   public HotRodOperation<Void> newUpdateBloomFilterOperation(byte[] bloomFilterBits) {
      return new UpdateBloomFilterOperation(remoteCache, bloomFilterBits);
   }

   public ClientListenerOperation newAddNearCacheListenerOperation(Object listener, int bloomBits) {
      return new AddBloomNearCacheClientListenerOperation(remoteCache, listener, bloomBits);
   }

   public <T> QueryOperation newQueryOperation(RemoteQuery<T> ts, boolean withHitCount) {
      return new QueryOperation(remoteCache, ts, withHitCount);
   }

   public AddClientListenerOperation newAddClientListenerOperation(Object listener) {
      return newAddClientListenerOperation(listener, null, null);
   }

   public AddClientListenerOperation newAddClientListenerOperation(Object listener, Object[] filterFactoryParams,
                                                                   Object[] converterFactoryParams) {
      return new AddClientListenerOperation(remoteCache, listener, marshallParams(filterFactoryParams),
            marshallParams(converterFactoryParams));
   }

   public byte[][] marshallParams(Object[] params) {
      if (params == null)
         return org.infinispan.commons.util.Util.EMPTY_BYTE_ARRAY_ARRAY;

      byte[][] marshalledParams = new byte[params.length][];
      for (int i = 0; i < marshalledParams.length; i++) {
         byte[] bytes = remoteCache.getDataFormat().keyToBytes(params[i]);// should be small
         marshalledParams[i] = bytes;
      }

      return marshalledParams;
   }
}
