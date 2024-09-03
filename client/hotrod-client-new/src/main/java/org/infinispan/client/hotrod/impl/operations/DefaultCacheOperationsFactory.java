package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.commons.util.IntSet;

public class DefaultCacheOperationsFactory implements CacheOperationsFactory {
   private final InternalRemoteCache<?, ?> remoteCache;

   public DefaultCacheOperationsFactory(InternalRemoteCache<?, ?> remoteCache) {
      this.remoteCache = Objects.requireNonNull(remoteCache);
   }

   public InternalRemoteCache<?, ?> getRemoteCache() {
      return remoteCache;
   }

   @Override
   public <V> HotRodOperation<V> newGetOperation(Object key) {
      // TODO: need to support when storage is object based
      return new GetOperation<>(remoteCache, key);
   }

   @Override
   public HotRodOperation<PingResponse> newPingOperation() {
      return new CachePingOperation(remoteCache.getName());
   }

   @Override
   public <T> HotRodOperation<T> executeOperation(String taskName, Map<String, byte[]> marshalledParams, Object key) {
      return new CacheExecuteOperation<>(remoteCache, taskName, marshalledParams, remoteCache.getDataFormat().keyToBytes(key));
   }

   @Override
   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                                     List<Modification> modifications,
                                                                     boolean recoverable, long timeoutMs) {
      return new PrepareTransactionOperation(remoteCache, xid, onePhaseCommit, modifications, recoverable, timeoutMs);
   }

   @Override
   public HotRodOperation<Void> newRemoveClientListenerOperation(Object listener) {
      ClientListenerNotifier cln = remoteCache.getDispatcher().getClientListenerNotifier();
      byte[] listenerId = cln.findListenerId(listener);
      if (listenerId == null) {
         return NoHotRodOperation.instance();
      }
      return new RemoveClientListenerOperation(remoteCache, cln, listenerId);
   }

   @Override
   public HotRodOperation<IterationStartResponse> newIterationStartOperation(String filterConverterFactory, byte[][] filterParams,
                                                                             IntSet segments, int batchSize, boolean metadata) {
      return new IterationStartOperation(remoteCache, filterConverterFactory, filterParams, segments, batchSize,
            metadata);
   }

   @Override
   public <K, E> HotRodOperation<IterationNextResponse<K, E>> newIterationNextOperation(byte[] iterationId, KeyTracker segmentKeyTracker) {
      return new IterationNextOperation<>(remoteCache, iterationId, segmentKeyTracker);
   }

   @Override
   public HotRodOperation<IterationEndResponse> newIterationEndOperation(byte[] iterationId) {
      return new IterationEndOperation(remoteCache, iterationId);
   }

   @Override
   public ClearOperation newClearOperation() {
      return new ClearOperation(remoteCache);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      // TODO: need to support when storage is object based
      return new PutOperation<>(remoteCache, key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V> HotRodOperation<V> newRemoveOperation(Object key) {
      return new RemoveOperation<>(remoteCache, key);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key) {
      return new ContainsKeyOperation<>(remoteCache, key);
   }

   @Override
   public <V, K> HotRodOperation<V> newReplaceOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                        long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new ReplaceOperation<>(remoteCache, key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                            long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new PutIfAbsentOperation<>(remoteCache, key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                            long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return new PutIfAbsentOperation<>(remoteCache.withFlags(flags), key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public HotRodOperation<ServerStatistics> newStatsOperation() {
      return new StatsOperation(remoteCache);
   }

   @Override
   public HotRodOperation<Integer> newSizeOperation() {
      return new SizeOperation(remoteCache);
   }

   @Override
   public <K, V> PutAllOperation<K, V> newPutAllOperation(Map<? extends K, ? extends V> data, long lifespan,
                                                          TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new PutAllOperation<>(remoteCache, data, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public <V, K> HotRodOperation<MetadataValue<V>> newGetWithMetadataOperation(K key) {
      return new GetWithMetadataOperation<>(remoteCache, key, null);
   }

   @Override
   public <V, K> GetWithMetadataOperation<K, V> newGetWithMetadataOperation(K key, SocketAddress preferredAddres) {
      return new GetWithMetadataOperation<>(remoteCache, key, preferredAddres);
   }

   @Override
   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan,
                                                                                                TimeUnit lifespanTimeUnit, long maxIdle,
                                                                                                TimeUnit maxIdleTimeUnit, long version) {
      return new ReplaceIfUnmodifiedOperation<>(remoteCache, key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
   }

   @Override
   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return new RemoveIfUnmodifiedOperation<>(remoteCache, key, version);
   }

   @Override
   public <K, V> GetAllOperation<K, V> newGetAllOperation(Set<K> byteKeys) {
      return new GetAllOperation<>(remoteCache, byteKeys);
   }

   @Override
   public HotRodOperation<Void> newUpdateBloomFilterOperation(byte[] bloomFilterBits) {
      return new UpdateBloomFilterOperation(remoteCache, bloomFilterBits);
   }

   @Override
   public ClientListenerOperation newAddNearCacheListenerOperation(Object listener, int bloomBits) {
      return new AddBloomNearCacheClientListenerOperation(remoteCache, listener, bloomBits);
   }

   @Override
   public <T> QueryOperation newQueryOperation(RemoteQuery<T> ts, boolean withHitCount) {
      return new QueryOperation(remoteCache, ts, withHitCount);
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener) {
      return newAddClientListenerOperation(listener, null, null);
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener, Object[] filterFactoryParams,
                                                                   Object[] converterFactoryParams) {
      return new AddClientListenerOperation(remoteCache, listener, marshallParams(filterFactoryParams),
            marshallParams(converterFactoryParams));
   }

   @Override
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

   @Override
   public CacheOperationsFactory newFactoryFor(InternalRemoteCache<?, ?> internalRemoteCache) {
      return new DefaultCacheOperationsFactory(internalRemoteCache);
   }
}
