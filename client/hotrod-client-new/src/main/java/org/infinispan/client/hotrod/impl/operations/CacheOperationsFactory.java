package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.commons.util.IntSet;

public interface CacheOperationsFactory {
   InternalRemoteCache<?, ?> getRemoteCache();
   <V> HotRodOperation<V> newGetOperation(Object key);

   HotRodOperation<PingResponse> newPingOperation();

   <T> HotRodOperation<T> executeOperation(String taskName, Map<String, byte[]> marshalledParams, Object key);

   PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                              List<Modification> modifications,
                                                              boolean recoverable, long timeoutMs);

   HotRodOperation<Void> newRemoveClientListenerOperation(Object listener);

   HotRodOperation<IterationStartResponse> newIterationStartOperation(String filterConverterFactory, byte[][] filterParams,
                                                                      IntSet segments, int batchSize, boolean metadata);

   <K, E> HotRodOperation<IterationNextResponse<K, E>> newIterationNextOperation(byte[] iterationId, KeyTracker segmentKeyTracker);

   HotRodOperation<IterationEndResponse> newIterationEndOperation(byte[] iterationId);

   HotRodOperation<Void> newClearOperation();

   <V, K> HotRodOperation<V> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   <V> HotRodOperation<V> newRemoveOperation(Object key);

   <K> HotRodOperation<Boolean> newContainsKeyOperation(K key);

   <V, K> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit,
                                                 long maxIdleTime, TimeUnit maxIdleTimeUnit);

   <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                     long maxIdleTime, TimeUnit maxIdleTimeUnit);

   <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                     long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   HotRodOperation<ServerStatistics> newStatsOperation();

   HotRodOperation<Integer> newSizeOperation();

   PutAllOperation newPutAllOperation(Map<byte[], byte[]> byteMap, long lifespan, TimeUnit lifespanUnit,
                                      long maxIdleTime, TimeUnit maxIdleTimeUnit);

   <V, K> HotRodOperation<MetadataValue<V>> newGetWithMetadataOperation(K key);

   <V, K> GetWithMetadataOperation<V> newGetWithMetadataOperation(K key, SocketAddress preferredAddres);

   <V, K> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan,
                                                                                         TimeUnit lifespanTimeUnit, long maxIdle,
                                                                                         TimeUnit maxIdleTimeUnit, long version);

   <V, K> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version);

   <K, V> GetAllOperation<K, V> newGetAllOperation(Set<byte[]> byteKeys);

   HotRodOperation<Void> newUpdateBloomFilterOperation(byte[] bloomFilterBits);

   ClientListenerOperation newAddNearCacheListenerOperation(Object listener, int bloomBits);

   <T> QueryOperation newQueryOperation(RemoteQuery<T> ts, boolean withHitCount);

   AddClientListenerOperation newAddClientListenerOperation(Object listener);

   AddClientListenerOperation newAddClientListenerOperation(Object listener, Object[] filterFactoryParams,
                                                            Object[] converterFactoryParams);

   byte[][] marshallParams(Object[] params);

   CacheOperationsFactory newFactoryFor(InternalRemoteCache<?, ?> internalRemoteCache);
}
