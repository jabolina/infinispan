package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;

public class RoutingCacheOperationsFactory extends DelegatingCacheOperationsFactory {
   public RoutingCacheOperationsFactory(CacheOperationsFactory delegate) {
      super(delegate);
   }

   @Override
   public <V> HotRodOperation<V> newGetOperation(Object key) {
      return new RoutingObjectOperation<>(super.newGetOperation(key), key);
   }

   @Override
   public <V> HotRodOperation<V> newRemoveOperation(Object key) {
      return new RoutingObjectOperation<>(super.newRemoveOperation(key), key);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new RoutingObjectOperation<>(super.newPutKeyValueOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), key);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new RoutingObjectOperation<>(super.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), key);
   }

   @Override
   public <V, K> HotRodOperation<V> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return new RoutingObjectOperation<>(super.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags), key);
   }

   @Override
   public <V, K> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new RoutingObjectOperation<>(super.newReplaceOperation(key, valueBytes, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), key);
   }

   @Override
   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version) {
      return new RoutingObjectOperation<>(super.newReplaceIfUnmodifiedOperation(key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version), key);
   }

   @Override
   public <V, K> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return new RoutingObjectOperation<>(super.newRemoveIfUnmodifiedOperation(key, version), key);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key) {
      return new RoutingObjectOperation<>(super.newContainsKeyOperation(key), key);
   }

   @Override
   protected DelegatingCacheOperationsFactory newFactoryFor(CacheOperationsFactory factory) {
      return new RoutingCacheOperationsFactory(factory);
   }
}
