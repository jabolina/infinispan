package org.infinispan.client.hotrod.counter.impl;

import org.infinispan.client.hotrod.counter.operation.AddListenerOperation;
import org.infinispan.client.hotrod.counter.operation.AddOperation;
import org.infinispan.client.hotrod.counter.operation.CompareAndSwapOperation;
import org.infinispan.client.hotrod.counter.operation.DefineCounterOperation;
import org.infinispan.client.hotrod.counter.operation.GetConfigurationOperation;
import org.infinispan.client.hotrod.counter.operation.GetCounterNamesOperation;
import org.infinispan.client.hotrod.counter.operation.GetValueOperation;
import org.infinispan.client.hotrod.counter.operation.IsDefinedOperation;
import org.infinispan.client.hotrod.counter.operation.RemoveListenerOperation;
import org.infinispan.client.hotrod.counter.operation.RemoveOperation;
import org.infinispan.client.hotrod.counter.operation.ResetOperation;
import org.infinispan.client.hotrod.counter.operation.SetOperation;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.counter.api.CounterConfiguration;

/**
 * A operation factory that builds counter operations.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterOperationFactory {

   public static final String COUNTER_CACHE_NAME = "org.infinispan.COUNTER";

   private final InternalRemoteCache<?, ?> cache;

   CounterOperationFactory(InternalRemoteCache<?, ?> cache) {
      this.cache = cache;
   }

   IsDefinedOperation newIsDefinedOperation(String counterName) {
      return new IsDefinedOperation(cache, counterName);
   }

   GetConfigurationOperation newGetConfigurationOperation(String counterName) {
      return new GetConfigurationOperation(cache, counterName);
   }

   DefineCounterOperation newDefineCounterOperation(String counterName, CounterConfiguration cfg) {
      return new DefineCounterOperation(cache, counterName, cfg);
   }

   RemoveOperation newRemoveOperation(String counterName, boolean useConsistentHash) {
      return new RemoveOperation(cache, counterName, useConsistentHash);
   }

   AddOperation newAddOperation(String counterName, long delta, boolean useConsistentHash) {
      return new AddOperation(cache, counterName, delta, useConsistentHash);
   }

   GetValueOperation newGetValueOperation(String counterName, boolean useConsistentHash) {
      return new GetValueOperation(cache, counterName, useConsistentHash);
   }

   ResetOperation newResetOperation(String counterName, boolean useConsistentHash) {
      return new ResetOperation(cache, counterName, useConsistentHash);
   }

   CompareAndSwapOperation newCompareAndSwapOperation(String counterName, long expect, long update,
                                                      CounterConfiguration counterConfiguration) {
      return new CompareAndSwapOperation(cache, counterName, expect, update, counterConfiguration);
   }

   SetOperation newSetOperation(String counterName, long value, boolean useConsistentHash) {
      return new SetOperation(cache, counterName, value, useConsistentHash);
   }

   GetCounterNamesOperation newGetCounterNamesOperation() {
      return new GetCounterNamesOperation(cache);
   }

   AddListenerOperation newAddListenerOperation(String counterName, byte[] listenerId) {
      return new AddListenerOperation(cache, counterName, listenerId);
   }

   RemoveListenerOperation newRemoveListenerOperation(String counterName, byte[] listenerId) {
      return new RemoveListenerOperation(cache, counterName, listenerId);
   }
}
