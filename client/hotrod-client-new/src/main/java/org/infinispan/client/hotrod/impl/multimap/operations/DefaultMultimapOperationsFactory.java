package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.multimap.MetadataCollection;

import net.jcip.annotations.Immutable;

/**
 * Factory for {@link HotRodOperation} objects on Multimap.
 *
 * @author karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class DefaultMultimapOperationsFactory implements MultimapOperationsFactory {

   private final InternalRemoteCache<?, ?> remoteCache;

   public DefaultMultimapOperationsFactory(InternalRemoteCache<?, ?> remoteCache) {
      this.remoteCache = remoteCache;
   }

   @Override
   public <K, V> HotRodOperation<Collection<V>> newGetKeyMultimapOperation(K key, boolean supportsDuplicates) {
      return new GetKeyMultimapOperation<>(remoteCache, key, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<MetadataCollection<V>> newGetKeyWithMetadataMultimapOperation(K key, boolean supportsDuplicates) {
      return new GetKeyWithMetadataMultimapOperation<>(remoteCache, key, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Void> newPutKeyValueOperation(K key, V value, long lifespan,
                                                               TimeUnit lifespanTimeUnit, long maxIdle,
                                                               TimeUnit maxIdleTimeUnit, boolean supportsDuplicates) {
      return new PutKeyValueMultimapOperation<>(remoteCache, key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, supportsDuplicates);
   }

   @Override
   public <K> HotRodOperation<Boolean> newRemoveKeyOperation(K key, boolean supportsDuplicates) {
      return new RemoveKeyMultimapOperation<>(remoteCache, key, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newRemoveEntryOperation(K key, V value, boolean supportsDuplicates) {
      return new RemoveEntryMultimapOperation<>(remoteCache, key, value, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newContainsEntryOperation(K key, V value, boolean supportsDuplicates) {
      return new ContainsEntryMultimapOperation<>(remoteCache, key, value, supportsDuplicates);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key, boolean supportsDuplicates) {
      return new ContainsKeyMultimapOperation<>(remoteCache, key, supportsDuplicates);
   }

   @Override
   public <V> HotRodOperation<Boolean> newContainsValueOperation(V value, boolean supportsDuplicates) {
      return new ContainsValueMultimapOperation<>(remoteCache, value, -1, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, supportsDuplicates);
   }

   @Override
   public HotRodOperation<Long> newSizeOperation(boolean supportsDuplicates) {
      return new SizeMultimapOperation(remoteCache, supportsDuplicates);
   }
}
