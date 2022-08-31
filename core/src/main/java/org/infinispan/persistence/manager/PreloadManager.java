package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.transaction.impl.FakeJTATransaction;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Separate the preload into its own component
 */
@Scope(Scopes.NAMED_CACHE)
public class PreloadManager {
   public static final long PRELOAD_FLAGS = FlagBitSets.CACHE_MODE_LOCAL |
                                            FlagBitSets.SKIP_OWNERSHIP_CHECK |
                                            FlagBitSets.IGNORE_RETURN_VALUES |
                                            FlagBitSets.SKIP_CACHE_STORE |
                                            FlagBitSets.SKIP_LOCKING |
                                            FlagBitSets.SKIP_XSITE_BACKUP |
                                            FlagBitSets.IRAC_STATE;
   public static final long PRELOAD_WITHOUT_INDEXING_FLAGS =
         EnumUtil.mergeBitSets(PRELOAD_FLAGS, FlagBitSets.SKIP_INDEXING);

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject Configuration configuration;
   @Inject protected PersistenceManager persistenceManager;
   @Inject TimeService timeService;
   @Inject protected ComponentRef<AdvancedCache<?, ?>> cache;
   @Inject CommandsFactory commandsFactory;
   @Inject KeyPartitioner keyPartitioner;
   @Inject InvocationContextFactory invocationContextFactory;
   @Inject InvocationHelper invocationHelper;
   @Inject TransactionCoordinator transactionCoordinator;
   @Inject TransactionManager transactionManager;
   @Inject TransactionTable transactionTable;
   @Inject MarshallableEntryFactory entryFactory;
   @Inject @ComponentName(NON_BLOCKING_EXECUTOR) ExecutorService nonBlockingExecutor;
   private LimitedExecutor executor;

   private volatile PreloadStatus status = PreloadStatus.NOT_RUNNING;
   private long version = 0;

   @Start
   public void blockingPreload() {
      executor = new LimitedExecutor("preloader-" + cache.wired().getName(), nonBlockingExecutor, 1);
      CompletionStages.join(preload());
   }

   public CompletionStage<Void> preload() {
      return CompletableFuture.supplyAsync(this::doPreload, executor)
            .thenCompose(Function.identity())
            .thenCompose(loaded -> {
               if (loaded == PreloadStatus.FAILED_LOAD) {
                  status = loaded;
               }
               return removeOldEntries().thenApply(ignore -> loaded);
            })
            .whenComplete((loadStatus, t) -> {
               version += 1;
               status = loadStatus;
            })
            .thenApply(ignore -> null);
   }

   private CompletionStage<PreloadStatus> doPreload() {
      if (status == PreloadStatus.RUNNING) {
         return CompletableFutures.completedExceptionFuture(new IllegalStateException("Preloader already running"));
      }

      status = PreloadStatus.RUNNING;
      Publisher<MarshallableEntry<Object, Object>> publisher = persistenceManager.preloadPublisher();

      long start = timeService.time();

      final long maxEntries = getMaxEntries();
      final long flags = getFlagsForStateInsertion();
      AdvancedCache<?,?> tmpCache = this.cache.wired().withStorageMediaType();
      DataConversion keyDataConversion = tmpCache.getKeyDataConversion();
      DataConversion valueDataConversion = tmpCache.getValueDataConversion();

      Transaction outerTransaction = suspendIfNeeded();
      try {
         return Flowable.fromPublisher(publisher)
                        .take(maxEntries)
                        .map(this::handleEntryPrivateMetadata)
                        .concatMapSingle(me -> preloadEntry(flags, me, keyDataConversion, valueDataConversion))
                        .count()
                        .toCompletionStage()
                        .whenComplete((insertAmount, t) -> {
                           log.debugf("Preloaded %d keys in %s", insertAmount,
                                      Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
                        })
                        .thenApply(insertAmount -> insertAmount < maxEntries
                              ? PreloadStatus.COMPLETE_LOAD
                              : PreloadStatus.PARTIAL_LOAD)
                        .exceptionally(t -> PreloadStatus.FAILED_LOAD);
      } finally {
         resumeIfNeeded(outerTransaction);
      }
   }

   private Single<?> preloadEntry(long flags, MarshallableEntry<Object, Object> me, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      // CallInterceptor will preserve the timestamps if the metadata is an InternalMetadataImpl instance
      InternalMetadataImpl metadata = new InternalMetadataImpl(me.getMetadata(), me.created(), me.lastUsed());
      // TODO If the storage media type is application/x-protostream, this will convert to POJOs and back
      Object key = keyDataConversion.toStorage(me.getKey());
      Object value = valueDataConversion.toStorage(me.getValue());
      PutKeyValueCommand cmd = commandsFactory.buildPutKeyValueCommand(key, value, keyPartitioner.getSegment(key),
                                                                       metadata, flags);
      cmd.setInternalMetadata(me.getInternalMetadata());

      CompletionStage<?> stage;
      if (configuration.transaction().transactionMode().isTransactional()) {
         try {
            Transaction transaction = new FakeJTATransaction();
            InvocationContext ctx = invocationContextFactory.createInvocationContext(transaction, false);
            LocalTransaction localTransaction = ((LocalTxInvocationContext) ctx).getCacheTransaction();
            stage = CompletionStages.handleAndCompose(invocationHelper.invokeAsync(ctx, cmd),
                                                      (__, t) -> completeTransaction(key, localTransaction, t))
                  .whenComplete((__, t) -> transactionTable.removeLocalTransaction(localTransaction));
         } catch (Exception e) {
            throw log.problemPreloadingKey(key, e);
         }
      } else {
         stage = invocationHelper.invokeAsync(cmd, 1);
      }
      // The return value doesn't matter, but it cannot be null
      return Completable.fromCompletionStage(stage).toSingleDefault(me);
   }

   private CompletionStage<Void> removeOldEntries() {
      if (version == 0 || status == PreloadStatus.FAILED_LOAD) return CompletableFutures.completedNull();

      AdvancedCache<?, ?> tmpCache = cache.wired().withFlags(EnumUtil.enumSetOf(getFlagsForStateInsertion(), Flag.class));
      CloseableIterator<? extends CacheEntry<?, ?>> it = tmpCache.cacheEntrySet().iterator();
      while (it.hasNext()) {
         CacheEntry<?, ?> entry = it.next();
         PrivateMetadata metadata = entry.getInternalMetadata();
         if (metadata != null) {
            EntryVersion entryVersion = metadata.entryVersion();
            if (entryVersion instanceof NumericVersion && ((NumericVersion) entryVersion).getVersion() < version) {
               it.remove();
            }
         }
      }

      return CompletableFutures.completedNull();
   }

   private MarshallableEntry<Object, Object> handleEntryPrivateMetadata(MarshallableEntry<Object, Object> entry) {
      PrivateMetadata privateMetadata = entry.getInternalMetadata();
      if (privateMetadata == null) {
         privateMetadata = PrivateMetadata.getBuilder(null)
               .build();
      }

      privateMetadata = privateMetadata.builder()
            .entryVersion(new NumericVersion(version))
            .preloadedEntry()
            .build();

      return entryFactory.create(entry.getKey(), entry.getValue(), entry.getMetadata(), privateMetadata,
            entry.created(), entry.lastUsed());
   }

   private CompletionStage<?> completeTransaction(Object key, LocalTransaction localTransaction, Throwable t) {
      if (t != null) {
         return transactionCoordinator.rollback(localTransaction)
                                      .whenComplete((__1, t1) -> {
                                         throw log.problemPreloadingKey(key, t);
                                      });
      }

      return transactionCoordinator.commit(localTransaction, true);
   }

   private void resumeIfNeeded(Transaction transaction) {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null &&
          transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   private Transaction suspendIfNeeded() {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            return transactionManager.suspend();
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
      return null;
   }

   private long getMaxEntries() {
      long maxCount;
      if (configuration.memory().isEvictionEnabled() && (maxCount = configuration.memory().maxCount()) > 0) {
         return maxCount;
      }
      return Long.MAX_VALUE;
   }

   private long getFlagsForStateInsertion() {
      boolean hasSharedStore = persistenceManager.hasStore(StoreConfiguration::shared);
      if (!hasSharedStore  || !configuration.indexing().isVolatile()) {
         return PRELOAD_WITHOUT_INDEXING_FLAGS;
      } else {
         return PRELOAD_FLAGS;
      }
   }

   /**
    * @return true if all entries from the store have been inserted to the cache. If the persistence/preload
    * is disabled or eviction limit was reached when preloading, returns false.
    */
   public boolean isFullyPreloaded() {
      return status.fullyPreloaded();
   }
}
