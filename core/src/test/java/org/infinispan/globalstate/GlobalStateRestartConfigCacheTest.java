package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(testName = "globalstate.GlobalStateRestartConfigCacheTest", groups = "functional")
public class GlobalStateRestartConfigCacheTest extends MultipleCacheManagersTest {

   private static final int CLUSTER_SIZE = 2;
   private static final int NUM_CACHES = 3;
   private static final int DATA_SIZE = 10;

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManagers();
   }

   private void createStatefulCacheManagers() {
      IntStream.range(0, CLUSTER_SIZE).forEach(this::createStatefulCacheManager);
   }

   private void createStatefulCacheManager(int index) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), Integer.toString(index));
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory)
            .configurationStorage(ConfigurationStorage.OVERLAY);
      addClusterEnabledCacheManager(global, null);
   }

   public void testRestartedNodeSkipsConfigCacheWrites() {
      Configuration cacheConfig = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2)
            .build();

      // Create caches on both nodes
      for (int c = 0; c < NUM_CACHES; c++) {
         String cacheName = "cache-" + c;
         manager(0).administration().createCache(cacheName, cacheConfig);
         waitForClusterToForm(cacheName);
         Cache<String, String> cache = manager(0).getCache(cacheName);
         for (int i = 0; i < DATA_SIZE; i++) {
            cache.put("key-" + i, "value-" + i);
         }
      }

      // Stop node-1, keep node-0 running
      EmbeddedCacheManager stoppedManager = manager(1);
      stoppedManager.stop();
      cacheManagers.remove(1);

      // Install a PutKeyValueCommand counter on node-0's CONFIG cache.
      // When node-1 restarts and its postStart() runs, any putIfAbsentAsync()
      // to CONFIG would replicate to node-0 and be caught by this interceptor.
      AtomicInteger configPuts = new AtomicInteger(0);
      Cache<?, ?> configCache = manager(0).getCache(InternalCacheNames.CONFIG_STATE_CACHE_NAME);
      PutCommandCounter counter = new PutCommandCounter(configPuts);
      extractInterceptorChain(configCache).addInterceptor(counter, 0);

      // Restart node-1
      createStatefulCacheManager(1);

      // Wait for all caches to form on both nodes
      for (int c = 0; c < NUM_CACHES; c++) {
         waitForClusterToForm("cache-" + c);
      }

      // The restarting node should NOT have sent any putIfAbsentAsync to CONFIG
      // because all entries already existed from state transfer
      assertThat(configPuts.get())
            .as("Restarting node should not write to CONFIG cache when entries already exist")
            .isZero();

      // Verify data is accessible on the restarted node
      for (int c = 0; c < NUM_CACHES; c++) {
         Cache<String, String> cache = manager(1).getCache("cache-" + c);
         assertThat(cache).hasSize(DATA_SIZE);
         for (int i = 0; i < DATA_SIZE; i++) {
            assertThat(cache.get("key-" + i)).isEqualTo("value-" + i);
         }
      }

      extractInterceptorChain(configCache).removeInterceptor(PutCommandCounter.class);
   }

   public void testRestartedNodeSkipsConfigCacheWritesForTemplates() {
      Configuration templateConfig = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2)
            .build();

      // Create templates on both nodes
      for (int t = 0; t < NUM_CACHES; t++) {
         manager(0).administration().createTemplate("template-" + t, templateConfig);
      }

      // Stop node-1, keep node-0 running
      EmbeddedCacheManager stoppedManager = manager(1);
      stoppedManager.stop();
      cacheManagers.remove(1);

      // Install a PutKeyValueCommand counter on node-0's CONFIG cache
      AtomicInteger configPuts = new AtomicInteger(0);
      Cache<?, ?> configCache = manager(0).getCache(InternalCacheNames.CONFIG_STATE_CACHE_NAME);
      PutCommandCounter counter = new PutCommandCounter(configPuts);
      extractInterceptorChain(configCache).addInterceptor(counter, 0);

      // Restart node-1 and wait for it to join
      createStatefulCacheManager(1);
      waitForClusterToForm(InternalCacheNames.CONFIG_STATE_CACHE_NAME);

      // The restarting node should NOT have sent any putIfAbsentAsync to CONFIG
      assertThat(configPuts.get())
            .as("Restarting node should not write to CONFIG cache for existing templates")
            .isZero();

      // Verify templates are available on the restarted node
      for (int t = 0; t < NUM_CACHES; t++) {
         assertThat(manager(1).getCacheConfiguration("template-" + t)).isNotNull();
      }

      extractInterceptorChain(configCache).removeInterceptor(PutCommandCounter.class);
   }

   static class PutCommandCounter extends DDAsyncInterceptor {
      private final AtomicInteger counter;

      PutCommandCounter(AtomicInteger counter) {
         this.counter = counter;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         counter.incrementAndGet();
         return invokeNext(ctx, command);
      }
   }
}
