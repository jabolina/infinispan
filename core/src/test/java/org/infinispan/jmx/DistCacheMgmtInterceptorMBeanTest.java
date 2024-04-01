package org.infinispan.jmx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.DistCacheMgmtInterceptorMBeanTest")
public class DistCacheMgmtInterceptorMBeanTest extends MultipleCacheManagersTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < 3; i++) {
         GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
         configureJmx(globalBuilder, jmxDomain(i), mBeanServerLookup);
         globalBuilder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
         builder.statistics().enable();

         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder);
         registerCacheManager(cm);
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      for (int i = 0; i < managers().length; i++) {
         TestingUtil.findInterceptor(cache(i), CacheMgmtInterceptor.class).resetStatistics();
      }
      super.clearContent();
   }

   private String jmxDomain(int i) {
      return DistCacheMgmtInterceptorMBeanTest.class.getSimpleName() + "-" + i;
   }

   private ObjectName cacheStats(int i, String component) {
      return getCacheObjectName(jmxDomain(i), getDefaultCacheName() + "(dist_sync)", component);
   }

   public void testSimpleAllOperations() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      Cache<String, String> c1 = cache(0);
      Cache<String, String> c2 = cache(1);
      ObjectName s1 = cacheStats(0, "Statistics");
      ObjectName s2 = cacheStats(1, "Statistics");

      assertThat(c1.put("key", "value")).isNull();

      assertThat(mBeanServer.getAttribute(s1, "Stores")).isEqualTo(1L);
      assertThat(mBeanServer.getAttribute(s2, "Stores")).isEqualTo(0L);

      assertThat(c2.get("key")).isEqualTo("value");

      assertThat(mBeanServer.getAttribute(s1, "Hits")).isEqualTo(0L);
      assertThat(mBeanServer.getAttribute(s2, "Hits")).isEqualTo(1L);

      Cache<String, String> c3 = cache(2);
      ObjectName s3 = cacheStats(2, "Statistics");

      assertThat(c3.remove("key")).isEqualTo("value");
      assertThat(mBeanServer.getAttribute(s1, "RemoveHits")).isEqualTo(0L);
      assertThat(mBeanServer.getAttribute(s2, "RemoveHits")).isEqualTo(0L);
      assertThat(mBeanServer.getAttribute(s3, "RemoveHits")).isEqualTo(1L);
   }

   public void testOwnerSimpleOperations() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();

      Cache<MagicKey, String> c1 = cache(0);
      Cache<MagicKey, String> c2 = cache(1);
      Cache<MagicKey, String> c3 = cache(2);

      ObjectName stats = cacheStats(2, "Statistics");

      // Write operation possibilities.
      assertStores(mBeanServer, stats, 0, 0, 0);
      // Non-owner store.
      MagicKey o1And2 = new MagicKey(c1, c2);
      c3.put(o1And2, "value");
      assertStores(mBeanServer, stats, 0, 0, 1);

      // Backup owner store.
      MagicKey o1And3 = new MagicKey(c1, c3);
      c3.put(o1And3, "value");
      assertStores(mBeanServer, stats, 0, 1, 1);

      // Primary owner store.
      MagicKey o3 = new MagicKey(c3);
      c3.put(o3, "value");
      assertStores(mBeanServer, stats, 1, 1, 1);

      // Now hits possibilities.
      assertHits(mBeanServer, stats, 0, 0, 0);

      // Non-owner hit.
      assertThat(c3.get(o1And2)).isEqualTo("value");
      assertHits(mBeanServer, stats, 0, 0, 1);

      // Backup owner hit.
      assertThat(c3.get(o1And3)).isEqualTo("value");
      assertHits(mBeanServer, stats, 0, 1, 1);

      // Primary owner hit.
      assertThat(c3.get(o3)).isEqualTo("value");
      assertHits(mBeanServer, stats, 1, 1, 1);

      // Now remove hits possibilities.
      assertRemovesHits(mBeanServer, stats, 0, 0, 0);

      // Non-owner hit.
      assertThat(c3.remove(o1And2)).isEqualTo("value");
      assertRemovesHits(mBeanServer, stats, 0, 0, 1);

      // Backup owner hit.
      assertThat(c3.remove(o1And3)).isEqualTo("value");
      assertRemovesHits(mBeanServer, stats, 0, 1, 1);

      // Primary owner hit.
      assertThat(c3.remove(o3)).isEqualTo("value");
      assertRemovesHits(mBeanServer, stats, 1, 1, 1);
   }

   public void testMissOperations() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();

      Cache<MagicKey, String> c1 = cache(0);
      Cache<MagicKey, String> c2 = cache(1);
      Cache<MagicKey, String> c3 = cache(2);

      // Write operation possibilities.
      // Non-owner store.
      MagicKey o1And2 = new MagicKey(c1, c2);
      c3.remove(o1And2);

      // Backup owner store.
      MagicKey o1And3 = new MagicKey(c1, c3);
      c3.remove(o1And3);

      // Primary owner store.
      MagicKey o3 = new MagicKey(c3);
      c3.remove(o3);
   }

   private void assertHits(MBeanServer server, ObjectName stats, long primary, long backup, long nonOwner) throws Exception {
      assertStats(server, stats, "Hits", primary, backup, nonOwner);
   }

   private void assertStores(MBeanServer server, ObjectName stats, long primary, long backup, long nonOwner) throws Exception {
      assertStats(server, stats, "Stores", primary, backup, nonOwner);
   }

   private void assertRemovesHits(MBeanServer server, ObjectName stats, long primary, long backup, long nonOwner) throws Exception {
      assertStats(server, stats, "RemoveHits", primary, backup, nonOwner);
   }

   private void assertStats(MBeanServer server, ObjectName stats, String name, long primary, long backup, long nonOwner) throws Exception {
      assertThat(server.getAttribute(stats, name)).isEqualTo(primary + backup + nonOwner);
      assertThat(server.getAttribute(stats, name + "Primary")).isEqualTo(primary);
      assertThat(server.getAttribute(stats, name + "Backup")).isEqualTo(backup);
      assertThat(server.getAttribute(stats, name + "NonOwner")).isEqualTo(nonOwner);
   }
}
