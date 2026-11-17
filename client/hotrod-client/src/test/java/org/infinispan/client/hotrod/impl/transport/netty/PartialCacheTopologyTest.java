package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.infinispan.client.hotrod.Internals;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.TopologyInfo;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Verifies how the client reacts when one cache's topology lists only a subset of the cluster.
 * <p>
 * A cache that is running on only some of the servers, for example while the other servers are still starting it,
 * has a consistent hash with only those servers. The servers send that subset as the cache's topology. Connections
 * are shared by every cache of a {@link RemoteCacheManager}, so that partial view must not affect the connections
 * other caches still use.
 * </p>
 */
@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.PartialCacheTopologyTest")
public class PartialCacheTopologyTest extends MultiHotRodServersTest {

   private static final int NUM_SERVERS = 2;
   private static final int NUM_SEGMENTS = 256;
   private static final short HASH_FUNCTION_VERSION = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServersWithoutClients(NUM_SERVERS, cacheConfiguration());
      clients.add(newClient());
   }

   public void testPartialCacheTopologyKeepsConnectionUsedByOtherCaches() {
      RemoteCacheManager client = client(0);
      OperationDispatcher dispatcher = Internals.dispatcher(client);
      InetSocketAddress secondServer = address(server(1));

      // The default cache runs on both servers, so its topology keeps the connection to the second server
      RemoteCache<String, String> defaultCache = client.getCache();
      for (int i = 0; i < 10; i++) {
         defaultCache.put("k" + i, "v" + i);
      }
      assertThat(dispatcher.getServers()).contains(secondServer);
      OperationChannel channelBefore = dispatcher.getHandlerForAddress(secondServer);
      assertThat(channelBefore).isNotNull();
      eventually(channelBefore::isAcceptingRequests);

      // A cache running only on the first server: the server sends a topology with only the first server
      String partialCacheName = cacheNameRoutedTo(dispatcher, server(0));
      defineCache(server(0), partialCacheName, cacheConfiguration());
      RemoteCache<String, String> partialCache = client.getCache(partialCacheName);
      partialCache.put("key", "value");
      assertThat(dispatcher.getCacheInfo(partialCacheName).getServers())
            .containsExactly(address(server(0)));

      // The partial view of one cache must not close the connection the default cache still uses
      assertThat(dispatcher.getHandlerForAddress(secondServer)).isSameAs(channelBefore);
      assertThat(channelBefore.getChannel().isActive()).isTrue();
   }

   public void testServerDroppedByEveryCacheTopologyClosesConnection() {
      // A client of its own: the topology injected below must not leak into the other tests
      RemoteCacheManager client = newClient();
      try {
         OperationDispatcher dispatcher = Internals.dispatcher(client);
         InetSocketAddress firstServer = address(server(0));
         InetSocketAddress secondServer = address(server(1));

         // The default cache is the only cache in use, and its topology lists both servers
         RemoteCache<String, String> defaultCache = client.getCache();
         for (int i = 0; i < 10; i++) {
            defaultCache.put("k" + i, "v" + i);
         }
         assertThat(dispatcher.getServers()).contains(secondServer);
         OperationChannel channelBefore = dispatcher.getHandlerForAddress(secondServer);
         assertThat(channelBefore).isNotNull();
         eventually(channelBefore::isAcceptingRequests);

         // The second server leaves the default cache's topology, as if it had left the cluster
         int topologyId = dispatcher.getCacheTopologyInfo(HotRodConstants.DEFAULT_CACHE_NAME).getTopologyId();
         SocketAddress[][] segmentOwners = new SocketAddress[NUM_SEGMENTS][];
         Arrays.fill(segmentOwners, new SocketAddress[]{firstServer});
         dispatcher.updateTopology(HotRodConstants.DEFAULT_CACHE_NAME, null, topologyId + 1,
               new InetSocketAddress[]{firstServer}, segmentOwners, HASH_FUNCTION_VERSION);

         // No cache uses the second server anymore, so its connection is closed
         assertThat(dispatcher.getHandlerForAddress(secondServer))
               .as("channel to the second server; cache topologies: %s", cacheTopologies(dispatcher))
               .isNull();
      } finally {
         HotRodClientTestingUtil.killRemoteCacheManager(client);
      }
   }

   /**
    * Creates a client that lists both servers as initial servers, so a new cache's topology starts with the whole
    * cluster.
    */
   private RemoteCacheManager newClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            createHotRodClientConfigurationBuilder(server(0));
      builder.addServer().host(server(1).getHost()).port(server(1).getPort());
      return new RemoteCacheManager(builder.build());
   }

   /**
    * Describes every cache the client tracks, with its topology id and servers, for assertion messages.
    */
   private static Map<String, String> cacheTopologies(OperationDispatcher dispatcher) {
      TopologyInfo topologyInfo = TestingUtil.extractField(dispatcher, "topologyInfo");
      Map<String, String> topologies = new TreeMap<>();
      topologyInfo.forEachCache((name, info) -> topologies.put(name, info.getTopologyId() + " " + info.getServers()));
      return topologies;
   }

   private static ConfigurationBuilder cacheConfiguration() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   private static InetSocketAddress address(HotRodServer server) {
      return InetSocketAddress.createUnresolved(server.getHost(), server.getPort());
   }

   /**
    * Returns a cache name whose first requests ({@code getCache} ping) the client sends to the given server.
    * The other server does not have the cache, and would reject the request.
    */
   private static String cacheNameRoutedTo(OperationDispatcher dispatcher, HotRodServer server) {
      for (int i = 0; ; i++) {
         String name = "partial-" + i;
         InetSocketAddress target = (InetSocketAddress) dispatcher.addressForCache(name);
         if (target.getPort() == server.getPort()) {
            return name;
         }
      }
   }
}
