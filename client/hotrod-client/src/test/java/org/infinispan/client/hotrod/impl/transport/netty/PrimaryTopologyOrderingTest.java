package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.AddPrimaryNearCacheClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.NoHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.RemoveClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.UpdateBloomFilterOperation;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.reactivex.rxjava3.core.Flowable;

@Test(groups = "unit", testName = "client.hotrod.impl.transport.netty.PrimaryTopologyOrderingTest")
public class PrimaryTopologyOrderingTest extends AbstractInfinispanTest {
   private static final InetSocketAddress A = InetSocketAddress.createUnresolved("localhost", 11222);
   private static final InetSocketAddress B = InetSocketAddress.createUnresolved("localhost", 11223);
   private static final InetSocketAddress[] SERVERS = {A, B};
   private static final String OTHER_CACHE = "other";

   private OperationDispatcher dispatcher() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host(A.getHostString()).port(A.getPort());
      builder.addServer().host(B.getHostString()).port(B.getPort());
      return new OperationDispatcher(builder.build(), mock(ExecutorService.class), new DefaultTimeService(),
            mock(ClientListenerNotifier.class), null);
   }

   private Channel connect(OperationDispatcher dispatcher, SocketAddress address) {
      Channel channel = new EmbeddedChannel();
      ChannelRecord.set(channel, address);
      OperationChannel connection = mock(OperationChannel.class);
      when(connection.getChannel()).thenReturn(channel);
      when(connection.isAcceptingRequests()).thenReturn(true);
      when(connection.pendingOperationFlowable()).thenReturn(Flowable.empty());
      when(connection.close()).thenAnswer(invocation -> {
         channel.close();
         return List.of();
      });
      ConcurrentMap<SocketAddress, OperationChannel> connections = TestingUtil.extractField(dispatcher.getChannelHandler(), "channels");
      connections.put(address, connection);
      return channel;
   }

   private void push(OperationDispatcher dispatcher, Channel channel, int sequence, int hash, SocketAddress primary) {
      push(dispatcher, channel, sequence, sequence - 1, new BitSet(), hash, primary);
   }

   private void push(OperationDispatcher dispatcher, Channel channel, int sequence, int baseSequence, BitSet changed,
                     int hash, SocketAddress primary) {
      dispatcher.updatePrimaryTopology("", channel, sequence, baseSequence, changed, hash, SERVERS,
            new SocketAddress[][]{{primary}}, (short) 3);
   }

   private static BitSet segments(int... segments) {
      BitSet set = new BitSet();
      for (int segment : segments) {
         set.set(segment);
      }
      return set;
   }

   private SocketAddress primary(OperationDispatcher dispatcher) {
      return ((SegmentConsistentHash) dispatcher.getConsistentHash("")).getSegmentOwners()[0][0];
   }

   public void testLatestTopologyWaitsForListenerNotification() throws Exception {
      topologyActionWaitsForListenerNotification((dispatcher, channel) -> dispatcher.withLatestTopology("", info -> {
         assertThat(info.getTopologyId()).isEqualTo(123);
         return null;
      }));
   }

   public void testResponseTopologyWaitsForListenerNotification() throws Exception {
      topologyActionWaitsForListenerNotification((dispatcher, channel) -> dispatcher.updateTopology("", null, 456,
            SERVERS, new SocketAddress[][]{{B}}, (short) 3, channel));
   }

   public void testTopologyWithoutSourceWaitsForListenerNotification() throws Exception {
      topologyActionWaitsForListenerNotification((dispatcher, channel) -> dispatcher.updateTopology("", null, 456,
            SERVERS, new SocketAddress[][]{{B}}, (short) 3));
   }

   private void topologyActionWaitsForListenerNotification(BiConsumer<OperationDispatcher, Channel> action) throws Exception {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      CountDownLatch notifying = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      doAnswer(invocation -> {
         notifying.countDown();
         assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
         return null;
      }).when(listener).afterTopologyUpdate(any(), any());
      dispatcher.addTopologyListener("", listener);
      Future<?> update = fork(() -> push(dispatcher, a, 100, 123, A));
      AtomicReference<Thread> waiting = new AtomicReference<>();
      Future<?> next = null;
      try {
         assertThat(notifying.await(10, TimeUnit.SECONDS)).isTrue();
         next = fork(() -> {
            waiting.set(Thread.currentThread());
            action.accept(dispatcher, a);
         });
         Future<?> pending = next;
         eventually(() -> pending.isDone() || waiting.get() != null && waiting.get().getState() == Thread.State.BLOCKED);
         assertThat(next.isDone()).as("The action must wait until listener notification finishes").isFalse();
      } finally {
         release.countDown();
         update.get(10, TimeUnit.SECONDS);
         if (next != null) {
            next.get(10, TimeUnit.SECONDS);
         }
      }
   }

   public void testDelayedPushCannotUndoNewOwnerOrNotifyListeners() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      Channel b = connect(dispatcher, B);
      List<Integer> applied = new ArrayList<>();
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      doAnswer(invocation -> {
         applied.add(invocation.<CacheInfo>getArgument(1).getTopologyId());
         return null;
      }).when(listener).afterTopologyUpdate(any(), any());
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, a, 100, 123, A);
      push(dispatcher, b, 101, -456, B);
      push(dispatcher, a, 100, 123, A);
      push(dispatcher, b, 101, 789, A);
      assertThat(primary(dispatcher)).isEqualTo(B);
      assertThat(applied).containsExactly(123, -456);
   }

   public void testSequenceAdvancesEvenWhenHashDoesNotChange() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      push(dispatcher, a, 100, 123, A);
      push(dispatcher, a, 102, 123, A);
      push(dispatcher, a, 101, 456, B);
      assertThat(primary(dispatcher)).isEqualTo(A);
   }

   public void testReplacementSnapshotWaitsForOldSocketsToRetire() {
      OperationDispatcher dispatcher = dispatcher();
      Channel oldA = connect(dispatcher, A);
      Channel oldB = connect(dispatcher, B);
      push(dispatcher, oldA, 100, 123, A);
      Channel newA = connect(dispatcher, A);
      dispatcher.retirePrimaryChannel(oldA);
      push(dispatcher, newA, 0, 456, B);
      push(dispatcher, newA, 2, 456, B);
      push(dispatcher, newA, 1, 789, A);
      assertThat(dispatcher.primaryTopologyReady("", newA).toCompletableFuture()).isNotDone();
      assertThat(primary(dispatcher)).isEqualTo(A);
      connect(dispatcher, B);
      dispatcher.retirePrimaryChannel(oldB);
      assertThat(primary(dispatcher)).isEqualTo(B);
      assertThat(dispatcher.primaryTopologyReady("", newA).toCompletableFuture()).isCompleted();
      // Old channels are still locally active, but no longer the current sockets at their addresses.
      assertThat(oldA.isActive()).isTrue();
      push(dispatcher, oldA, 101, 789, A);
      push(dispatcher, oldB, 102, 789, A);
      assertThat(primary(dispatcher)).isEqualTo(B);
   }

   public void testAllServersRestartBeforeFirstReconnect() {
      OperationDispatcher dispatcher = dispatcher();
      Channel oldA = connect(dispatcher, A);
      Channel oldB = connect(dispatcher, B);
      push(dispatcher, oldA, 100, 123, A);
      oldA.close();
      oldB.close();
      dispatcher.retirePrimaryChannel(oldA);
      dispatcher.retirePrimaryChannel(oldB);
      Channel newA = connect(dispatcher, A);
      connect(dispatcher, B);
      push(dispatcher, newA, 0, 456, B);
      assertThat(primary(dispatcher)).isEqualTo(B);
   }

   public void testResponseFromReplacementRetiresExcludedSockets() {
      OperationDispatcher dispatcher = dispatcher();
      Channel oldA = connect(dispatcher, A);
      Channel oldB = connect(dispatcher, B);
      push(dispatcher, oldA, 100, 123, A);
      Channel newA = connect(dispatcher, A);
      dispatcher.updateTopology("", null, 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3, newA);
      assertThat(dispatcher.isCurrentChannel(oldB)).isFalse();
      dispatcher.updatePrimaryTopology("", newA, 0, -1, new BitSet(), 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);
      push(dispatcher, oldB, 101, 789, B);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);
   }

   /**
    * A server excluded by a replacement's response can still be in another cache's topology. Its connection is
    * shared by every cache, so it stays open, while the cache with the primary listener moves to the replacement's
    * topology as if the server's socket had retired.
    */
   public void testResponseFromReplacementKeepsSocketUsedByAnotherCache() {
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      ReplacedWhileShared state = replaceFirstServerWhileOtherCacheUsesSecond(listener);
      OperationDispatcher dispatcher = state.dispatcher();

      assertThat(dispatcher.isCurrentChannel(state.oldB())).isTrue();
      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);
      assertThat(dispatcher.getTopologyId(OTHER_CACHE)).isEqualTo(321);
      // The near cache learns that the second server no longer owns anything, so it stops relying on it
      ArgumentCaptor<CacheInfo> notified = ArgumentCaptor.forClass(CacheInfo.class);
      verify(listener, atLeastOnce()).afterTopologyUpdate(any(), notified.capture());
      assertThat(notified.getValue().getServers()).containsExactly(A);
   }

   /**
    * The socket kept open for another cache belongs to the generation the replacement connection superseded. Its
    * sequences are not comparable with the replacement's, so it can't change the topology of the cache that moved
    * to the replacement, through pushes or response headers.
    */
   public void testSocketKeptForAnotherCacheCannotOverrideReplacementTopology() {
      ReplacedWhileShared state = replaceFirstServerWhileOtherCacheUsesSecond(mock(NearCacheTopologyListener.class));
      OperationDispatcher dispatcher = state.dispatcher();

      push(dispatcher, state.oldB(), 101, 789, B);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);

      dispatcher.updateTopology("", null, 790, SERVERS, new SocketAddress[][]{{B}}, (short) 3, state.oldB());
      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);

      // The socket still serves the other cache
      dispatcher.updateTopology(OTHER_CACHE, null, 322, SERVERS, new SocketAddress[][]{{A}}, (short) 3, state.oldB());
      assertThat(dispatcher.getTopologyId(OTHER_CACHE)).isEqualTo(322);
   }

   /**
    * State where both caches use both servers through the original sockets: the default cache has a primary
    * listener ordered by those sockets, and another cache has received a topology listing both servers.
    *
    * @param dispatcher the dispatcher holding that state
    * @param oldA       the first server's original socket, which delivered the default cache's ordered snapshot
    * @param oldB       the second server's original socket
    */
   private record SharedByBothCaches(OperationDispatcher dispatcher, Channel oldA, Channel oldB) { }

   /**
    * Builds the state described by {@link SharedByBothCaches}.
    * <p>
    * The other cache receives topology 321 listing both servers. The default cache registers the listener and then
    * accepts a push from the first server with sequence 100 and topology 123, listing both servers.
    * </p>
    *
    * @param listener the near cache topology listener to register for the default cache before any topology
    * @return the resulting state
    */
   private SharedByBothCaches shareBothServersWithOtherCache(NearCacheTopologyListener listener) {
      OperationDispatcher dispatcher = dispatcher();
      Channel oldA = connect(dispatcher, A);
      Channel oldB = connect(dispatcher, B);
      dispatcher.addCacheTopologyInfoIfAbsent(OTHER_CACHE);
      dispatcher.updateTopology(OTHER_CACHE, null, 321, SERVERS, new SocketAddress[][]{{B}}, (short) 3);
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, oldA, 100, 123, A);
      return new SharedByBothCaches(dispatcher, oldA, oldB);
   }

   /**
    * State after the first server's replacement connection moved the default cache to a topology without the second
    * server, while another cache's topology still lists the second server.
    *
    * @param dispatcher the dispatcher holding that state
    * @param newA       the first server's replacement socket, the default cache's only primary listener socket
    * @param oldB       the second server's socket, from before the replacement and still open
    */
   private record ReplacedWhileShared(OperationDispatcher dispatcher, Channel newA, Channel oldB) { }

   /**
    * Builds the state described by {@link ReplacedWhileShared}.
    * <p>
    * Starts from {@link SharedByBothCaches}. Then the first server's replacement connection answers a request and
    * pushes a snapshot, both listing only the first server, with topology id 456 and a reset sequence.
    * </p>
    *
    * @param listener the near cache topology listener to register for the default cache before any topology
    * @return the resulting state
    */
   private ReplacedWhileShared replaceFirstServerWhileOtherCacheUsesSecond(NearCacheTopologyListener listener) {
      SharedByBothCaches shared = shareBothServersWithOtherCache(listener);
      OperationDispatcher dispatcher = shared.dispatcher();

      // The first server restarted, and its replacement connection reports a topology without the second server
      Channel newA = connect(dispatcher, A);
      dispatcher.updateTopology("", null, 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3, newA);
      dispatcher.updatePrimaryTopology("", newA, 0, -1, new BitSet(), 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3);
      return new ReplacedWhileShared(dispatcher, newA, shared.oldB());
   }

   /**
    * A response header from the socket kept open for another cache can't act as a replacement connection for the
    * cache that moved away from its server. A delayed header listing only that server would otherwise retire the
    * accepted replacement and put back the old, superseded topology.
    */
   public void testHeaderFromSocketKeptForAnotherCacheCannotRetireReplacement() {
      ReplacedWhileShared state = replaceFirstServerWhileOtherCacheUsesSecond(mock(NearCacheTopologyListener.class));
      OperationDispatcher dispatcher = state.dispatcher();
      // Otherwise the header is dropped as coming from a closed socket, and the test proves nothing
      assertThat(dispatcher.isCurrentChannel(state.oldB())).as("oldB is kept open for the other cache").isTrue();

      dispatcher.updateTopology("", null, 789, new InetSocketAddress[]{B}, new SocketAddress[][]{{B}}, (short) 3,
            state.oldB());

      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);
      assertThat(dispatcher.getCacheInfo("").getServers()).containsExactly(A);
      assertThat(dispatcher.primaryTopologyReady("", state.newA()).toCompletableFuture()).isCompleted();
   }

   /**
    * A push that drops a server from the cache's topology leaves that server's socket open while another cache still
    * lists it. The socket no longer serves the cache's primary listener, so it can't hold back the snapshot of a
    * later replacement connection, which would otherwise wait forever for the socket to retire.
    */
   public void testSocketKeptForAnotherCacheDoesNotHoldBackReplacementAfterShrink() {
      SharedByBothCaches shared = shareBothServersWithOtherCache(mock(NearCacheTopologyListener.class));
      OperationDispatcher dispatcher = shared.dispatcher();
      Channel oldA = shared.oldA();
      Channel oldB = shared.oldB();

      // The default cache shrinks to the first server, while the other cache keeps the second server's socket
      dispatcher.updatePrimaryTopology("", oldA, 101, 100, new BitSet(), 124, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(124);
      assertThat(dispatcher.isCurrentChannel(oldB)).isTrue();

      // The first server restarts, and its replacement connection pushes a snapshot with a reset sequence
      Channel newA = connect(dispatcher, A);
      dispatcher.retirePrimaryChannel(oldA);
      dispatcher.updatePrimaryTopology("", newA, 0, -1, new BitSet(), 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3);

      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);
      assertThat(dispatcher.primaryTopologyReady("", newA).toCompletableFuture()).isCompleted();
      assertThat(dispatcher.isCurrentChannel(oldB)).isTrue();
   }

   /**
    * A replacement connection's snapshot waits while old sockets still order the cache's topology. If a push then
    * drops the replacement's server, while another cache keeps its socket open, the waiting snapshot is stale: it
    * must not be applied once the old sockets retire, as it would put back the server the push dropped.
    */
   public void testShrinkDiscardsQueuedSnapshotOfSocketKeptForAnotherCache() {
      SharedByBothCaches shared = shareBothServersWithOtherCache(mock(NearCacheTopologyListener.class));
      OperationDispatcher dispatcher = shared.dispatcher();
      Channel oldA = shared.oldA();

      // The second server restarts, and its replacement snapshot with a reset sequence waits behind the first server's socket
      Channel newB = connect(dispatcher, B);
      CompletableFuture<Channel> newBReady = dispatcher.primaryTopologyReady("", newB).toCompletableFuture();
      dispatcher.updatePrimaryTopology("", newB, 0, -1, new BitSet(), 789, SERVERS,
            new SocketAddress[][]{{B}}, (short) 3);
      assertThat(dispatcher.getTopologyId("")).as("the replacement snapshot waits for the old sockets").isEqualTo(123);
      assertThat(newBReady).isNotDone();

      // The default cache shrinks to the first server, while the other cache keeps the second server's socket
      dispatcher.updatePrimaryTopology("", oldA, 101, 100, new BitSet(), 124, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(124);
      assertThat(dispatcher.isCurrentChannel(newB)).as("the other cache still lists the second server").isTrue();
      assertThat(newBReady).as("the registration on the dropped server can be retried").isCompletedExceptionally();

      // The first server's socket retires, the last one ordering the default cache
      connect(dispatcher, A);
      dispatcher.retirePrimaryChannel(oldA);

      assertThat(dispatcher.getTopologyId("")).as("the stale snapshot must not be applied").isEqualTo(124);
      assertThat(dispatcher.getCacheInfo("").getServers()).containsExactly(A);
   }

   /**
    * A replacement connection's response can arrive before its listener snapshot. Its topology drops the second
    * server, whose socket stays open for another cache, and no socket orders the cache's topology until the
    * replacement's snapshot arrives. A delayed push from the second server's socket in that window must not restore
    * the old ordering, which would hold back the replacement's snapshot.
    */
   public void testPushFromDetachedSocketCannotRestoreOrdering() {
      SharedByBothCaches shared = shareBothServersWithOtherCache(mock(NearCacheTopologyListener.class));
      OperationDispatcher dispatcher = shared.dispatcher();
      Channel oldB = shared.oldB();

      // The first server restarted, and its replacement connection answers a request before pushing its snapshot
      Channel newA = connect(dispatcher, A);
      CompletableFuture<Channel> newAReady = dispatcher.primaryTopologyReady("", newA).toCompletableFuture();
      dispatcher.updateTopology("", null, 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3, newA);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);
      assertThat(dispatcher.isCurrentChannel(oldB)).as("the other cache still lists the second server").isTrue();

      // A listener registered on the second server before the response pushes a delayed snapshot
      CompletableFuture<Channel> oldBReady = dispatcher.primaryTopologyReady("", oldB).toCompletableFuture();
      push(dispatcher, oldB, 101, 789, B);

      assertThat(dispatcher.getTopologyId("")).as("the dropped server's push must not be applied").isEqualTo(456);
      assertThat(oldBReady).as("the registration on the dropped server can be retried").isCompletedExceptionally();

      // The replacement's snapshot, with a reset sequence, is applied at once
      dispatcher.updatePrimaryTopology("", newA, 0, -1, new BitSet(), 457, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(457);
      assertThat(newAReady).isCompletedWithValue(newA);
   }

   /**
    * Between a replacement connection's response and its listener snapshot, no socket orders the cache's topology.
    * A delayed response header from the second server's socket, kept open for another cache, must not list that
    * server again: its later pushes would then pass as coming from a listed server and restore the old ordering.
    */
   public void testHeaderFromDetachedSocketCannotRelistServer() {
      SharedByBothCaches shared = shareBothServersWithOtherCache(mock(NearCacheTopologyListener.class));
      OperationDispatcher dispatcher = shared.dispatcher();
      Channel oldB = shared.oldB();

      // The first server restarted, and its replacement connection answers a request before pushing its snapshot
      Channel newA = connect(dispatcher, A);
      dispatcher.updateTopology("", null, 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3, newA);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(456);
      assertThat(dispatcher.isCurrentChannel(oldB)).as("the other cache still lists the second server").isTrue();

      // A delayed response on the second server's socket still lists both servers
      dispatcher.updateTopology("", null, 790, SERVERS, new SocketAddress[][]{{B}}, (short) 3, oldB);

      assertThat(dispatcher.getTopologyId("")).as("the dropped server's header must not be applied").isEqualTo(456);
      assertThat(dispatcher.getCacheInfo("").getServers()).containsExactly(A);
   }

   /**
    * The server sends the registration snapshot exactly once. If it arrives while a cluster switch is in progress
    * ({@code priorAgeOperations != null}) it is dropped by {@code applyTopology}, so the readiness future must not be
    * left pending forever: nothing will ever re-deliver the snapshot, and the registration would hang.
    */
   public void testRegistrationSnapshotDuringClusterSwitchDoesNotHang() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      CompletableFuture<Channel> ready = dispatcher.primaryTopologyReady("", a).toCompletableFuture();

      // Simulate a cluster switch that still has operations from the previous age in flight
      Set<HotRodOperation<?>> priorAge = ConcurrentHashMap.newKeySet();
      priorAge.add(NoHotRodOperation.instance());
      TestingUtil.replaceField(priorAge, "priorAgeOperations", dispatcher, OperationDispatcher.class);
      push(dispatcher, a, 100, 123, A);

      // The cluster switch finishes, the snapshot is not resent
      TestingUtil.replaceField(null, "priorAgeOperations", dispatcher, OperationDispatcher.class);

      assertThat(ready).as("registration snapshot was dropped, the listener registration must not wait forever").isDone();
   }

   public void testResponseCannotOverwriteSequencedPush() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      push(dispatcher, a, 100, 123, A);
      dispatcher.updateTopology("", null, 456, SERVERS, new SocketAddress[][]{{B}}, (short) 3, a);
      assertThat(primary(dispatcher)).isEqualTo(A);
      Channel replacement = connect(dispatcher, A);
      dispatcher.updateTopology("", null, 789, SERVERS, new SocketAddress[][]{{B}}, (short) 3, a);
      assertThat(dispatcher.isCurrentChannel(replacement)).isTrue();
      assertThat(primary(dispatcher)).isEqualTo(A);
   }

   /**
    * The hash is an opaque identity. A newer server topology ID is accepted whatever its hash is numerically, and
    * an older one is rejected whatever its hash is.
    */
   public void testServerTopologyIdOrdersPushesNotTheHash() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      push(dispatcher, a, 100, 1_000_000, A);
      // Newer server ID, numerically smaller hash
      push(dispatcher, a, 101, 100, segments(0), -5, B);
      assertThat(primary(dispatcher)).isEqualTo(B);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(-5);
      // Older server IDs, numerically larger hashes
      push(dispatcher, a, 100, 1_000_000, A);
      push(dispatcher, a, 99, 2_000_000, A);
      assertThat(primary(dispatcher)).isEqualTo(B);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(-5);
   }

   /**
    * The changed segments are fenced while the previous owners are still published, also when the owners the push
    * leads to are the ones that were published before.
    */
   public void testChangedSegmentsAreFencedBeforeTheTopologyIsPublished() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      List<String> events = new ArrayList<>();
      // The listener runs under the write lock, so it must not read the dispatcher
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      doAnswer(invocation -> {
         events.add("fence " + invocation.getArgument(0));
         return null;
      }).when(listener).primaryOwnersChanged(any());
      doAnswer(invocation -> {
         events.add("before " + invocation.<CacheInfo>getArgument(1).getTopologyId());
         return null;
      }).when(listener).beforeTopologyUpdate(any(), any());
      doAnswer(invocation -> {
         events.add("published " + invocation.<CacheInfo>getArgument(1).getTopologyId());
         return null;
      }).when(listener).afterTopologyUpdate(any(), any());
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, a, 100, 123, A);
      // Ownership of segment 0 moved to B and back to A in coalesced pushes, with the same hash
      push(dispatcher, a, 102, 100, segments(0), 123, A);
      assertThat(events).containsExactly("before 123", "published 123", "fence {0}", "published 123");
      assertThat(primary(dispatcher)).isEqualTo(A);
   }

   public void testRemovingListenerRemovesAllCallbacksAndPreservesOtherListeners() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      NearCacheTopologyListener removed = mock(NearCacheTopologyListener.class);
      NearCacheTopologyListener remaining = mock(NearCacheTopologyListener.class);
      dispatcher.addTopologyListener("", removed);
      dispatcher.addTopologyListener("", remaining);
      push(dispatcher, a, 100, 123, A);
      clearInvocations(removed, remaining);

      dispatcher.removeTopologyListener("", removed);
      push(dispatcher, a, 101, 100, segments(0), 456, B);

      verifyNoInteractions(removed);
      verify(remaining).primaryOwnersChanged(segments(0));
      verify(remaining).beforeTopologyUpdate(any(), any());
      verify(remaining).afterTopologyUpdate(any(), any());
      // Removing the last registration also releases topology readiness and ordering state.
      CompletableFuture<Channel> ready = dispatcher.primaryTopologyReady("", connect(dispatcher, A)).toCompletableFuture();
      dispatcher.removeTopologyListener("", remaining);
      assertThat(ready).isCompletedExceptionally();
      PrimaryTopologyTracker tracker = TestingUtil.extractField(dispatcher, "primaryTopologyTracker");
      Map<?, ?> topologies = TestingUtil.extractField(tracker, "primaryTopologies");
      assertThat(topologies).isEmpty();
   }

   public void testRemovingLastListenerPreservesOtherCacheState() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      NearCacheTopologyListener removed = mock(NearCacheTopologyListener.class);
      NearCacheTopologyListener remaining = mock(NearCacheTopologyListener.class);
      dispatcher.addTopologyListener("", removed);
      dispatcher.addTopologyListener("other", remaining);
      CompletableFuture<Channel> removedReady = dispatcher.primaryTopologyReady("", a).toCompletableFuture();
      CompletableFuture<Channel> remainingReady = dispatcher.primaryTopologyReady("other", a).toCompletableFuture();

      dispatcher.removeTopologyListener("other", removed);
      dispatcher.removeTopologyListener("", removed);

      assertThat(removedReady).isCompletedExceptionally();
      assertThat(remainingReady).isNotDone();
      dispatcher.addCacheTopologyInfoIfAbsent("other");
      dispatcher.updatePrimaryTopology("other", a, 100, -1, new BitSet(), 123, SERVERS,
            new SocketAddress[][]{{A}}, (short) 3);
      assertThat(remainingReady).isCompletedWithValue(a);
      verify(remaining).afterTopologyUpdate(any(), any());
      verifyNoInteractions(removed);
   }

   public void testPushWithoutHistoryFencesEverySegment() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      List<BitSet> fenced = new ArrayList<>();
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      doAnswer(invocation -> {
         fenced.add(invocation.getArgument(0));
         return null;
      }).when(listener).primaryOwnersChanged(any());
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, a, 100, 123, A);
      // The server has no history before 105, but 100 was the last accepted push
      push(dispatcher, a, 110, 105, new BitSet(), 456, A);
      assertThat(fenced).containsExactly((BitSet) null);
   }

   public void testConsecutiveRegistrationSnapshotDoesNotFenceUnchangedOwners() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      Channel b = connect(dispatcher, B);
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, a, 100, 123, A);
      clearInvocations(listener);

      // A new listener snapshots the final rebalance phase before the existing listener sends it.
      push(dispatcher, b, 101, -1, new BitSet(), 123, A);

      verify(listener, never()).primaryOwnersChanged(any());
      verify(listener).afterTopologyUpdate(any(), any());
      assertThat(dispatcher.primaryTopologyReady("", b).toCompletableFuture()).isCompletedWithValue(b);
      // The old listener's notification for the same phase must now be ignored.
      clearInvocations(listener);
      push(dispatcher, a, 101, 100, new BitSet(), 123, A);
      verifyNoInteractions(listener);
   }

   public void testRegistrationSnapshotWithSequenceGapFencesEverySegment() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      Channel b = connect(dispatcher, B);
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, a, 100, 123, A);
      clearInvocations(listener);

      // Identical final owners cannot rule out a move away and back in the missing topology.
      push(dispatcher, b, 102, -1, new BitSet(), 123, A);

      verify(listener).primaryOwnersChanged(null);
   }

   public void testConsecutiveRegistrationSnapshotWithHashCollisionUpdatesOwners() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      Channel b = connect(dispatcher, B);
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, a, 100, 123, A);
      clearInvocations(listener);

      // Different owners can have the same hash. The sequence still makes this snapshot authoritative.
      push(dispatcher, b, 101, -1, new BitSet(), 123, B);

      assertThat(primary(dispatcher)).isEqualTo(B);
      verify(listener).beforeTopologyUpdate(any(), any());
      verify(listener).afterTopologyUpdate(any(), any());
      assertThat(dispatcher.primaryTopologyReady("", b).toCompletableFuture()).isCompletedWithValue(b);
   }

   public void testNothingIsFencedWhenNoPrimaryOwnerChanged() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      List<BitSet> fenced = new ArrayList<>();
      NearCacheTopologyListener listener = mock(NearCacheTopologyListener.class);
      doAnswer(invocation -> {
         fenced.add(invocation.getArgument(0));
         return null;
      }).when(listener).primaryOwnersChanged(any());
      dispatcher.addTopologyListener("", listener);
      push(dispatcher, a, 100, 123, A);
      push(dispatcher, a, 101, 456, A);
      assertThat(fenced).isEmpty();
   }

   /** A replacement membership without any previous owner retires the old sockets, so its IDs may start lower. */
   public void testReplacementMembershipAcceptsResetSequence() {
      OperationDispatcher dispatcher = dispatcher();
      Channel oldA = connect(dispatcher, A);
      Channel oldB = connect(dispatcher, B);
      push(dispatcher, oldA, 5000, 123, A);
      oldA.close();
      oldB.close();
      dispatcher.retirePrimaryChannel(oldA);
      dispatcher.retirePrimaryChannel(oldB);
      Channel newA = connect(dispatcher, A);
      connect(dispatcher, B);
      push(dispatcher, newA, 1, -1, new BitSet(), 999, B);
      assertThat(primary(dispatcher)).isEqualTo(B);
      // The old view is gone: its delayed messages are rejected
      push(dispatcher, oldA, 5001, 5000, new BitSet(), 123, A);
      assertThat(primary(dispatcher)).isEqualTo(B);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(999);
   }

   /**
    * Ordinary responses have no sequence, so they must not be ordered numerically against each other, but they
    * must not replace a topology that was established by sequenced pushes either.
    */
   public void testResponsesAreNotOrderedByHash() {
      OperationDispatcher dispatcher = dispatcher();
      Channel a = connect(dispatcher, A);
      connect(dispatcher, B);
      HotRodOperation<?> op = mock(HotRodOperation.class);
      dispatcher.updateTopology("", op, 10, SERVERS, new SocketAddress[][]{{A}}, (short) 3, a);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(10);
      // Without a sequence there is no way to tell which one is newer, so a different hash is taken as a change
      dispatcher.updateTopology("", op, 5, SERVERS, new SocketAddress[][]{{B}}, (short) 3, a);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(5);
      assertThat(primary(dispatcher)).isEqualTo(B);
      // An established sequence can't be overwritten by a response
      push(dispatcher, a, 100, 777, A);
      dispatcher.updateTopology("", op, 1000, SERVERS, new SocketAddress[][]{{B}}, (short) 3, a);
      assertThat(dispatcher.getTopologyId("")).isEqualTo(777);
      assertThat(primary(dispatcher)).isEqualTo(A);
   }

   /**
    * A primary listener registration is bound to the owner it targets. When its channel goes away while it is still
    * queued, it fails instead of being sent to another server.
    */
   public void testQueuedPrimaryRegistrationIsNotRelocatedWhenItsChannelIsReplaced() {
      InternalRemoteCache<?, ?> cache = mock(InternalRemoteCache.class);
      when(cache.getName()).thenReturn("");
      queuedListenerOperationIsNotRelocated(new AddPrimaryNearCacheClientListenerOperation(cache, new Object(), new byte[]{1}, 0));
   }

   public void testQueuedListenerRemovalIsNotRelocatedWhenItsChannelIsReplaced() {
      queuedListenerOperationIsNotRelocated(removal());
   }

   private HotRodOperation<Void> removal() {
      InternalRemoteCache<?, ?> cache = mock(InternalRemoteCache.class);
      when(cache.getName()).thenReturn("");
      return new RemoveClientListenerOperation(cache, mock(ClientListenerNotifier.class), new byte[]{1}) { };
   }

   private HotRodOperation<Void> bloomUpdate() {
      InternalRemoteCache<?, ?> cache = mock(InternalRemoteCache.class);
      when(cache.getName()).thenReturn("");
      return new UpdateBloomFilterOperation(cache, new byte[]{1}) { };
   }

   public void testQueuedBloomFilterUpdateIsNotRelocatedWhenItsChannelIsReplaced() {
      queuedListenerOperationIsNotRelocated(bloomUpdate());
   }

   public void testListenerRemovalDoesNotRelocateFromSuspectedOwnerOrRetry() {
      operationDoesNotRelocateFromSuspectedOwnerOrRetry(removal());
   }

   public void testBloomFilterUpdateDoesNotRelocateFromSuspectedOwnerOrRetry() {
      operationDoesNotRelocateFromSuspectedOwnerOrRetry(bloomUpdate());
   }

   private void operationDoesNotRelocateFromSuspectedOwnerOrRetry(HotRodOperation<?> operation) {
      OperationDispatcher dispatcher = dispatcher();
      connect(dispatcher, A);
      connect(dispatcher, B);
      ConcurrentMap<SocketAddress, OperationChannel> connections = TestingUtil.extractField(dispatcher.getChannelHandler(), "channels");
      dispatcher.getConnectionFailedServers().add(A);

      dispatcher.executeOnSingleAddress(operation, A);
      verify(connections.get(A)).sendOperation(operation);
      verify(connections.get(B), never()).sendOperation(operation);

      dispatcher.handleResponse(operation, -1, A, null, new IOException("A is down"));
      assertThat(operation.asCompletableFuture()).isCompletedExceptionally();
      verify(connections.get(B), never()).sendOperation(operation);
   }

   private void queuedListenerOperationIsNotRelocated(HotRodOperation<?> operation) {
      ExecutorService inline = mock(ExecutorService.class);
      org.mockito.Mockito.doAnswer(invocation -> {
         ((Runnable) invocation.getArgument(0)).run();
         return CompletableFuture.completedFuture(null);
      }).when(inline).submit(org.mockito.ArgumentMatchers.any(Runnable.class));
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host(A.getHostString()).port(A.getPort());
      builder.addServer().host(B.getHostString()).port(B.getPort());
      OperationDispatcher dispatcher = new OperationDispatcher(builder.build(), inline, new DefaultTimeService(),
            mock(ClientListenerNotifier.class), null);
      connect(dispatcher, A);
      connect(dispatcher, B);
      ConcurrentMap<SocketAddress, OperationChannel> connections = TestingUtil.extractField(dispatcher.getChannelHandler(), "channels");
      OperationChannel channelA = connections.get(A);
      OperationChannel channelB = connections.get(B);

      // The owner-bound operation is queued on A when A is dropped from the topology
      org.mockito.Mockito.doAnswer(invocation -> {
         invocation.<io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer<HotRodOperation<?>>>getArgument(0).accept(operation);
         return null;
      }).when(channelA).drainQueue(org.mockito.ArgumentMatchers.any());

      dispatcher.updateTopology("", mock(HotRodOperation.class), 77, new java.net.InetSocketAddress[]{B},
            new SocketAddress[][]{{B}}, (short) 3);

      assertThat(operation.asCompletableFuture()).isCompletedExceptionally();
      org.mockito.Mockito.verify(channelB, org.mockito.Mockito.never()).sendOperation(operation);
   }
}
