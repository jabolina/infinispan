package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ClusterConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.TopologyInfo;
import org.infinispan.client.hotrod.impl.Util;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.DelegatingHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.topology.ClusterInfo;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.concurrent.CompletionStages;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderException;
import net.jcip.annotations.GuardedBy;

/**
 * This class handles dispatching various HotRodOperations to the appropriate server.
 * It also handles retries and failover operations.
 * <p>
 * Depending upon the operation it is first determined which server it should go to, it is then
 * sent to the ChannelHandler which creates a new channel if needed for the operation which then
 * hands it off to the OperationChannel which actually dispatches the command to the socket.
 * <p>
 * When an operation is dispatched it is first registered with the {@link HeaderDecoder} so
 * that its response can be processed.
 */
public class OperationDispatcher {
   private static final Log log = LogFactory.getLog(OperationDispatcher.class, Log.class);
   public static final String DEFAULT_CLUSTER_NAME = "___DEFAULT-CLUSTER___";

   private final StampedLock lock = new StampedLock();
   private final List<ClusterInfo> clusters;
   private final int maxRetries;
   // TODO: need to add to this on retries
   private final AtomicLong retryCounter = new AtomicLong();
   // The internal state of this is not thread safe
   @GuardedBy("lock")
   private final TopologyInfo topologyInfo;
   @GuardedBy("lock")
   private CompletableFuture<Void> clusterSwitchStage;
   // Servers for which the last connection attempt failed - when this matches the current topology we will
   // fail back to initial list and if all of those fail we fail over to a different cluster if present

   // This is normally null, however when a cluster switch happens this will be initialized and any currently
   // pending operations will be added to it. Then when a command is completed it will be removed from this
   // Set and set back to null again when empty
   @GuardedBy("lock")
   private Set<HotRodOperation<?>> priorAgeOperations = null;
   private final ChannelHandler channelHandler;

   private final ClientListenerNotifier clientListenerNotifier;

   public OperationDispatcher(Configuration configuration, ExecutorService executorService,
                              ClientListenerNotifier clientListenerNotifier, Consumer<ChannelPipeline> pipelineDecorator) {
      this.clientListenerNotifier = clientListenerNotifier;
      this.maxRetries = configuration.maxRetries();

      List<InetSocketAddress> initialServers = new ArrayList<>();
      for (ServerConfiguration server : configuration.servers()) {
         initialServers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
      }
      ClusterInfo mainCluster = new ClusterInfo(DEFAULT_CLUSTER_NAME, initialServers, configuration.clientIntelligence(),
            configuration.security().ssl().sniHostName());

      this.topologyInfo = new TopologyInfo(configuration, mainCluster);

      List<ClusterInfo> clustersDefinitions = new ArrayList<>();
      if (log.isDebugEnabled()) {
         log.debugf("Statically configured servers: %s", initialServers);
         log.debugf("Tcp no delay = %b; client socket timeout = %d ms; connect timeout = %d ms",
               configuration.tcpNoDelay(), configuration.socketTimeout(), configuration.connectionTimeout());
      }

      if (!configuration.clusters().isEmpty()) {
         for (ClusterConfiguration clusterConfiguration : configuration.clusters()) {
            List<InetSocketAddress> alternateServers = new ArrayList<>();
            for (ServerConfiguration server : clusterConfiguration.getCluster()) {
               alternateServers.add(InetSocketAddress.createUnresolved(server.host(), server.port()));
            }
            ClientIntelligence intelligence = clusterConfiguration.getClientIntelligence() != null ?
                  clusterConfiguration.getClientIntelligence() :
                  configuration.clientIntelligence();

            String sniHostName = clusterConfiguration.sniHostName() != null ? clusterConfiguration.sniHostName() : configuration.security().ssl().sniHostName();
            ClusterInfo alternateCluster =
                  new ClusterInfo(clusterConfiguration.getClusterName(), alternateServers, intelligence, sniHostName);
            log.debugf("Add secondary cluster: %s", alternateCluster);
            clustersDefinitions.add(alternateCluster);
         }
         clustersDefinitions.add(mainCluster);
      }
      clusters = List.copyOf(clustersDefinitions);

      // Use the configuration sni host name
      channelHandler = new ChannelHandler(configuration, topologyInfo.getCluster().getSniHostName(), executorService,
            this, pipelineDecorator);

      topologyInfo.getOrCreateCacheInfo(HotRodConstants.DEFAULT_CACHE_NAME);
   }

   public CacheInfo getCacheInfo(String cacheName) {
      // This method is invoked for EVERY key based cache operation, so we optimize the optimistic read mode
      long stamp = lock.tryOptimisticRead();
      CacheInfo cacheInfo = topologyInfo.getCacheInfo(cacheName);
      if (lock.validate(stamp)) {
         stamp = lock.readLock();
         try {
            cacheInfo = topologyInfo.getCacheInfo(cacheName);
         } finally {
            lock.unlockRead(stamp);
         }
      }
      return cacheInfo;
   }

   ClusterInfo getClusterInfo() {
      long stamp = lock.readLock();
      try {
         return topologyInfo.getCluster();
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public ClientListenerNotifier getClientListenerNotifier() {
      return clientListenerNotifier;
   }

   public void start() {
      Util.await(CompletionStages.performSequentially(topologyInfo.getCluster().getInitialServers().iterator(),
            channelHandler::startChannelIfNeeded));
   }

   public void stop() {
      try {
         channelHandler.close();
      } catch (Exception e) {
         log.warn("Exception while shutting down the operation dispatcher.", e);
      }
   }

   public <E> CompletionStage<E> execute(HotRodOperation<E> operation) {
      return execute(operation, Set.of());
   }

   public <E> CompletionStage<E> execute(HotRodOperation<E> operation, Set<SocketAddress> failedServers) {
      Object routingObj = operation.getRoutingObject();
      if (routingObj != null) {
         CacheInfo cacheInfo = getCacheInfo(operation.getCacheName());
         if (cacheInfo != null && cacheInfo.getConsistentHash() != null) {
            SocketAddress server = cacheInfo.getConsistentHash().getServer(routingObj);
            if (server != null && (failedServers == null || !failedServers.contains(server))) {
               return executeOnSingleAddress(operation, server);
            }
         }
      }
      SocketAddress targetAddress = getBalancer(operation.getCacheName()).nextServer(failedServers);
      return executeOnSingleAddress(operation, targetAddress);
   }

   public <E> CompletionStage<E> executeOnSingleAddress(HotRodOperation<E> operation, SocketAddress socketAddress) {
      return channelHandler.submitOperation(operation, socketAddress);
   }

   public FailoverRequestBalancingStrategy getBalancer(String cacheName) {
      return getCacheInfo(cacheName).getBalancer();
   }

   public ClientIntelligence getClientIntelligence() {
      return getClusterInfo().getIntelligence();
   }

   public CacheTopologyInfo getCacheTopologyInfo(String cacheName) {
      return getCacheInfo(cacheName).getCacheTopologyInfo();
   }

   public ClientTopology getClientTopologyInfo(String cacheName) {
      return getCacheInfo(cacheName).getClientTopologyRef().get();
   }

   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByAddress(String cacheName) {
      CacheInfo cacheInfo = getCacheInfo(cacheName);
      return cacheInfo != null ? cacheInfo.getPrimarySegments() : null;
   }

   public Collection<InetSocketAddress> getServers() {
      long stamp = lock.readLock();
      try {
         return topologyInfo.getAllServers();
      } finally {
         lock.unlockRead(stamp);
      }
   }

   @GuardedBy("lock")
   private boolean fromPreviousAge(HotRodOperation<?> operation) {
      return priorAgeOperations != null && priorAgeOperations.contains(operation);
   }

   public void updateTopology(String cacheName, HotRodOperation<?> operation, int responseTopologyId,
                              InetSocketAddress[] addresses, SocketAddress[][] segmentOwners, short hashFunctionVersion) {
      long stamp = lock.writeLock();
      try {
         CacheInfo cacheInfo = topologyInfo.getCacheInfo(cacheName);
         assert cacheInfo != null : "The cache info must exist before receiving a topology update";

         // Only accept the update if it's from the current age and the topology id is greater than the current one
         // Relies on TopologyInfo.switchCluster() to update the topologyAge for caches first
         if (priorAgeOperations == null && responseTopologyId != cacheInfo.getTopologyId()) {
            List<InetSocketAddress> addressList = Arrays.asList(addresses);
            // We don't track topology ages anymore as a number
            HOTROD.newTopology(responseTopologyId, -1, addresses.length, addressList);
            CacheInfo newCacheInfo;
            if (hashFunctionVersion >= 0) {
               SegmentConsistentHash consistentHash =
                     createConsistentHash(segmentOwners, hashFunctionVersion, cacheInfo.getCacheName());
               newCacheInfo = cacheInfo.withNewHash(responseTopologyId, addressList,
                     consistentHash, segmentOwners.length);
            } else {
               newCacheInfo = cacheInfo.withNewServers(responseTopologyId, addressList);
            }
            updateCacheInfo(cacheName, newCacheInfo);
         } else {
            if (log.isTraceEnabled())
               log.tracef("[%s] Ignoring outdated topology: topology id = %s, previous topology age = %s, servers = %s",
                     cacheInfo.getCacheName(), responseTopologyId, fromPreviousAge(operation),
                     Arrays.toString(addresses));
         }
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   private SegmentConsistentHash createConsistentHash(SocketAddress[][] segmentOwners, short hashFunctionVersion,
                                                      String cacheNameString) {
      if (log.isTraceEnabled()) {
         if (hashFunctionVersion == 0)
            log.tracef("[%s] Not using a consistent hash function (hash function version == 0).",
                  cacheNameString);
         else
            log.tracef("[%s] Updating client hash function with %s number of segments",
                  cacheNameString, segmentOwners.length);
      }
      return topologyInfo.createConsistentHash(segmentOwners.length, hashFunctionVersion, segmentOwners);
   }

   @GuardedBy("lock")
   protected void updateCacheInfo(String cacheName, CacheInfo newCacheInfo) {
      List<InetSocketAddress> newServers = newCacheInfo.getServers();
      CacheInfo oldCacheInfo = topologyInfo.getCacheInfo(cacheName);
      List<InetSocketAddress> oldServers = oldCacheInfo.getServers();
      Set<SocketAddress> addedServers = new HashSet<>(newServers);
      oldServers.forEach(addedServers::remove);
      Set<SocketAddress> removedServers = new HashSet<>(oldServers);
      newServers.forEach(removedServers::remove);
      if (log.isTraceEnabled()) {
         String cacheNameString = newCacheInfo.getCacheName();
         log.tracef("[%s] Current list: %s", cacheNameString, oldServers);
         log.tracef("[%s] New list: %s", cacheNameString, newServers);
         log.tracef("[%s] Added servers: %s", cacheNameString, addedServers);
         log.tracef("[%s] Removed servers: %s", cacheNameString, removedServers);
      }

      // First add new servers. For servers that went down, the returned transport will fail for now
      for (SocketAddress server : addedServers) {
         HOTROD.newServerAdded(server);
         // We don't wait for server to connect as it will just enqueue as needed
         channelHandler.startChannelIfNeeded(server);
      }

      // Then update the server list for new operations
      topologyInfo.updateCacheInfo(cacheName, oldCacheInfo, newCacheInfo);

      // And finally remove the failed servers
      for (SocketAddress server : removedServers) {
         HOTROD.removingServer(server);
         channelHandler.closeChannel(server);
      }
   }

   public void onConnectionEvent(SocketAddress address) {
//      boolean allInitialServersFailed;
//      long stamp = lock.writeLock();
//      try {
//         // TODO: need to do this
//         if (type == ChannelEventType.CONNECTED) {
//            failedServers.remove(pool.getAddress());
//            return;
//         } else if (type == ChannelEventType.CONNECT_FAILED) {
//            if (pool.getConnected() == 0) {
//               failedServers.add(pool.getAddress());
//            }
//         } else {
//            // Nothing to do
//            return;
//         }
//
//         if (log.isTraceEnabled())
//            log.tracef("Connection attempt failed, we now have %d servers with no established connections: %s",
//                  failedServers.size(), failedServers);
//         allInitialServersFailed = failedServers.containsAll(topologyInfo.getCluster().getInitialServers());
//         if (!allInitialServersFailed || clusters.isEmpty()) {
//            resetCachesWithFailedServers();
//         }
//      } finally {
//         lock.unlockWrite(stamp);
//      }
//
//      if (allInitialServersFailed && !clusters.isEmpty()) {
//         trySwitchCluster();
//      }
   }

   private void trySwitchCluster() {
      ClusterInfo cluster;
      long stamp = lock.writeLock();
      try {
         cluster = topologyInfo.getCluster();
         if (clusterSwitchStage != null) {
            if (log.isTraceEnabled())
               log.trace("Cluster switch is already in progress");
            return;
         }

         clusterSwitchStage = new CompletableFuture<>();
      } finally {
         lock.unlockWrite(stamp);
      }

      // TODO: need to do this
//      checkServersAlive(cluster.getInitialServers())
//            .thenCompose(alive -> {
//               if (alive) {
//                  // The live check removed the server from failedServers when it established a connection
//                  if (log.isTraceEnabled()) log.tracef("Cluster %s is still alive, not switching", cluster);
//                  return CompletableFuture.completedFuture(null);
//               }
//
//               if (log.isTraceEnabled())
//                  log.tracef("Trying to switch cluster away from '%s'", cluster.getName());
//               return findLiveCluster(cluster, ageBeforeSwitch);
//            })
//            .thenAccept(newCluster -> {
//               if (newCluster != null) {
//                  automaticSwitchToCluster(newCluster, cluster, ageBeforeSwitch);
//               }
//            })
//            .whenComplete((__, t) -> completeClusterSwitch());
   }

   private void automaticSwitchToCluster(ClusterInfo newCluster, ClusterInfo failedCluster, int ageBeforeSwitch) {
      long stamp = lock.writeLock();
      try {
         if (clusterSwitchStage == null || priorAgeOperations != null) {
            log.debugf("Cluster switch already completed by another thread, bailing out");
            return;
         }

         log.debugf("Switching to cluster %s, servers: %s",   newCluster.getName(), newCluster.getInitialServers());
         markPendingOperationsAsPriorAge();
         topologyInfo.switchCluster(newCluster);
      } finally {
         lock.unlockWrite(stamp);
      }

      if (!newCluster.getName().equals(DEFAULT_CLUSTER_NAME))
         HOTROD.switchedToCluster(newCluster.getName());
      else
         HOTROD.switchedBackToMainCluster();
   }

   public boolean manualSwitchToCluster(String clusterName) {
      if (clusters.isEmpty()) {
         log.debugf("No alternative clusters configured, so can't switch cluster");
         return false;
      }

      ClusterInfo newCluster = null;
      for (ClusterInfo cluster : clusters) {
         if (cluster.getName().equals(clusterName))
            newCluster = cluster;
      }
      if (newCluster == null) {
         log.debugf("Cluster named %s does not exist in the configuration", clusterName);
         return false;
      }

      long stamp = lock.writeLock();
      boolean shouldComplete = false;
      try {
         if (clusterSwitchStage != null) {
            log.debugf("Another cluster switch is already in progress, overriding it");
            shouldComplete = true;
         }
         log.debugf("Switching to cluster %s, servers: %s", clusterName, newCluster.getInitialServers());
         markPendingOperationsAsPriorAge();
         topologyInfo.switchCluster(newCluster);
      } finally {
         lock.unlockWrite(stamp);
      }

      if (!clusterName.equals(DEFAULT_CLUSTER_NAME))
         HOTROD.manuallySwitchedToCluster(clusterName);
      else
         HOTROD.manuallySwitchedBackToMainCluster();

      if (shouldComplete) {
         completeClusterSwitch();
      }
      return true;
   }

   @GuardedBy("lock")
   private void markPendingOperationsAsPriorAge() {
      // TODO: this has a window where if a command is about to be dispatched and then a new topology age is installed
      // that that command may not be found and flagged as from the old topology
      if (priorAgeOperations == null) {
         priorAgeOperations = ConcurrentHashMap.newKeySet();
      }
      channelHandler.gatherOperations().forEach(op -> {
         priorAgeOperations.add(op);
         op.whenComplete((e, t) -> {
            priorAgeOperations.remove(op);
            // This check is done in case if there are multiple switches before all commands are finished
            if (priorAgeOperations.isEmpty()) {
               // All commands should be done, but actually acquire lock to make sure no other switches
               long stamp = lock.writeLock();
               try {
                  if (priorAgeOperations.isEmpty()) {
                     priorAgeOperations = null;
                  }
               } finally {
                  lock.unlockWrite(stamp);
               }
            }
         });
      });
   }

   private void completeClusterSwitch() {
      CompletableFuture<Void> localStage;
      long stamp = lock.writeLock();
      try {
         localStage = this.clusterSwitchStage;
         this.clusterSwitchStage = null;
      } finally {
         lock.unlockWrite(stamp);
      }

      // An automatic cluster switch could be cancelled by a manual switch,
      // and a manual cluster switch would not have a stage to begin with
      if (localStage != null) {
         localStage.complete(null);
      }
   }

   /**
    * This method is called back whenever an operation is completed either through success (null Throwable)
    * or through exception/error (non null Throwable)
    * @param op the operation that completed
    * @param channel the channel that performed the operation
    * @param returnValue the return value for the operation (may be null)
    * @param t the throwable, which if a problem was encountered will be not null, otherwise null
    * @param <E> the operation result type
    */
   public <E> void handleResponse(HotRodOperation<E> op, Channel channel, E returnValue, Throwable t) {
      if (log.isTraceEnabled()) {
         log.tracef("Completing response with value %s or exception %s into op %s", returnValue, t, op);
      }
      if (t != null) {
         RetryingHotRodOperation<?> retryOp = checkException(t, channel, op);
         if (retryOp != null) {
            logAndRetryOrFail(t, retryOp);
         }
      } else {
         op.complete(returnValue);
      }
   }

   static class RetryingHotRodOperation<T> extends DelegatingHotRodOperation<T> {
      private final Set<SocketAddress> failedServers;
      private int retryCount;

      static <T> RetryingHotRodOperation<T> retryingOp(HotRodOperation<T> op) {
         if (op instanceof RetryingHotRodOperation<T> operation) {
            return operation;
         }
         return new RetryingHotRodOperation<>(op);
      }

      RetryingHotRodOperation(HotRodOperation<T> op) {
         super(op);

         this.failedServers = new HashSet<>();
      }

      void addFailedServer(SocketAddress socketAddress) {
         failedServers.add(socketAddress);
      }

      int incrementRetry() {
         return ++retryCount;
      }

      public Set<SocketAddress> getFailedServers() {
         return failedServers;
      }
   }

   private RetryingHotRodOperation<?> checkException(Throwable cause, Channel channel, HotRodOperation<?> op) {
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      if (cause instanceof RemoteNodeSuspectException) {
         // TODO Clients should never receive a RemoteNodeSuspectException, see ISPN-11636
         return RetryingHotRodOperation.retryingOp(op);
      } else if (isServerError(cause)) {
         op.completeExceptionally(cause);
         return null;
      } else {
         if (Thread.interrupted()) {
            InterruptedException e = new InterruptedException();
            e.addSuppressed(cause);
            op.completeExceptionally(e);
            return null;
         }
         var retrying = RetryingHotRodOperation.retryingOp(op);
         SocketAddress server = channelHandler.unresolvedAddressForChannel(channel);
         if (server != null) {
            retrying.addFailedServer(server);
         }
         return retrying;
      }
   }

   protected final boolean isServerError(Throwable t) {
      return t instanceof HotRodClientException && ((HotRodClientException) t).isServerError();
   }

   protected void logAndRetryOrFail(Throwable e, RetryingHotRodOperation<?> op) {
      int retryAttempt = op.incrementRetry();
      if (retryAttempt <= maxRetries) {
         if (log.isTraceEnabled()) {
            log.tracef(e, "Exception encountered in %s. Retry %d out of %d", this, retryAttempt, maxRetries);
         }
         retryCounter.incrementAndGet();
         execute(op, op.getFailedServers());
      } else {
         HOTROD.exceptionAndNoRetriesLeft(retryAttempt, maxRetries, e);
         op.completeExceptionally(e);
      }
   }

   public void handleConnectionFailure(OperationChannel operationChannel, Throwable t) {
      // TODO: need to add to suspect and retry operations
   }

   public String getCurrentClusterName() {
      return getClusterInfo().getName();
   }

   public long getRetries() {
      return retryCounter.get();
   }

   public ConsistentHash getConsistentHash(String cacheName) {
      return getCacheInfo(cacheName).getConsistentHash();
   }

   public int getTopologyId(String cacheName) {
      return getCacheInfo(cacheName).getTopologyId();
   }

   public Collection<InetSocketAddress> getServers(String cacheName) {
      long stamp = lock.readLock();
      try {
         return topologyInfo.getServers(cacheName);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public void addCacheTopologyInfoIfAbsent(String cacheName) {
      topologyInfo.getOrCreateCacheInfo(cacheName);
   }
}
