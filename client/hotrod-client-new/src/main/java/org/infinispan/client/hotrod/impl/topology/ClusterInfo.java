package org.infinispan.client.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;

/**
 * Cluster definition
 *
 * @author Dan Berindei
 */
public class ClusterInfo {
   private final String clusterName;
   private final List<InetSocketAddress> servers;
   private final ClientIntelligence intelligence;
   private final String sniHostName;

   public ClusterInfo(String clusterName, List<InetSocketAddress> servers, ClientIntelligence intelligence, String sniHostName) {
      this.clusterName = clusterName;
      this.servers = List.copyOf(servers);
      this.intelligence = Objects.requireNonNull(intelligence);
      this.sniHostName = sniHostName;
   }

   public String getName() {
      return clusterName;
   }

   public List<InetSocketAddress> getInitialServers() {
      return servers;
   }

   public ClientIntelligence getIntelligence() {
      return intelligence;
   }

   public String getSniHostName() {
      return sniHostName;
   }

   @Override
   public String toString() {
      return "ClusterInfo{" +
            "name='" + clusterName + '\'' +
            ", servers=" + servers +
            ", intelligence=" + intelligence +
            ", sniHostname=" + sniHostName +
            '}';
   }
}
