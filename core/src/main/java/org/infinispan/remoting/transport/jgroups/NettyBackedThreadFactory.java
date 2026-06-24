package org.infinispan.remoting.transport.jgroups;

final class NettyBackedThreadFactory implements org.jgroups.util.ThreadFactory {

   private final java.util.concurrent.ThreadFactory delegate;
   private boolean includeClusterName;
   private String clusterName;
   private String address;

   NettyBackedThreadFactory(java.util.concurrent.ThreadFactory delegate) {
      this.delegate = delegate;
   }

   @Override
   public Thread newThread(Runnable r, String name) {
      Thread t = delegate.newThread(r);
      t.setName(name);
      return t;
   }

   @Override
   public boolean useVirtualThreads() {
      return true;
   }

   @Override
   public void setPattern(String pattern) {
      if (pattern != null)
         includeClusterName = pattern.contains("c");
   }

   @Override
   public void setIncludeClusterName(boolean includeClusterName) {
      this.includeClusterName = includeClusterName;
   }

   @Override
   public void setClusterName(String channelName) {
      this.clusterName = channelName;
   }

   @Override
   public void setAddress(String address) {
      this.address = address;
   }

   @Override
   public void renameThread(String base_name, Thread thread) {
      StringBuilder sb = new StringBuilder(base_name);
      sb.append('-').append("???");
      if (includeClusterName && clusterName != null)
         sb.append(",").append(clusterName);

      if (address != null)
         sb.append(",").append(address);

      thread.setName(sb.toString());
   }

   @Override
   public Thread newThread(Runnable r) {
      return delegate.newThread(r);
   }
}
