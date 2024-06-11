package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.DataFormat;

public abstract class AbstractNoCacheHotRodOperation<E> extends HotRodOperation<E> {
   @Override
   public String getCacheName() {
      return null;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return null;
   }

   @Override
   public int flags() {
      return 0;
   }

   @Override
   public Object getRoutingObject() {
      return null;
   }

   @Override
   public boolean supportRetry() {
      // Operations not tied to a cache shouldn't be retried normally
      return false;
   }

   @Override
   public DataFormat getDataFormat() {
      return null;
   }
}
