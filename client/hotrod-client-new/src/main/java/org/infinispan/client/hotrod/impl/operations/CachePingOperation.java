package org.infinispan.client.hotrod.impl.operations;

import java.nio.charset.StandardCharsets;

import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;

public class CachePingOperation extends NoCachePingOperation implements RetryingOperation {
   private final String cacheName;

   public CachePingOperation(String cacheName) {
      this.cacheName = cacheName;
   }


   @Override
   public short requestOpCode() {
      return HotRodConstants.PING_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.PING_RESPONSE;
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return cacheName.getBytes(StandardCharsets.UTF_8);
   }

   @Override
   public boolean forceSend() {
      return true;
   }

   @Override
   public boolean supportRetry() {
      // This is actually a cache operation, but we reuse the no cache logic
      return true;
   }
}
