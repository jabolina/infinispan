package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * TODO: need to implement parallel commands for dispatcher!!
 */
public class PutAllParallelOperation extends AbstractCacheOperation<Void> {
   protected PutAllParallelOperation(InternalRemoteCache<?, ?> internalRemoteCache) {
      super(internalRemoteCache);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return PUT_ALL_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return PUT_ALL_RESPONSE;
   }
}
