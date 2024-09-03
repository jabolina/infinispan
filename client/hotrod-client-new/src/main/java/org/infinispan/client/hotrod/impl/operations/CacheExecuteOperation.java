package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

public class CacheExecuteOperation<E> extends AbstractCacheOperation<E> {
   private final String taskName;
   private final Map<String, byte[]> marshalledParams;
   private final byte[] key;

   public CacheExecuteOperation(InternalRemoteCache<?, ?> internalRemoteCache, String taskName,
                                Map<String, byte[]> marshalledParams, byte[] key) {
      super(internalRemoteCache);
      this.taskName = taskName;
      this.marshalledParams = marshalledParams;
      this.key = key;
   }

   @Override
   public E createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.EXEC_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.EXEC_RESPONSE;
   }

   @Override
   public Object getRoutingObject() {
      return key;
   }
}
