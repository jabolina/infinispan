package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

public class GetOperation<K, R> extends AbstractKeyOperation<K, R> {
   public GetOperation(InternalRemoteCache<?, ?> internalRemoteCache, K key) {
      super(internalRemoteCache, key);
   }

   @Override
   public R createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status)) {
         return unmarshaller.readValue(buf);
      }
      return null;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.GET_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.GET_RESPONSE;
   }
}
