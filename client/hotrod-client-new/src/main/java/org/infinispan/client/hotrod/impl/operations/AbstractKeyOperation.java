package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractKeyOperation<K, R> extends AbstractCacheOperation<R> {
   protected final K key;

   protected AbstractKeyOperation(InternalRemoteCache<?, ?> internalRemoteCache, K key) {
      super(internalRemoteCache);
      this.key = key;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec, CacheMarshaller marshaller) {
      marshaller.writeKey(buf, key);
   }

   @Override
   public Object getRoutingObject() {
      return key;
   }

   protected <T> T returnPossiblePrevValue(ByteBuf buf, short status, Codec codec, CacheUnmarshaller unmarshaller) {
      return (T) codec.returnPossiblePrevValue(buf, status, unmarshaller);
   }

   protected <E> VersionedOperationResponse<E> returnVersionedOperationResponse(ByteBuf buf, short status, Codec codec,
                                                                         CacheUnmarshaller unmarshaller) {
      VersionedOperationResponse.RspCode code;
      if (HotRodConstants.isSuccess(status)) {
         code = VersionedOperationResponse.RspCode.SUCCESS;
      } else if (HotRodConstants.isNotExecuted(status)) {
         code = VersionedOperationResponse.RspCode.MODIFIED_KEY;
      } else if (HotRodConstants.isNotExist(status)) {
         code = VersionedOperationResponse.RspCode.NO_SUCH_KEY;
      } else {
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(status));
      }
      Object prevValue = returnPossiblePrevValue(buf, status, codec, unmarshaller);
      return new VersionedOperationResponse<E>((E) prevValue, code);
   }
}
