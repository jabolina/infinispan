package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Remove client listener operation. In order to avoid issues with concurrent
 * event consumption, removing client listener operation is sent in a separate
 * connection to the one used for event consumption, but it must go to the
 * same node where the listener has been added.
 *
 * @author William Burns
 */
public class RemoveClientListenerOperation extends AbstractCacheOperation<Void> {

   private final ClientListenerNotifier listenerNotifier;
   private final byte[] listenerId;

   protected RemoveClientListenerOperation(InternalRemoteCache<?, ?> internalRemoteCache,
                                           ClientListenerNotifier listenerNotifier, byte[] listenerId) {
      super(internalRemoteCache);
      this.listenerNotifier = listenerNotifier;
      this.listenerId = listenerId;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec, CacheMarshaller marshaller) {
      ByteBufUtil.writeArray(buf, listenerId);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isSuccess(status) || HotRodConstants.isNotExecuted(status)) {
         listenerNotifier.removeClientListener(listenerId);
      }
      return null;
   }

   @Override
   public short requestOpCode() {
      return REMOVE_CLIENT_LISTENER_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_CLIENT_LISTENER_RESPONSE;
   }
}
