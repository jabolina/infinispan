package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "removeIfUnmodified" operation as defined by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class RemoveIfUnmodifiedOperation<K, V> extends AbstractKeyOperation<K, VersionedOperationResponse<V>> {

   private final long version;

   public RemoveIfUnmodifiedOperation(InternalRemoteCache<?, ?> remoteCache, K key, long version) {
      super(remoteCache, key);
      this.version = version;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec, CacheMarshaller marshaller) {
      super.writeOperationRequest(channel, buf, codec, marshaller);
      buf.writeLong(version);
   }

   @Override
   public VersionedOperationResponse<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return returnVersionedOperationResponse(buf, status, codec, unmarshaller);
   }

   @Override
   public short requestOpCode() {
      return REMOVE_IF_UNMODIFIED_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_IF_UNMODIFIED_RESPONSE;
   }
}
