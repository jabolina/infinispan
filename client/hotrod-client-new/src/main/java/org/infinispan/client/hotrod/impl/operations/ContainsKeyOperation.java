package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * Implements "containsKey" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ContainsKeyOperation<K> extends AbstractKeyOperation<K, Boolean> {

   public ContainsKeyOperation(InternalRemoteCache<?, ?> remoteCache, K key) {
      super(remoteCache, key);
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return !HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status);
   }

   @Override
   public short requestOpCode() {
      return CONTAINS_KEY_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return CONTAINS_KEY_RESPONSE;
   }
}
