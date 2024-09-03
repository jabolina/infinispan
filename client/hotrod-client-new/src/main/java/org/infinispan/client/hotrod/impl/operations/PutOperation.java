package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import net.jcip.annotations.Immutable;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutOperation<K, V> extends AbstractKeyValueOperation<K, V, V> {
   public PutOperation(InternalRemoteCache<?, ?> cache, K key, V value, long lifespan,
                       TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(cache, key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   public V createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isSuccess(status)) {
         // TODO: how should we do stats?
//         statsDataStore();
         if (HotRodConstants.hasPrevious(status)) {
//            statsDataRead(true);
         }
         return returnPossiblePrevValue(buf, status, codec, unmarshaller);
      } else {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
   }

   @Override
   public short requestOpCode() {
      return PUT_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return PUT_RESPONSE;
   }
}
