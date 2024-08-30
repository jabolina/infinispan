package org.infinispan.client.hotrod.counter.operation;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AbstractCacheOperation;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterManager;

import io.netty.buffer.ByteBuf;

/**
 * A counter operation for {@link CounterManager#getCounterNames()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetCounterNamesOperation extends AbstractCacheOperation<Collection<String>> {
   private int size;
   private Collection<String> names;

   public GetCounterNamesOperation(InternalRemoteCache<?, ?> remoteCache) {
      super(remoteCache);
   }

   @Override
   public void reset() {
      names = null;
   }

   @Override
   public Collection<String> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      assert status == NO_ERROR_STATUS;
      if (names == null) {
         size = ByteBufUtil.readVInt(buf);
         names = new ArrayList<>(size);
         decoder.checkpoint();
      }
      while (names.size() < size) {
         names.add(ByteBufUtil.readString(buf));
         decoder.checkpoint();
      }
      return names;
   }

   @Override
   public short requestOpCode() {
      return COUNTER_GET_NAMES_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_GET_NAMES_RESPONSE;
   }
}
