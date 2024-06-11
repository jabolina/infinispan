package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class NoopChannelOperation extends HotRodOperation<Channel> {
   @Override
   public Channel createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return decoder.getChannel();
   }

   @Override
   public short requestOpCode() {
      return 0;
   }

   @Override
   public short responseOpCode() {
      return 0;
   }

   @Override
   public int flags() {
      return 0;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return HotRodConstants.DEFAULT_CACHE_NAME_BYTES;
   }

   @Override
   public String getCacheName() {
      return HotRodConstants.DEFAULT_CACHE_NAME;
   }

   @Override
   public DataFormat getDataFormat() {
      return null;
   }
}
