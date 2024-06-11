package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.commons.configuration.ClassAllowList;

import io.netty.buffer.ByteBuf;

public class ByteBufCacheUnmarshaller implements CacheUnmarshaller {
   private final ClassAllowList allowList;

   private final ByteBufSwappableInputStream inputStream;

   private DataFormat format;

   public ByteBufCacheUnmarshaller(ClassAllowList allowList) {
      this.allowList = allowList;
      this.inputStream = new ByteBufSwappableInputStream();
   }

   public void setDataFormat(DataFormat dataFormat) {
      this.format = dataFormat;
   }

   @Override
   public <E> E readKey(ByteBuf buf) {
      int length = ByteBufUtil.readVInt(buf);
      return readKey(buf, length);
   }

   @Override
   public <E> E readKey(ByteBuf buf, int length) {
      if (buf.readableBytes() < length) {
         // Not enough bytes to read object so just skip to force it to throw SIGNAL - this assumes we are using a
         // ReplayingDecoder
         buf.skipBytes(length);
      }
      inputStream.setBuffer(buf, length);
      return (E) format.keyToObject(inputStream, allowList);
   }

   @Override
   public <E> E readValue(ByteBuf buf) {
      int length = ByteBufUtil.readVInt(buf);
      return readValue(buf, length);
   }

   @Override
   public <E> E readValue(ByteBuf buf, int length) {
      if (buf.readableBytes() < length) {
         // Not enough bytes to read object so just skip to force it to throw SIGNAL - this assumes we are using a
         // ReplayingDecoder
         buf.skipBytes(length);
      }
      inputStream.setBuffer(buf, length);
      return (E) format.valueToObj(inputStream, allowList);
   }
}
