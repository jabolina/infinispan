package org.infinispan.client.hotrod.impl.operations;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

public class ByteBufCacheMarshaller implements CacheMarshaller {

   private final ByteBufOutputStream outputStream;
   private DataFormat format;

   public ByteBufCacheMarshaller() {
      this.outputStream = new ByteBufOutputStream();
   }

   public void setDataFormat(DataFormat format) {
      this.format = format;
   }

   @Override
   public <K> void writeKey(ByteBuf buf, K key) {
      outputStream.setBuffer(buf);
      format.keyToStream(key, outputStream);
   }

   @Override
   public <V> void writeValue(ByteBuf buf, V value) {
      outputStream.setBuffer(buf);
      format.valueToStream(value, outputStream);
   }

   private static final class ByteBufOutputStream extends OutputStream {

      private ByteBuf buffer;

      private void setBuffer(ByteBuf buffer) {
         this.buffer = buffer;
      }

      @Override
      public void write(int b) throws IOException {
         if (!buffer.isWritable())
            throw new IOException("Buffer not available to write");

         buffer.writeByte(b & 0xFF);
      }

      @Override
      public void write(byte[] b) throws IOException {
         write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
         Objects.requireNonNull(b, "Array is null");

         if (off < 0 || len < 0 || (off + len) > buffer.writableBytes())
            throw new IndexOutOfBoundsException("No space available on buffer");

         if (!buffer.isWritable())
            throw new IOException("Buffer not available to write");

         ByteBufUtil.writeVInt(buffer, len - off);
         buffer.writeBytes(b, off, len);
      }
   }
}
