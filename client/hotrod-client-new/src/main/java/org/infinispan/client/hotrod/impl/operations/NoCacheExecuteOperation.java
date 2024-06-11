package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

public class NoCacheExecuteOperation extends AbstractNoCacheHotRodOperation<String> {
   private final String taskName;
   private final Map<String, byte[]> marshalledParams;

   public NoCacheExecuteOperation(String taskName, Map<String, byte[]> marshalledParams) {
      this.taskName = taskName;
      this.marshalledParams = marshalledParams;
   }

   @Override
   public String createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.EXEC_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.EXEC_RESPONSE;
   }
}
