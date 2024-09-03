package org.infinispan.client.hotrod.impl.operations;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;

/**
 * Corresponds to getWithMetadata operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class GetWithMetadataOperation<V> extends AbstractKeyOperation<MetadataValue<V>> implements RetryAwareCompletionStage<MetadataValue<V>> {

   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);

   private final SocketAddress preferredServer;

   private volatile Boolean retried;

   public GetWithMetadataOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, SocketAddress preferredServer) {
      super(remoteCache, keyBytes);
      // We should always be passing resolved addresses here to confirm it matches
      assert !(preferredServer instanceof InetSocketAddress) || !((InetSocketAddress) preferredServer).isUnresolved();
      this.preferredServer = preferredServer;
   }

   @Override
   public MetadataValue<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (preferredServer != null) {
         retried = preferredServer.equals(decoder.getChannel().remoteAddress());
      }
      MetadataValue<V> metadataValue = readMetadataValue(buf, status, unmarshaller);
      // TODO: need to do stats
//      statsDataRead(metadataValue != null);
      return metadataValue;
   }

   public static <V> MetadataValue<V> readMetadataValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isNotExist(status) || (!HotRodConstants.isSuccess(status) && !HotRodConstants.hasPrevious(status))) {
         return null;
      }
      short flags = buf.readUnsignedByte();
      long creation = -1;
      int lifespan = -1;
      long lastUsed = -1;
      int maxIdle = -1;
      if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
         creation = buf.readLong();
         lifespan = ByteBufUtil.readVInt(buf);
      }
      if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
         lastUsed = buf.readLong();
         maxIdle = ByteBufUtil.readVInt(buf);
      }
      long version = buf.readLong();
      if (log.isTraceEnabled()) {
         log.tracef("Received version: %d", version);
      }
      V value = unmarshaller.readValue(buf);

      return new MetadataValueImpl<>(creation, lifespan, lastUsed, maxIdle, version, value);
   }

   @Override
   public Boolean wasRetried() {
      return retried;
   }

   @Override
   public short requestOpCode() {
      return GET_WITH_METADATA;
   }

   @Override
   public short responseOpCode() {
      return GET_WITH_METADATA_RESPONSE;
   }
}
