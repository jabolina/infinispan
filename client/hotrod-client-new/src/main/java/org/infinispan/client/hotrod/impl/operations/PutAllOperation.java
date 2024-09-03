package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class PutAllOperation<K, V> extends HotRodBulkOperation<Void, PutAllOperation<K, V>> {
   protected final Map<? extends K, ? extends V> map;
   protected final long lifespan;
   private final TimeUnit lifespanTimeUnit;
   protected final long maxIdle;
   private final TimeUnit maxIdleTimeUnit;

   public PutAllOperation(InternalRemoteCache<?, ?> remoteCache, Map<? extends K, ? extends V> map,
                          long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(remoteCache);
      this.map = map;
      this.lifespan = lifespan;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdle = maxIdle;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec, CacheMarshaller marshaller) {
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeVInt(buf, map.size());
      for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
         marshaller.writeKey(buf, entry.getKey());
         marshaller.writeValue(buf, entry.getValue());
      }
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isSuccess(status)) {
         // TODO: need to do stats
//         statsDataStore(map.size());
         return null;
      }
      throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
   }

   @Override
   public short requestOpCode() {
      return PUT_ALL_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return PUT_ALL_RESPONSE;
   }

   @Override
   public Map<SocketAddress, PutAllOperation<K, V>> operations(Function<Object, SocketAddress> mapper) {
      Map<SocketAddress, Map<K, V>> split = new HashMap<>();

      for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
         SocketAddress target = mapper.apply(entry.getKey());
         Map<K, V> segment = split.computeIfAbsent(target, ignore -> new HashMap<>());
         segment.put(entry.getKey(), entry.getValue());
      }

      return split.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> newInstance(e.getValue())));
   }

   private PutAllOperation<K, V> newInstance(Map<K, V> content) {
      return new PutAllOperation<>(internalRemoteCache, content, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   public void complete(Collection<Void> responses) {
      complete((Void) null);
   }
}
