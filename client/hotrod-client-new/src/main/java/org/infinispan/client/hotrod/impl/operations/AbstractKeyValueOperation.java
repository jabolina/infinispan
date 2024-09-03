package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractKeyValueOperation<K, V, R> extends AbstractKeyOperation<K, R> {
   protected final V value;
   protected final long lifespan;

   protected final long maxIdle;

   protected final TimeUnit lifespanTimeUnit;

   protected final TimeUnit maxIdleTimeUnit;
   protected AbstractKeyValueOperation(InternalRemoteCache<?, ?> internalRemoteCache, K key, V value,
                                       long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(internalRemoteCache, key);

      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec, CacheMarshaller marshaller) {
      super.writeOperationRequest(channel, buf, codec, marshaller);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      marshaller.writeValue(buf, value);
   }
}
