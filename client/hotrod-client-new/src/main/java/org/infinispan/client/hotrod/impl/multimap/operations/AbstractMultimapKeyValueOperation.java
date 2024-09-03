package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyValueOperation;
import org.infinispan.client.hotrod.impl.operations.CacheMarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractMultimapKeyValueOperation<K, V, R> extends AbstractKeyValueOperation<K, V, R> {

    protected final boolean supportsDuplicates;

    protected AbstractMultimapKeyValueOperation(InternalRemoteCache<?, ?> remoteCache, K key, V value,
                                                long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit,
                                                boolean supportsDuplicates) {
        super(remoteCache, key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
        this.supportsDuplicates = supportsDuplicates;
    }

    @Override
    public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec, CacheMarshaller marshaller) {
        super.writeOperationRequest(channel, buf, codec, marshaller);
        codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
    }
}
