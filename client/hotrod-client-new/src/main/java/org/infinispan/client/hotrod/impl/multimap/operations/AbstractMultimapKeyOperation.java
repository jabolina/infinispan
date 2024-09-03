package org.infinispan.client.hotrod.impl.multimap.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.client.hotrod.impl.operations.CacheMarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractMultimapKeyOperation<K, R> extends AbstractKeyOperation<K, R> {

    protected final boolean supportsDuplicates;

    public AbstractMultimapKeyOperation(InternalRemoteCache<?, ?> remoteCache, K key, boolean supportsDuplicates) {
        super(remoteCache, key);
        this.supportsDuplicates = supportsDuplicates;
    }

    @Override
    public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec, CacheMarshaller marshaller) {
        super.writeOperationRequest(channel, buf, codec, marshaller);
        codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
    }
}
