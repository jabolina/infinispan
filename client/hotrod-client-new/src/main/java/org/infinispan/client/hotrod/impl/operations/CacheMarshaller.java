package org.infinispan.client.hotrod.impl.operations;

import io.netty.buffer.ByteBuf;

public interface CacheMarshaller {

   <K> void writeKey(ByteBuf buf, K key);

   <V> void writeValue(ByteBuf buf, V value);
}
