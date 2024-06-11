package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;

public abstract class ClientListenerOperation extends AbstractCacheOperation<SocketAddress> {
   public final Object listener;
   public final byte[] listenerId;

   protected ClientListenerOperation(InternalRemoteCache<?, ?> internalRemoteCache, Object listener) {
      super(internalRemoteCache);
      this.listener = listener;
      this.listenerId = generateListenerId();
   }

   protected static byte[] generateListenerId() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      byte[] listenerId = new byte[16];
      ByteBuffer bb = ByteBuffer.wrap(listenerId);
      bb.putLong(random.nextLong());
      bb.putLong(random.nextLong());
      return listenerId;
   }

   protected ClientListener extractClientListener() {
      ClientListener l = ReflectionUtil.getAnnotation(listener.getClass(), ClientListener.class);
      if (l == null)
         throw HOTROD.missingClientListenerAnnotation(listener.getClass().getName());
      return l;
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append("listenerId=").append(Util.printArray(listenerId));
   }

   public abstract ClientListenerOperation copy();
   // TODO: need to implement
}
