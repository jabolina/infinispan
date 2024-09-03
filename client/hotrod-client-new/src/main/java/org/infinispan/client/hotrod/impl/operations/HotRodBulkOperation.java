package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;

/**
 * An HotRod operation that span across multiple remote nodes concurrently (like getAll / putAll).
 *
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public abstract class HotRodBulkOperation<V, O extends HotRodOperation<V>> extends AbstractCacheOperation<V> {


   protected HotRodBulkOperation(InternalRemoteCache<?, ?> internalRemoteCache) {
      super(internalRemoteCache);
   }

   /**
    * Split the operation to target the correct servers.
    *
    * <p>
    * A single bulk operation touches multiple keys. Therefore, it is necessary to segment the operation's key space
    * in multiple smaller operations, each targeting the correct server who owns the keys. The mapper function
    * identifies which server owns a key.
    * </p>
    *
    * @param mapper A function that receives a key and identify the server who owns it.
    * @return A map of the segmented operations. Each entry is the sub-operation and the key is the target server.
    */
   public abstract Map<SocketAddress, O> operations(Function<Object, SocketAddress> mapper);

   /**
    * Reduces all the sub-operation's responses to a single object.
    *
    * <p>
    * After the operation is segmented and redirect to each specific server, this method is responsible for putting
    * the multiple responses together into a single object and finishing the operation.
    * </p>
    *
    * <b>Warning:</b> implementors <b>must</b> call the {@link #complete(Object)} method after the reduce process.
    *
    * @param responses A collection with the responses from all targeted servers.
    */
   public abstract void complete(Collection<V> responses);
}
