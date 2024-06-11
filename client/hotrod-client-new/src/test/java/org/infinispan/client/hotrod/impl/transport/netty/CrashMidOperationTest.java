package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.retry.AbstractRetryTest;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

@CleanupAfterMethod
@Test(testName = "client.hotrod.impl.transport.netty.CrashMidOperationTest", groups = "functional")
public class CrashMidOperationTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      return builder;
   }

   @Override
   protected void amendRemoteCacheManagerConfiguration(org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder) {
      builder.maxRetries(0);
   }

   public void killServerMidOperation() throws Exception {
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());

      CountDownLatch operationLatch = new CountDownLatch(1);
      AtomicReference<Channel> channelRef = new AtomicReference<>();

      NoopRetryingOperation firstOperation = new NoopRetryingOperation(0, channelRef, operationLatch);
      fork(() -> dispatcher.execute(firstOperation));

      eventually(() -> channelRef.get() != null);
      Channel channel = channelRef.get();

      HotRodClientTestingUtil.killServers(hotRodServer1);
      eventually(() -> !channel.isActive());

      eventually(firstOperation::isDone);
      Exceptions.expectExecutionException(TransportException.class, firstOperation);

      // Since the first operation failed midway execution, we don't know if the server has failed or only the channel.
      // The second operation will try to connect and fail, and then update the failed list.
      NoopRetryingOperation secondOperation = new NoopRetryingOperation(1, channelRef, operationLatch);
      dispatcher.execute(secondOperation);
      eventually(secondOperation::isDone);
      Assertions.assertThatThrownBy(() -> secondOperation.get(10, TimeUnit.SECONDS))
            .cause()
            .isInstanceOf(ConnectException.class);

      // We only release the latch now, but notice that all the other operations were able to finish.
      operationLatch.countDown();

      // The failed list was update, the next operation should succeed.
      NoopRetryingOperation thirdOperation = new NoopRetryingOperation(2, channelRef, operationLatch);
      dispatcher.executeOnSingleAddress(thirdOperation, address);
      eventually(thirdOperation::isDone);
      thirdOperation.get(10, TimeUnit.SECONDS);
   }

   static class NoopRetryingOperation extends HotRodOperation<Void> {
      private final AtomicReference<Channel> channelRef;
      private final CountDownLatch firstOp;
      private final int id;

      protected NoopRetryingOperation(int nbr, AtomicReference<Channel> channelRef, CountDownLatch firstOp) {
         this.channelRef = channelRef;
         this.firstOp = firstOp;
         this.id = nbr;
      }

      @Override
      public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
         if (channelRef.compareAndSet(null, channel)) {
            try {
               firstOp.await();
            } catch (InterruptedException e) {
               completeExceptionally(e);
            }
            assert isDone() : "Should be done";
         }
      }

      @Override
      public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
         return null;
      }

      @Override
      public short requestOpCode() {
         return 0;
      }

      @Override
      public short responseOpCode() {
         return 0;
      }

      @Override
      public int flags() {
         return 0;
      }

      @Override
      public byte[] getCacheNameBytes() {
         return new byte[0];
      }

      @Override
      public String getCacheName() {
         return null;
      }

      @Override
      public DataFormat getDataFormat() {
         return null;
      }

      @Override
      public String toString() {
         return "id = " + id;
      }
   }
}
