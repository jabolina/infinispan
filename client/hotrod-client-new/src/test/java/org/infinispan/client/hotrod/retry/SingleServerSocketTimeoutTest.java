package org.infinispan.client.hotrod.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.test.NoopChannelOperation;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@Test(groups = "functional", testName = "client.hotrod.retry.SingleServerSocketTimeoutTest")
public class SingleServerSocketTimeoutTest extends SocketTimeoutFailureRetryTest {

   {
      // This reproduces the case when an operation times out but there is only a single server.
      // The operation is then registered again on the same channel and it succeeds.
      nbrOfServers = 1;
   }

   public void testChannelIsReUtilizedForRetry() throws Exception {
      Integer key = 2;
      remoteCache.put(key, "v1");
      assertEquals("v1", remoteCache.get(key));

      AdvancedCache<?, ?> nextCache = cacheToHit(key);
      DelayingInterceptor interceptor = TestingUtil.extractInterceptorChain(nextCache)
            .findInterceptorExtending(DelayingInterceptor.class);
      CompletableFuture<Void> delay = new CompletableFuture<>();
      interceptor.delayNextRequest(delay);

      assertEquals(0, remoteCacheManager.getOperationDispatcher().getRetries());
      assertEquals("v1", remoteCache.get(key));
      assertEquals(1, remoteCacheManager.getOperationDispatcher().getRetries());

      Channel initialChannel = remoteCacheManager.getOperationDispatcher()
            .executeOnSingleAddress(new NoopChannelOperation(), getAddress(hotRodServer1))
            .toCompletableFuture().get(10, TimeUnit.SECONDS);


      assertThat(initialChannel.isActive()).isTrue();
      delay.complete(null);

      assertEquals("v1", remoteCache.get(key));
      assertEquals(1, remoteCacheManager.getOperationDispatcher().getRetries());

      Channel other = remoteCacheManager.getOperationDispatcher()
            .executeOnSingleAddress(new NoopChannelOperation(), getAddress(hotRodServer1))
            .toCompletableFuture().get(10, TimeUnit.SECONDS);

      assertThat(other).isSameAs(initialChannel);
   }
}
