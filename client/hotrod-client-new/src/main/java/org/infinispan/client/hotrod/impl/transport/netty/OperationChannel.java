package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;
import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

public class OperationChannel extends CompletableFuture<Void> implements MessagePassingQueue.Consumer<HotRodOperation<?>> {
   private static final Log log = LogFactory.getLog(OperationChannel.class);

   public static final AttributeKey<OperationChannel> OPERATION_CHANNEL_ATTRIBUTE_KEY = AttributeKey.newInstance("hotrod-operation");

   private final Runnable SEND_OPERATIONS = this::sendOperations;
   private final SocketAddress address;
   private final ChannelInitializer newChannelInvoker;
   // TODO: should we use AtomicFieldUpdate with Boolean?
   private final AtomicBoolean attemptedConnect = new AtomicBoolean();

   private final Function<String, ClientTopology> currentCacheTopologyFunction;
   private final BiConsumer<OperationChannel, Throwable> connectionFailureListener;
   // Unfortunately MessagePassingQueue doesn't implement Queue so we use the concrete class
   private final MpscUnboundedArrayQueue<HotRodOperation<?>> queue = new MpscUnboundedArrayQueue<>(128);

   // Volatile as operations can be subitted outside of the event loop (channel only written in event loop once)
   private volatile Channel channel;

   // All variable after this can ONLY be read/written while in the event loop

   // Here to signal that non authenticated operations can now proceed
   private boolean acceptingRequests;
   // Will be initialized to configured default, but it is possible it is overridden during the negotiation process
   Codec codec;
   HeaderDecoder headerDecoder;
   ByteBuf buffer;

   OperationChannel(SocketAddress address, ChannelInitializer channelInitializer,
                    Function<String, ClientTopology> currentCacheTopologyFunction, BiConsumer<OperationChannel, Throwable> connectionFailureListener) {
      this.address = address;
      this.newChannelInvoker = channelInitializer;
      this.currentCacheTopologyFunction = currentCacheTopologyFunction;
      this.connectionFailureListener = connectionFailureListener;
   }

   public static OperationChannel createAndStart(SocketAddress address, ChannelInitializer newChannelInvoker,
                                                  Function<String, ClientTopology> currentCacheTopologyFunction,
                                                  BiConsumer<OperationChannel, Throwable> connectionFailureListener) {
      OperationChannel operationChannel = new OperationChannel(address, newChannelInvoker, currentCacheTopologyFunction, connectionFailureListener);
      operationChannel.attemptConnect();
      return operationChannel;
   }

   void attemptConnect() {
      if (attemptedConnect.getAndSet(true)) {
         return;
      }
      ChannelFuture channelFuture = newChannelInvoker.createChannel();
      channelFuture.addListener(f -> {
         if (f.isSuccess()) {
            Channel c = channelFuture.channel();
            assert c.eventLoop().inEventLoop();
            c.attr(OPERATION_CHANNEL_ATTRIBUTE_KEY).set(this);
            connectionFailureListener.accept(this, null);
            channel = c;
            headerDecoder = c.pipeline().get(HeaderDecoder.class);
            codec = headerDecoder.getConfiguration().version().getCodec();
            log.tracef("OperationChannel %s connect complete to %s", this, channel);
            complete(null);
         } else {
            TransportException cause = new TransportException(f.cause(), address);
            // HeaderDecoder should handle already sent operations
            // TODO: need to send requests up so they can be retried!
//            queue.drain(callback -> callback.cancel(address, cause));
            connectionFailureListener.accept(this, cause);
            completeExceptionally(cause);
         }

      });
   }

   public void setCodec(Codec codec) {
      assert channel.eventLoop().inEventLoop();
      this.codec = codec;
   }

   public void markAcceptingRequests() {
      channel.eventLoop().submit(() -> {
         acceptingRequests = true;
         // We can't send requests unless the stage is completed as well.
            sendOperations();
      });
   }

   HotRodOperation<?> forceSendOperation(HotRodOperation<?> operation) {
      if (channel.eventLoop().inEventLoop()) {
         actualForceSingleOperation(operation);
      } else {
         channel.eventLoop().submit(() -> actualForceSingleOperation(operation));
      }
      return operation;
   }

   private void actualForceSingleOperation(HotRodOperation<?> operation) {
      assert channel.eventLoop().inEventLoop();
      long messageId = headerDecoder.registerOperation(operation);
      ByteBuf buffer = channel.alloc().buffer();
      codec.writeHeader(buffer, messageId, currentCacheTopologyFunction.apply(operation.getCacheName()), operation);
      operation.writeOperationRequest(channel, buffer, codec);
      channel.writeAndFlush(buffer, channel.voidPromise());
   }

   public void sendOperation(HotRodOperation<?> operation) {
      if (operation.forceSend()) {
         forceSendOperation(operation);
         return;
      }
      queue.offer(operation);
      Channel channel = this.channel;
      // TODO: maybe implement a way to only submit if required
      if (channel != null) {
         channel.eventLoop().submit(SEND_OPERATIONS);
      } else {
         attemptConnect();
      }
   }

   private void sendOperations() {
      assert channel.eventLoop().inEventLoop();
      if (queue.isEmpty()) {
         return;
      }

      if (log.isTraceEnabled()) {
         log.tracef("OperationChannel %s Sending commands: %s enqueue to send to channel %s", this, queue.size(), channel);
      }

      // We only send up to 256 commands a time
      queue.drain(this, 256);
      if (buffer != null) {
         log.tracef("Flushing commands to channel %s", channel);
         channel.writeAndFlush(buffer, channel.voidPromise());
         buffer = null;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Queue size after: %s", queue.size());
      }
      // If Queue wasn't empty try to send again, but note this is sent on eventLoop so other operations can
      // barge in between our calls
      if (!queue.isEmpty()) {
         log.tracef("Resubmitting as more operations in queue after sending");
         channel.eventLoop().submit(SEND_OPERATIONS);
      }
   }

   public SocketAddress getAddress() {
      return address;
   }

   // TODO: look into close later!
//   public void close() {
//      TransportException cause = new TransportException("Pool was closing", address);
//      // HeaderDecoder should handle already sent operations
//      queue.drain(callback -> {
//         // Let HotRodOperations try to be retried
//         if (callback instanceof HotRodOperation) {
//            ((HotRodOperation<?>) callback).exceptionCaught(channel, cause);
//         } else {
//            callback.cancel(address, cause);
//         }
//      });
//      if (channel != null) {
//         // We don't want to fail all operations on given channel,
//         // e.g. when moving from unresolved to resolved addresses
//         channel.pipeline().fireUserEventTriggered(ChannelPoolCloseEvent.INSTANCE);
//      }
//   }

   @Override
   public void accept(HotRodOperation<?> operation) {
      try {
         if (buffer == null) {
            buffer = channel.alloc().buffer();
         }
         long messageId = headerDecoder.registerOperation(operation);
         codec.writeHeader(buffer, messageId, currentCacheTopologyFunction.apply(operation.getCacheName()), operation);
         operation.writeOperationRequest(channel, buffer, codec);
      } catch (Throwable t) {
         operation.completeExceptionally(t);
      }
   }

   public Queue<HotRodOperation<?>> pendingChannelOperations() {
      return queue;
   }

   public void close() {
      headerDecoder.close();
   }

   public Channel getChannel() {
      return channel;
   }
}
