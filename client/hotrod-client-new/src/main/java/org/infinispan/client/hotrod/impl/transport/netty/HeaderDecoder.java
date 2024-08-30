package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_CREATED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_EXPIRED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_MODIFIED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_REMOVED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.COUNTER_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.ByteBufCacheUnmarshaller;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Signal;

public class HeaderDecoder extends HintedReplayingDecoder<HeaderDecoder.State> {
   private static final Log log = LogFactory.getLog(HeaderDecoder.class);
   // used for HeaderOrEventDecoder, too, as the function is similar
   public static final String NAME = "header-decoder";
   private final Configuration configuration;
   private final OperationDispatcher dispatcher;
   // This is a ConcurrentHashMap solely for methods reading incomplete. Writing to the map is done solely
   // in the event loop
   private final Map<Long, HotRodOperation<?>> incomplete = new ConcurrentHashMap<>();
   private final Map<Long, ScheduledFuture<?>> timeouts = new HashMap<>();
   private final List<byte[]> listeners = new ArrayList<>();
   // Marshaller is shared for the entire connection and swaps out DataFormat and ByteBuf per request
   private final ByteBufCacheUnmarshaller unmarshaller;
   private volatile boolean closing;

   // All the following can only be accessed while in the event loop of the channel
   private Channel channel;

   private HotRodOperation<?> operation;
   private short status;
   private long receivedMessageId;
   private short receivedOpCode;

   private long messageOffset;
   private Codec codec;

   public HeaderDecoder(Configuration configuration, OperationDispatcher dispatcher) {
      super(State.READ_MESSAGE_ID);
      this.configuration = configuration;
      this.dispatcher = dispatcher;
      this.unmarshaller = new ByteBufCacheUnmarshaller(configuration.getClassAllowList());
   }

   public Configuration getConfiguration() {
      return configuration;
   }

   public Channel getChannel() {
      assert channel.eventLoop().inEventLoop();
      return channel;
   }

   @Override
   public boolean isSharable() {
      return false;
   }

   public long registerOperation(HotRodOperation<?> operation) {
      log.tracef("Decoder is %s Channel is %s", this, channel);
      assert channel.eventLoop().inEventLoop();

      long messageId = messageOffset++;

      if (log.isTraceEnabled()) {
         log.tracef("Registering operation %s(%08X) with id %d on %s",
               operation, System.identityHashCode(operation), messageId, channel);
      }
      if (closing) {
         throw HOTROD.noMoreOperationsAllowed();
      }
      Long messageIdLong = messageId;
      HotRodOperation<?> prev = incomplete.put(messageIdLong, operation);
      assert prev == null;
      ScheduledFuture<?> future = channel.eventLoop().schedule(() -> {
               timeouts.remove(messageIdLong);
               dispatcher.handleResponse(operation, channel, null,
                     new SocketTimeoutException(this + " timed out after " + configuration.socketTimeout() + " ms"));
            }, configuration.socketTimeout(), TimeUnit.MILLISECONDS);
      timeouts.put(messageIdLong, future);
      return messageId;
   }

   @Override
   public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      super.channelRegistered(ctx);
      channel = ctx.channel();
      log.tracef("Decoder %s has Channel %s registered", this, channel);
      codec = configuration.version().getCodec();
   }

   private HotRodOperation<?> removeOperation(Long messageId) {
      HotRodOperation<?> op = incomplete.remove(messageId);
      ScheduledFuture<?> future = timeouts.remove(messageId);
      if (future != null) {
         future.cancel(false);
      }
      return op;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      try {
         switch (state()) {
            case READ_MESSAGE_ID:
               receivedMessageId = codec.readMessageId(in);
               receivedOpCode = in.readUnsignedByte();
               switch (receivedOpCode) {
                  case CACHE_ENTRY_CREATED_EVENT_RESPONSE:
                  case CACHE_ENTRY_MODIFIED_EVENT_RESPONSE:
                  case CACHE_ENTRY_REMOVED_EVENT_RESPONSE:
                  case CACHE_ENTRY_EXPIRED_EVENT_RESPONSE:
                     operation = receivedMessageId == 0 ? null : incomplete.get(receivedMessageId);
                     // The operation may be null even if the messageId was set: the server does not really wait
                     // until all events are sent, only until these are queued. In such case the operation may
                     // complete earlier.
                     if (operation != null && !(operation instanceof AddClientListenerOperation)) {
                        throw HOTROD.operationIsNotAddClientListener(receivedMessageId, operation.toString());
                     } else if (log.isTraceEnabled()) {
                        log.tracef("Received event for request %d", receivedMessageId, operation);
                     }
                     checkpoint(State.READ_CACHE_EVENT);
                     // the loop in HintedReplayingDecoder will call decode again
                     return;
                  case COUNTER_EVENT_RESPONSE:
                     checkpoint(State.READ_COUNTER_EVENT);
                     // the loop in HintedReplayingDecoder will call decode again
                     return;
               }
               // we can remove the operation at this point since we'll read no more in this state
               operation = removeOperation(receivedMessageId);
               if (operation == null) {
                  throw HOTROD.unknownMessageId(receivedMessageId);
               }
               if (log.isTraceEnabled()) {
                  log.tracef("Received response for request %d, %s", receivedMessageId, operation);
               }
               checkpoint(State.READ_STATUS);
               // fall through
            case READ_STATUS:
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding header for message %s", HotRodConstants.Names.of(receivedOpCode));
               }
               status = in.readUnsignedByte();
               short topologyChangeByte = in.readUnsignedByte();
               if (topologyChangeByte == 1) {
                  checkpoint(State.READ_TOPOLOGY);
                  return;
               }
               checkpoint(State.READ_PAYLOAD);
               // fall through
            case READ_PAYLOAD:
               // Now that all headers values have been read, check the error responses.
               // This avoids situations where an exceptional return ends up with
               // the socket containing data from previous request responses.
               if (receivedOpCode != operation.responseOpCode()) {
                  if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
                     codec.checkForErrorsInResponseStatus(in, operation.getCacheName(), receivedMessageId, status, channel.remoteAddress());
                  }
                  throw HOTROD.invalidResponse(operation.getCacheName(), operation.responseOpCode(), receivedOpCode);
               }
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding payload for message %s", HotRodConstants.Names.of(receivedOpCode));
               }
               try {
                  unmarshaller.setDataFormat(operation.getDataFormat());
                  Object resp = operation.createResponse(in, status, this, codec, unmarshaller);
                  dispatcher.handleResponse((HotRodOperation<Object>) operation, ctx.channel(), resp, null);
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  dispatcher.handleResponse(operation, ctx.channel(), null, t);
               }
               checkpoint(State.READ_MESSAGE_ID);
               break;
            case READ_CACHE_EVENT:
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding cache event %s", HotRodConstants.Names.of(receivedOpCode));
               }
               AbstractClientEvent cacheEvent;
               try {
                  cacheEvent = codec.readCacheEvent(in, receivedMessageId, dispatcher.getClientListenerNotifier()::getCacheDataFormat,
                        receivedOpCode, configuration.getClassAllowList(), ctx.channel().remoteAddress());
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  log.unableToReadEventFromServer(t, ctx.channel().remoteAddress());
                  throw t;
               }
               invokeEvent(cacheEvent.getListenerId(), cacheEvent);
               checkpoint(State.READ_MESSAGE_ID);
               break;
            case READ_COUNTER_EVENT:
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding counter event %s", HotRodConstants.Names.of(receivedOpCode));
               }
               HotRodCounterEvent counterEvent;
               try {
                  counterEvent = codec.readCounterEvent(in);
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  HOTROD.unableToReadEventFromServer(t, ctx.channel().remoteAddress());
                  throw t;
               }
               invokeEvent(counterEvent.getListenerId(), counterEvent);
               checkpoint(State.READ_MESSAGE_ID);
               break;
            case READ_TOPOLOGY:
               readNewTopologyAndHash(in, operation.getCacheName());
               checkpoint(State.READ_PAYLOAD);
               break;
         }
      } catch (Exception e) {
         // If this is server error make sure to restart the state of decoder
         checkpoint(State.READ_MESSAGE_ID);
         throw e;
      }
   }

   private void readNewTopologyAndHash(ByteBuf buf, String cacheName) {
      int newTopologyId = ByteBufUtil.readVInt(buf);

      InetSocketAddress[] addresses = readTopology(buf);

      final short hashFunctionVersion;
      final SocketAddress[][] segmentOwners;
      if (dispatcher.getClientIntelligence().getValue() == ClientIntelligence.HASH_DISTRIBUTION_AWARE.getValue()) {
         // Only read the hash if we asked for it
         hashFunctionVersion = buf.readUnsignedByte();
         int numSegments = ByteBufUtil.readVInt(buf);
         segmentOwners = new SocketAddress[numSegments][];
         if (hashFunctionVersion > 0) {
            for (int i = 0; i < numSegments; i++) {
               short numOwners = buf.readUnsignedByte();
               segmentOwners[i] = new SocketAddress[numOwners];
               for (int j = 0; j < numOwners; j++) {
                  int memberIndex = ByteBufUtil.readVInt(buf);
                  segmentOwners[i][j] = addresses[memberIndex];
               }
            }
         }
      } else {
         hashFunctionVersion = -1;
         segmentOwners = null;
      }

      dispatcher.updateTopology(cacheName, operation, newTopologyId,
            addresses, segmentOwners, hashFunctionVersion);
   }

   private InetSocketAddress[] readTopology(ByteBuf buf) {
      int clusterSize = ByteBufUtil.readVInt(buf);
      InetSocketAddress[] addresses = new InetSocketAddress[clusterSize];
      for (int i = 0; i < clusterSize; i++) {
         String host = ByteBufUtil.readString(buf);
         int port = buf.readUnsignedShort();
         addresses[i] = InetSocketAddress.createUnresolved(host, port);
      }
      return addresses;
   }

   public void setCodec(Codec codec) {
      assert channel.eventLoop().inEventLoop();
      if (configuration.version() == ProtocolVersion.PROTOCOL_VERSION_AUTO) {
         this.codec = codec;
         channel.attr(OperationChannel.OPERATION_CHANNEL_ATTRIBUTE_KEY).get().setCodec(codec);
      }
   }

   private void invokeEvent(byte[] listenerId, Object cacheEvent) {
      try {
         dispatcher.getClientListenerNotifier().invokeEvent(listenerId, cacheEvent);
      } catch (Exception e) {
         HOTROD.unexpectedErrorConsumingEvent(cacheEvent, e);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.fatal("Exception encountered!" + cause);
      if (operation != null) {
         dispatcher.handleResponse(operation, ctx.channel(), null, cause);
      } else {
         TransportException transportException = log.errorFromUnknownOperation(ctx.channel(), cause, ctx.channel().remoteAddress());
         for (HotRodOperation<?> op : incomplete.values()) {
            try {
               dispatcher.handleResponse(operation, ctx.channel(), null, transportException);
            } catch (Throwable t) {
               HOTROD.errorf(t, "Failed to complete %s", op);
            }
         }
         if (log.isTraceEnabled()) {
            log.tracef(cause, "Requesting %s close due to exception", ctx.channel());
         }
         ctx.close();
      }
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      close();
      super.channelInactive(ctx);
   }

   void failoverClientListeners() {
      for (byte[] listenerId : listeners) {
         dispatcher.getClientListenerNotifier().failoverClientListener(listenerId);
      }
   }

   public void close() {
      TransportException transportException = log.connectionClosed(channel.remoteAddress(), channel.remoteAddress());
      for (HotRodOperation<?> op : incomplete.values()) {
         try {
            dispatcher.handleResponse(op, channel, null, transportException);
         } catch (Throwable t) {
            HOTROD.errorf(t, "Failed to complete %s", op);
         }
      }
      failoverClientListeners();
   }

   public CompletableFuture<Void> allCompleteFuture() {
      return CompletableFuture.allOf(incomplete.values().toArray(new CompletableFuture[0]));
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt instanceof ChannelPoolCloseEvent) {
         closing = true;
         allCompleteFuture().whenComplete((nil, throwable) -> ctx.channel().close());
      } else if (evt instanceof IdleStateEvent) {
         // If we have incomplete operations this channel is not idle!
         if (!incomplete.isEmpty()) {
            return;
         }
      }
      ctx.fireUserEventTriggered(evt);
   }

   /**
    * {@inheritDoc}
    *
    * Checkpoint is exposed for implementations of {@link HotRodOperation}
    */
   @Override
   public void checkpoint() {
      super.checkpoint();
   }

   public Map<Long, HotRodOperation<?>> registeredOperationsById() {
      return incomplete;
   }

   public void addListener(byte[] listenerId) {
      if (log.isTraceEnabled()) {
         log.tracef("Decoder %08X adding listener %s", hashCode(), Util.printArray(listenerId));
      }
      listeners.add(listenerId);
   }

   // must be called from event loop thread!
   public void removeListener(byte[] listenerId) {
      boolean removed = listeners.removeIf(id -> Arrays.equals(id, listenerId));
      if (log.isTraceEnabled()) {
         log.tracef("Decoder %08X removed? %s listener %s", hashCode(), Boolean.toString(removed), Util.printArray(listenerId));
      }
   }

   public int registeredOperations() {
      return incomplete.size();
   }

   enum State {
      READ_MESSAGE_ID,
      READ_STATUS,
      READ_TOPOLOGY,
      READ_PAYLOAD,
      READ_CACHE_EVENT, READ_COUNTER_EVENT,
   }
}
