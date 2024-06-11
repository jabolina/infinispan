package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler that is added to the end of pipeline during channel creation and handshake.
 * Its task is to complete {@link ChannelRecord}.
 */
@Sharable
class ActivationHandler extends ChannelInboundHandlerAdapter {
   static final String NAME = "activation-handler";
   private static final Log log = LogFactory.getLog(ActivationHandler.class);
   static final ActivationHandler INSTANCE = new ActivationHandler();

   @Override
   public void channelActive(ChannelHandlerContext ctx) {
      if (log.isTraceEnabled()) {
         log.tracef("Activating channel %s", ctx.channel());
      }
      ctx.pipeline().remove(this);
      ctx.channel()
            .attr(OperationChannel.OPERATION_CHANNEL_ATTRIBUTE_KEY)
            .get()
            .markAcceptingRequests();
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      Channel channel = ctx.channel();
      if (log.isTraceEnabled()) {
         log.tracef(cause, "Failed to activate channel %s", channel);
      }
      try {
         ctx.close();
      } finally {
         // TODO: notify of failure
      }
   }
}
