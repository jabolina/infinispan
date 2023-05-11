package org.infinispan.server.resp.commands.connection;

import io.netty.channel.ChannelHandlerContext;

import org.infinispan.security.Security;
import org.infinispan.server.resp.CacheRespRequestHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

/**
 * @link https://redis.io/commands/auth/
 * @since 14.0
 */
public class AUTH extends RespCommand implements AuthResp3Command {
   public AUTH() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      CompletionStage<Subject> successStage = handler.performAuth(ctx, arguments.get(0), arguments.get(1));

      return handler.stageToReturn(successStage, ctx, subject -> {
         if (subject == null) return handler;
         return createAfterAuthentication(subject, handler);
      });
   }

   static RespRequestHandler createAfterAuthentication(Subject subject, Resp3AuthHandler prev) {
      return Security.doAs(subject, () -> {
         CacheRespRequestHandler next = prev.respServer().newHandler();
         next.setCache(prev.cache());
         return next;
      });
   }
}
