package org.infinispan.server.core.security.sasl.jgroups;

import java.util.function.Supplier;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

@MBean(description = "Provides SASL authentication")
public class SASL extends Protocol {

   static final short SASL_ID = ClassConfigurator.getProtocolId(SASL.class);

   static final String SASL_PROTOCOL_NAME = "jgroups";

   private final Supplier<SaslContext> initializationContext;

   private SaslContext context;


   public SASL(Supplier<SaslContext> initializationContext) {
      this.initializationContext = initializationContext;
   }

   @Override
   public void start() throws Exception {
      super.start();
      context = initializationContext.get();
   }

   @Override
   public void stop() {
      super.stop();
      System.out.println("Invoke SASL stop");
   }

   @Override
   public void destroy() {
      super.destroy();
      System.out.println("Invoke sasl destroy");
   }

   @Override
   public Object up(Message msg) {
      System.out.println("Invoke SASL up message");
      return super.up(msg);
   }

   @Override
   public void up(MessageBatch batch) {
      System.out.println("Invoke SASL up batch");
      super.up(batch);
   }

   @Override
   public Object down(Message msg) {
      System.out.println("Invoke SASL down msg");
      return super.down(msg);
   }

   private boolean isAuthenticationEnabled() {
      return context != null;
   }
}
