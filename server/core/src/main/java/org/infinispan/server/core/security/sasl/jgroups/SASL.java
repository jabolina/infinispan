package org.infinispan.server.core.security.sasl.jgroups;

import java.util.function.Supplier;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

@MBean(description = "Provides SASL authentication")
public class SASL extends Protocol {

   private final Supplier<SASLContext> initializationContext;

   public SASL(Supplier<SASLContext> initializationContext) {
      this.initializationContext = initializationContext;
   }

   @Override
   public void init() throws Exception {
      super.init();
      System.out.println("Invoke SASL init");
   }

   @Override
   public void start() throws Exception {
      super.start();
      System.out.println("Invoke SASL start");
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
}
