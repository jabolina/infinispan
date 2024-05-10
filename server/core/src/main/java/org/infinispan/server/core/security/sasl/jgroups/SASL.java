package org.infinispan.server.core.security.sasl.jgroups;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.security.sasl.SaslException;

import org.infinispan.server.core.security.InetAddressPrincipal;
import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.EmptyMessage;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

@MBean(description = "Provides SASL authentication")
public class SASL extends Protocol {

   private static final short GMS_ID = ClassConfigurator.getProtocolId(GMS.class);
   static final short SASL_ID = ClassConfigurator.getProtocolId(SASL.class);

   static final String SASL_PROTOCOL_NAME = "jgroups";

   private final Supplier<SaslContext> initializationContext;
   private final Map<Address, SaslProtocolHandler> handlers = new HashMap<>();

   private SaslContext context;
   private List<Principal> principals;


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
      cleanup();
   }

   @Override
   public void destroy() {
      super.destroy();
      cleanup();
   }

   private void cleanup() {
      handlers.values().forEach(SaslProtocolHandler::dispose);
      handlers.clear();
   }

   @Override
   public Object up(Message msg) {
      if (!isAuthenticationEnabled()) return super.up(msg);

      SaslHeader saslHeader = msg.getHeader(SASL_ID);
      GMS.GmsHeader gmsHeader = msg.getHeader(GMS_ID);
      Address src = msg.getSrc();

      if (needsAuthentication(gmsHeader, src)) {
         if (saslHeader == null)
            throw new IllegalStateException("Found GMS join or merge request but no SASL header");

         // In any case, the server will issue the challenge and we can return.
         if (!serverChallenge(gmsHeader, saslHeader, msg))
            return null;
      } else if (saslHeader != null) {
         SaslProtocolHandler handler = handlers.get(src);
         if (handler == null)
            throw new IllegalStateException("Cannot find server context to challenge SASL request from: " + src);

         log.trace("%s: received %s from %s", local_addr, saslHeader.getType(), src);

         try {
            Message res = handler.next(src, saslHeader);
            if (res != null) {
               if (log.isTraceEnabled()) {
                  SaslHeader.Type resType = saslHeader.getType() == SaslHeader.Type.CHALLENGE
                        ? SaslHeader.Type.RESPONSE
                        : SaslHeader.Type.CHALLENGE;
                  log.trace("%s: sending %s to %s", local_addr, resType, src);
               }

               down_prot.down(res);
               return null;
            }

            if (!handler.isSuccessful()) {
               String s = String.format("computed %s is null but challenge-response cycle not complete!", saslHeader.getType());
               throw new SaslException(s);
            }

            log.trace("%s: finished challenge-response cycle for %s", local_addr, src);
         } catch (SaslException e) {
            dispose(src);
            log.warn(local_addr + ": failed vo validate CHALLENGED from " + src, e);
         }

         return null;
      }
      return super.up(msg);
   }

   @Override
   public void up(MessageBatch batch) {
      Iterator<Message> it = batch.iterator();
      while (it.hasNext()) {
         Message msg = it.next();
         GMS.GmsHeader gmsHeader = msg.getHeader(GMS_ID);
         Address src = msg.getSrc();
         if (needsAuthentication(gmsHeader, src)) {
            SaslHeader saslHeader = msg.getHeader(SASL_ID);
            if (saslHeader == null) {
               log.warn("Found GMS join or merge request but no SASL header");
               sendRejectionMessage(gmsHeader.getType(), batch.sender(), "join or merge without an SASL header");
               it.remove();
            } else if (!serverChallenge(gmsHeader, saslHeader, msg))
               it.remove(); // don't pass up
         }
      }

      if (!batch.isEmpty())
         up_prot.up(batch);
   }

   private void dispose(Address address) {
      SaslProtocolHandler handler = handlers.remove(address);
      if (handler != null)
         handler.dispose();
   }

   private boolean serverChallenge(GMS.GmsHeader gmsHeader, SaslHeader saslHeader, Message msg) {
      switch (gmsHeader.getType()) {
         case GMS.GmsHeader.JOIN_REQ:
         case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
         case GMS.GmsHeader.MERGE_REQ:
            Address src = msg.getSrc();
            SaslProtocolHandler handler = null;

            try {
               handler = new SaslProtocolServerHandler(context, getPrincipals());
               handlers.put(src, handler);

               log.trace("%s: send first CHALLENGE to %s", local_addr, src);
               this.getDownProtocol().down(handler.next(src, saslHeader));
            } catch (SaslException e) {
               log.warn("failed to validate SaslHeader from %s, header: %s", msg.getSrc(), saslHeader);
               sendRejectionMessage(gmsHeader.getType(), src, "authentication failed");
            }

            // Now waits for completion.
            return false;
         default:
            return true;
      }
   }

   @Override
   public Object down(Message msg) {
      GMS.GmsHeader hdr = msg.getHeader(GMS_ID);
      Address dest = msg.getDest();
      if (needsAuthentication(hdr, dest)) {
         // We are the client needing to authenticate on remote.
         SaslProtocolHandler handler = null;

         try {
            // FIXME: Need to handle the client side.
            handler = new SaslProtocolClientHandler(context);
         } finally {

         }
      }
      return super.down(msg);
   }

   private void sendRejectionMessage(byte type, Address dest, String error_msg) {
      switch (type) {
         case GMS.GmsHeader.JOIN_REQ:
         case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            sendJoinRejectionMessage(dest, error_msg);
            break;
         case GMS.GmsHeader.MERGE_REQ:
            sendMergeRejectionMessage(dest);
            break;
         default:
            log.error("type " + type + " unknown");
            break;
      }
   }

   private void sendJoinRejectionMessage(Address dest, String error_msg) {
      if (dest == null)
         return;

      JoinRsp joinRes = new JoinRsp(error_msg); // specify the error message on the JoinRsp
      Message msg = new BytesMessage(dest)
            .putHeader(GMS_ID, new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP))
            .setArray(GMS.marshal(joinRes));
      down_prot.down(msg);
   }

   private void sendMergeRejectionMessage(Address dest) {
      Message msg = new EmptyMessage(dest).setFlag(Message.Flag.OOB);
      GMS.GmsHeader hdr = new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
      hdr.setMergeRejected(true);
      msg.putHeader(GMS_ID, hdr);
      if (log.isDebugEnabled())
         log.debug("merge response=" + hdr);
      down_prot.down(msg);
   }

   private boolean needsAuthentication(GMS.GmsHeader hdr, Address remoteAddress) {
      if (hdr != null) {
         switch (hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
               return true;
            case GMS.GmsHeader.MERGE_REQ:
               return !isSelf(remoteAddress);
            default:
               return false;
         }
      } else {
         return false;
      }
   }

   private boolean isSelf(Address remoteAddress) {
      return remoteAddress.equals(local_addr);
   }

   private boolean isAuthenticationEnabled() {
      return context != null;
   }

   private List<Principal> getPrincipals() {
      if (principals != null) return principals;

      principals = new ArrayList<>();
      Address addr = (org.jgroups.Address) down(new Event(Event.GET_PHYSICAL_ADDRESS, getAddress()));
      if (addr instanceof IpAddress) {
         IpAddress ipAddress = (IpAddress) addr;
         principals.add(new InetAddressPrincipal(ipAddress.getIpAddress()));
      }

      return principals;
   }
}
