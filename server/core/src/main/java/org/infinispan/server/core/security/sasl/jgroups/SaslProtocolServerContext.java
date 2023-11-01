package org.infinispan.server.core.security.sasl.jgroups;

import static org.infinispan.server.core.security.sasl.jgroups.SASL.SASL_PROTOCOL_NAME;

import java.security.Principal;
import java.util.List;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.server.core.configuration.SaslConfiguration;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;

public class SaslProtocolServerContext implements SaslProtocolContext {

   private final SaslServer server;

   public SaslProtocolServerContext(SaslAuthenticator authenticator, SaslConfiguration configuration, String mechanism,
                                    List<Principal> principals) throws SaslException {
      this.server = authenticator.createSaslServer(configuration, principals, mechanism, SASL_PROTOCOL_NAME);
   }

   @Override
   public boolean isSuccessful() {
      return server.isComplete();
   }

   @Override
   public boolean needsWrapping() {
      if (server.isComplete()) {
         String qop = (String) server.getNegotiatedProperty(Sasl.QOP);
         return qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf"));
      }

      return false;
   }

   @Override
   public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
      return server.wrap(outgoing, offset, len);
   }

   @Override
   public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
      return server.unwrap(incoming, offset, len);
   }

   @Override
   public void dispose() {
      try {
         server.dispose();
      } catch (SaslException ignore) { }
   }

   @Override
   public Message next(Address address, SaslHeader header) throws SaslException {
      byte[] challenge = server.evaluateResponse(header.getPayload());
      if (challenge == null) return null;

      Message message = new EmptyMessage(address).setFlag(Message.Flag.OOB);
      return message.putHeader(SASL.SASL_ID, new SaslHeader(SaslHeader.Type.CHALLENGE, challenge));
   }
}
