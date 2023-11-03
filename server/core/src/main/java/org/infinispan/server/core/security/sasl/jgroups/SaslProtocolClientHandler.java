package org.infinispan.server.core.security.sasl.jgroups;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;

public class SaslProtocolClientHandler implements SaslProtocolHandler {

   private static final byte[] EMPTY_CHALLENGE = new byte[0];
   private final SaslClient client;
   private final Subject subject;

   public SaslProtocolClientHandler(SaslContext context) {
      this.subject = context.configuration().serverSubject();
      this.client = context.saslAuthenticator().createSaslClient();
   }

   @Override
   public boolean isSuccessful() {
      return client.isComplete();
   }

   @Override
   public boolean needsWrapping() {
      if (client.isComplete()) {
         String qop = (String) client.getNegotiatedProperty(Sasl.QOP);
         return qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf"));
      }
      return false;
   }

   @Override
   public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
      return client.wrap(outgoing, offset, len);
   }

   @Override
   public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
      return client.unwrap(incoming, offset, len);
   }

   @Override
   public void dispose() {
      try {
         client.dispose();
      } catch (SaslException ignore) { }
   }

   @Override
   public Message next(Address address, SaslHeader header) throws SaslException {
      return addHeader(address, header != null ? header.getPayload() : null);
   }

   private Message addHeader(Address address, byte[] payload) throws SaslException {
      byte[] response;

      if (payload == null) {
         response = client.hasInitialResponse()
               ? evaluateChallenge(EMPTY_CHALLENGE)
               : EMPTY_CHALLENGE;
      } else {
         response = evaluateChallenge(payload);
      }

      if (response == null) return null;

      Message msg = new EmptyMessage(address).setFlag(Message.Flag.OOB);
      return msg.putHeader(SASL.SASL_ID, new SaslHeader(SaslHeader.Type.RESPONSE, response));
   }

   private byte[] evaluateChallenge(byte[] challenge) throws SaslException {
      if (subject != null) {
         try {
            return Subject.doAs(subject, (PrivilegedExceptionAction<byte[]>) () -> client.evaluateChallenge(challenge));
         } catch (PrivilegedActionException e) {
            throw (SaslException) e.getCause();
         }
      }

      return client.evaluateChallenge(challenge);
   }
}
