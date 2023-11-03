package org.infinispan.server.core.security.sasl.jgroups;

import javax.security.sasl.SaslException;

import org.jgroups.Address;
import org.jgroups.Message;

interface SaslProtocolHandler {

   boolean isSuccessful();

   boolean needsWrapping();

   byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException;

   byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException;

   void dispose();

   Message next(Address address, SaslHeader header) throws SaslException;
}
