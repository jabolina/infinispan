package org.infinispan.server.core.security.sasl.jgroups;

import org.infinispan.server.core.configuration.SaslConfiguration;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;

public interface SaslContext {

   SaslAuthenticator saslAuthenticator();

   SaslConfiguration configuration();
}
