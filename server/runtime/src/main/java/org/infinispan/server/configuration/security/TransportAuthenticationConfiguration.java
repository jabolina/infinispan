package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.core.configuration.SaslConfiguration;

/**
 * @author José Bolina
 * @since 15.0
 */
public class TransportAuthenticationConfiguration extends ConfigurationElement<TransportAuthenticationConfiguration> {

   private final SaslConfiguration saslConfiguration;

   public TransportAuthenticationConfiguration(AttributeSet attributes,
                                               SaslConfiguration saslConfiguration) {
      super(Element.TRANSPORT_AUTHENTICATION, attributes);
      this.saslConfiguration = saslConfiguration;
   }

   public SaslConfiguration saslConfiguration() {
      return saslConfiguration;
   }

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TransportAuthenticationConfiguration.class);
   }
}
