package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.SaslConfigurationBuilder;

/**
 * @author José Bolina
 * @since 15.0
 */
public class TransportAuthenticationConfigurationBuilder implements Builder<TransportAuthenticationConfiguration> {

   private final AttributeSet attributes;
   private SaslConfigurationBuilder saslConfigurationBuilder = null;

   public TransportAuthenticationConfigurationBuilder() {
      this.attributes = TransportAuthenticationConfiguration.attributeDefinitionSet();
   }

   @Override
   public TransportAuthenticationConfiguration create() {
      return new TransportAuthenticationConfiguration(
            attributes.protect(),
            saslConfigurationBuilder != null ? saslConfigurationBuilder.create() : null);
   }

   @Override
   public void validate() {
      if (saslConfigurationBuilder != null)
         saslConfigurationBuilder.validate();
   }

   @Override
   public Builder<?> read(TransportAuthenticationConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public SaslConfigurationBuilder saslConfigurationBuilder() {
      return saslConfigurationBuilder = new SaslConfigurationBuilder();
   }
}
