package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.configuration.endpoint.EndpointsConfigurationBuilder;
import org.infinispan.server.configuration.security.RealmConfiguration;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.configuration.security.SecurityConfigurationBuilder;
import org.infinispan.server.configuration.security.ServerTransportConfigurationBuilder;
import org.infinispan.server.configuration.security.TransportAuthenticationConfiguration;
import org.infinispan.server.core.configuration.SaslConfiguration;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.infinispan.server.core.security.sasl.jgroups.SaslContext;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public class ServerConfigurationBuilder implements Builder<ServerConfiguration> {
   private final Properties properties = new Properties();
   private final InterfacesConfigurationBuilder interfaces = new InterfacesConfigurationBuilder();
   private final SocketBindingsConfigurationBuilder socketBindings = new SocketBindingsConfigurationBuilder(this);
   private final SecurityConfigurationBuilder security = new SecurityConfigurationBuilder(this);
   private final DataSourcesConfigurationBuilder dataSources = new DataSourcesConfigurationBuilder();
   private final EndpointsConfigurationBuilder endpoints = new EndpointsConfigurationBuilder(this);
   private final ServerTransportConfigurationBuilder transport = new ServerTransportConfigurationBuilder();
   private final List<ConfigurableSupplier<?>> suppliers = new ArrayList<>();
   private final GlobalConfigurationBuilder builder;

   public ServerConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   public ServerConfigurationBuilder properties(Properties properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
   }

   public Properties properties() {
      return properties;
   }

   public SecurityConfigurationBuilder security() {
      return security;
   }

   public InterfacesConfigurationBuilder interfaces() {
      return interfaces;
   }

   public SocketBindingsConfigurationBuilder socketBindings() {
      return socketBindings;
   }

   public DataSourcesConfigurationBuilder dataSources() {
      return dataSources;
   }

   public EndpointsConfigurationBuilder endpoints() {
      return endpoints;
   }

   public ServerTransportConfigurationBuilder transport() {
      return transport;
   }

   @Override
   public void validate() {
      Arrays.asList(interfaces, socketBindings, security, endpoints, transport).forEach(Builder::validate);
   }

   @Override
   public ServerConfiguration create() {
      SecurityConfiguration securityConfiguration = security.create();
      InterfacesConfiguration interfacesConfiguration = interfaces.create();
      SocketBindingsConfiguration bindingsConfiguration = socketBindings.create(interfacesConfiguration);
      ServerConfiguration configuration = new ServerConfiguration(
            interfacesConfiguration,
            bindingsConfiguration,
            securityConfiguration,
            dataSources.create(),
            endpoints.create(builder, bindingsConfiguration, securityConfiguration),
            transport.create()
      );

      for(ConfigurableSupplier<?> supplier : suppliers) {
         supplier.setConfiguration(configuration);
      }

      return configuration;
   }

   @Override
   public Builder<?> read(ServerConfiguration template, Combine combine) {
      // Do nothing
      return this;
   }

   public Supplier<SSLContext> serverSSLContextSupplier(String sslContextName) {
      SSLContextSupplier supplier = new SSLContextSupplier(sslContextName, false);
      suppliers.add(supplier);
      return supplier;
   }

   public Supplier<SSLContext> clientSSLContextSupplier(String sslContextName) {
      SSLContextSupplier supplier = new SSLContextSupplier(sslContextName, true);
      suppliers.add(supplier);
      return supplier;
   }

   public Supplier<SaslContext> saslContextSupplier(String realmName) {
      SASLContextSupplier supplier = new SASLContextSupplier(realmName);
      suppliers.add(supplier);
      return supplier;
   }

   private static abstract class ConfigurableSupplier<T> implements Supplier<T> {
      private ServerConfiguration configuration;

      protected void setConfiguration(ServerConfiguration configuration) {
         this.configuration = configuration;
      }

      protected ServerConfiguration serverConfiguration() {
         return configuration;
      }

      protected SecurityConfiguration securityConfiguration() {
         if (configuration == null) return null;

         return configuration.security();
      }
   }

   private static class SSLContextSupplier extends ConfigurableSupplier<SSLContext> {
      final String name;
      final boolean client;

      SSLContextSupplier(String name, boolean client) {
         this.name = name;
         this.client = client;
      }

      @Override
      public SSLContext get() {
         return client
               ? securityConfiguration().realms().getRealm(name).clientSSLContext()
               : securityConfiguration().realms().getRealm(name).serverSSLContext();
      }
   }

   private static class SASLContextSupplier extends ConfigurableSupplier<SaslContext> {
      private final String realmName;

      private SASLContextSupplier(String realmName) {
         this.realmName = realmName;
      }

      @Override
      public SaslContext get() {
         SecurityConfiguration security = securityConfiguration();
         RealmConfiguration realmConfiguration = security.realms().getRealm(realmName);
         TransportAuthenticationConfiguration configuration = realmConfiguration.transportAuthenticationConfiguration();
         if (configuration == null) return null;

         return new SaslContext() {
            @Override
            public SaslAuthenticator saslAuthenticator() {
               return configuration.saslConfiguration().authenticator();
            }

            @Override
            public SaslConfiguration configuration() {
               return configuration.saslConfiguration();
            }
         };
      }
   }
}
