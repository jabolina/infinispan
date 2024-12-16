package org.infinispan.server.cdc;

import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_CONTAINER_DATABASE_PROPERTIES;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;
import org.infinispan.server.persistence.PersistenceIT;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.persistence.DatabaseServerListener;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite(failIfNoTests = false)
@SelectClasses({
      CDCCacheTestIT.class
})
public class ChangeDataCaptureIT extends InfinispanSuite {

   public static final String CDC_TABLE_NAME = "student";

   private static final Properties properties;

   static {
      properties = new Properties();
      properties.setProperty(INFINISPAN_TEST_CONTAINER_DATABASE_PROPERTIES, "target/test-classes/database/cdc");
      properties.putAll(System.getProperties());
   }

   public static String[] DEFAULT_DATABASES = Arrays.stream(DatabaseVendor.values())
         .map(Enum::name)
         .map(String::toLowerCase)
         .toArray(String[]::new);
   public static final DatabaseServerListener DATABASE_LISTENER = new DatabaseServerListener(properties, DEFAULT_DATABASES);

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config(System.getProperty(ChangeDataCaptureIT.class.getName(), "configuration/ClusteredServerTest.xml"))
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(PersistenceIT.getJdbcDrivers())
               .artifacts(PersistenceIT.getJavaArchive())
               .addListener(DATABASE_LISTENER)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   public static class DatabaseProvider implements ArgumentsProvider {

      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         return Arrays.stream(DATABASE_LISTENER.getDatabaseTypes())
               .map(DATABASE_LISTENER::getDatabase)
               .map(Arguments::of);
      }
   }
}
