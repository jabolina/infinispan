package org.infinispan.client.hotrod.telemetry.impl;

import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;

public class NoOpTelemetryService implements TelemetryService {
   public static NoOpTelemetryService INSTANCE = new NoOpTelemetryService();

   private NoOpTelemetryService() { }

   @Override
   public <K, V> Function<RemoteCache<K, V>, CacheOperationsFactory> wrapWithTelemetry(
         Function<RemoteCache<K, V>, CacheOperationsFactory> function) {
      return function;
   }
}
