package org.infinispan.client.hotrod.telemetry.impl;

import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;

public class TelemetryServiceImpl implements TelemetryService {
   public static TelemetryServiceImpl INSTANCE = new TelemetryServiceImpl();
   private TelemetryServiceImpl() { }
   @Override
   public <K, V> Function<RemoteCache<K, V>, CacheOperationsFactory> wrapWithTelemetry(
         Function<RemoteCache<K, V>, CacheOperationsFactory> function) {
      return rc -> new TelemetryCacheOperationsFactory(function.apply(rc));
   }
}
