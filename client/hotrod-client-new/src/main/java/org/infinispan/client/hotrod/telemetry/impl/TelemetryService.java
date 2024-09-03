package org.infinispan.client.hotrod.telemetry.impl;

import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;

public interface TelemetryService {

   <K, V> Function<RemoteCache<K, V>, CacheOperationsFactory> wrapWithTelemetry(Function<RemoteCache<K, V>, CacheOperationsFactory> function);

}
