package org.infinispan.client.hotrod.telemetry.impl;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.impl.operations.DelegatingHotRodOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;

public class TelemetryOperation<E> extends DelegatingHotRodOperation<E> {
   protected TelemetryOperation(HotRodOperation<E> delegate) {
      super(delegate);
   }

   @Override
   public Map<String, byte[]> additionalParameters() {
      Map<String, byte[]> mapToUse = super.additionalParameters();
      if (mapToUse == null) {
         mapToUse = new HashMap<>();
      }
      W3CTraceContextPropagator.getInstance()
            .inject(Context.current(), mapToUse,
            (carrier, paramKey, paramValue) ->
                  carrier.put(paramKey, paramValue.getBytes(StandardCharsets.UTF_8)));
      return mapToUse;
   }
}
