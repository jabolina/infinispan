package org.infinispan.client.hotrod.telemetry.impl;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;

public class TelemetryServiceImpl implements TelemetryService {

   private final W3CTraceContextPropagator propagator;

   public TelemetryServiceImpl() {
      propagator = W3CTraceContextPropagator.getInstance();
   }

   // TODO: need to fix this
//   public void injectSpanContext(HeaderParams header) {
//      // Inject the request with the *current* Context, which contains client current Span if exists.
//      propagator.inject(Context.current(), header,
//            (carrier, paramKey, paramValue) ->
//                  carrier.otherParam(paramKey, paramValue.getBytes(StandardCharsets.UTF_8))
//      );
//   }
}
