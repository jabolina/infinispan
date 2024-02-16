package org.infinispan.server.functional.resp.interop;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.TestClass;

import io.vertx.ext.unit.junit.VertxUnitRunner;

public class VertxRespNestRunner extends VertxUnitRunner {

   public VertxRespNestRunner(Class<?> klass) throws InitializationError {
      super(klass);
   }

   @Override
   public void run(RunNotifier notifier) {
      try {
         FrameworkMethod method = null;
         // We are running in a nested environment.
         // This run needs to break out to the host and retrieve all the information.
         if (isRunningInsideSuite()) {
            method = VertxRespRunner.retrieveExtensionMethod(getTestClass());
         } else {
            TestClass host = new TestClass(getTestClass().getJavaClass().getNestHost());
            method = VertxRespRunner.retrieveExtensionMethod(host);
         }

         if (method != null) {
            // Now we hijack the nested execution utilizing the host's info.
            VertxRespRunner.hijackMovingTrain(method);
            VertxRespRunner.stopOnExit(true);
         } else {
            VertxRespRunner.stopOnExit(false);
         }

         super.run(notifier);
      } catch (Throwable t) {
         notifier.fireTestFailure(new Failure(Description.TEST_MECHANISM, t));
      }
   }

   private static boolean isRunningInsideSuite() {
      AtomicBoolean hasSuite = new AtomicBoolean(false);
      StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
      walker.forEach(frame -> {
         if (frame.getDeclaringClass() == VertxRespRunner.class) {
            hasSuite.set(true);
         }
      });
      return hasSuite.get();
   }
}
