package org.infinispan.server.functional.resp.interop;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.RunnerBuilder;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.TestDescription;
import org.testcontainers.lifecycle.TestLifecycleAware;

import io.vertx.redis.harness.RespServerRule;
import io.vertx.redis.harness.RespServerRuleBuilder;

public class VertxRespRunner extends Suite {

   private static final AtomicBoolean shutdown = new AtomicBoolean(false);

   @Retention(RetentionPolicy.RUNTIME)
   @Target(ElementType.METHOD)
   public @interface GlueExtensionAndRule {
      int server() default 0;
   }

   private final FrameworkMethod method;

   public VertxRespRunner(Class<?> klass, RunnerBuilder builder) throws InitializationError {
      super(klass, builder);
      this.method = retrieveExtensionMethod(getTestClass());

      if (method == null)
         throw new InitializationError("No public static method to retrieve JUnit5 extension");
   }

   public VertxRespRunner(RunnerBuilder builder, Class<?>[] classes) throws InitializationError {
      super(builder, classes);
      this.method = retrieveExtensionMethod(getTestClass());

      if (method == null)
         throw new InitializationError("No public static method to retrieve JUnit5 extension");
   }

   static FrameworkMethod retrieveExtensionMethod(TestClass testClass) throws InitializationError {
      List<FrameworkMethod> methods = testClass
            .getAnnotatedMethods(GlueExtensionAndRule.class);

      for (FrameworkMethod method : methods) {
         if (method.isStatic() && method.isPublic())
            return method;
      }

      return null;
   }

   public static void stopOnExit(boolean value) {
      shutdown.set(value);
   }

   @Override
   public void run(RunNotifier notifier) {
      try {
         hijackMovingTrain(method);
         super.run(notifier);
      } catch (Throwable t) {
         notifier.fireTestFailure(new Failure(Description.TEST_MECHANISM, t));
      }
   }

   static void hijackMovingTrain(FrameworkMethod method) throws Throwable {
      Object extension = method.invokeExplosively(null);
      if (!(extension instanceof InfinispanServerExtension servers))
         throw new IllegalStateException("Extension is not Infinispan servers");

      GlueExtensionAndRule glue = method.getAnnotation(GlueExtensionAndRule.class);

      if (!servers.getTestServer().isDriverInitialized())
         servers.getTestServer().initServerDriver();

      RespServerRule rule = new VertxRespServerRule(servers.getServerDriver(), glue);

      Properties properties = System.getProperties();
      properties.put(RespServerRuleBuilder.CONTAINER_BUILD_OVERRIDE_PROPERTY, rule);
      System.setProperties(properties);
   }

   private static class VertxRespServerRule implements RespServerRule {

      private final ContainerInfinispanServerDriver driver;
      private final GlueExtensionAndRule configuration;
      private GenericContainer<?> delegate;

      private VertxRespServerRule(InfinispanServerDriver driver, GlueExtensionAndRule configuration) {
         if (!(driver instanceof ContainerInfinispanServerDriver ci)) {
            throw new IllegalStateException("Not utilizing container mode");
         }

         this.driver = ci;
         this.configuration = configuration;
         this.delegate = null;
      }

      @Override
      public String getHost() {
         return delegate.getHost();
      }

      @Override
      public Integer getFirstMappedPort() {
         return delegate.getFirstMappedPort();
      }

      @Override
      public GenericContainer<?> get() {
         return delegate;
      }

      @Override
      public Statement apply(Statement base, Description description) {
         return new Statement() {
            @Override
            public void evaluate() throws Throwable {
               List<Throwable> errors = new ArrayList<>();
               String name = sanitize(description.getDisplayName());

               try {
                  if (!driver.isRunning(configuration.server())) {
                     driver.prepare(name);
                     driver.start(name);
                  }

                  delegate = driver.container(configuration.server());

                  if (delegate instanceof TestLifecycleAware tlc) {
                     tlc.beforeTest(toDescription(description));
                  }

                  delegate.start();

                  base.evaluate();

                  if (delegate instanceof TestLifecycleAware tlc) {
                     tlc.afterTest(toDescription(description), Optional.empty());
                  }
               } catch (Throwable e) {
                  errors.add(e);
                  if (delegate instanceof TestLifecycleAware tlc) {
                     tlc.afterTest(toDescription(description), Optional.of(e));
                  }
               } finally {
                  if (shutdown.get()) {
                     driver.stop(name);
                  }
               }

               MultipleFailureException.assertEmpty(errors);
            }
         };
      }

      private String sanitize(String name) {
         return name.split("\\$")[0];
      }

      private TestDescription toDescription(Description description) {
         return new TestDescription() {
            @Override
            public String getTestId() {
               return description.getDisplayName();
            }

            @Override
            public String getFilesystemFriendlyName() {
               return description.getClassName() + "-" + description.getMethodName();
            }
         };
      }
   }
}
