package org.infinispan.server.functional.resp;

import static org.infinispan.server.functional.ClusteredIT.artifacts;
import static org.infinispan.server.functional.ClusteredIT.mavenArtifacts;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_EXPOSE_PORT;

import org.infinispan.server.functional.resp.interop.VertxRespNestRunner;
import org.infinispan.server.functional.resp.interop.VertxRespRunner;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import io.vertx.ext.unit.TestContext;

@RunWith(VertxRespRunner.class)
@Suite.SuiteClasses({
      VertxRespClientTest.RedisClientTest.class,
      VertxRespClientTest.RedisClient7Test.class,
      VertxRespClientTest.RedisClient6Test.class,
      VertxRespClientTest.RedisClient6SecureTest.class,
      VertxRespClientTest.RedisReconnectTest.class,
})
public class VertxRespClientTest {


   @VertxRespRunner.GlueExtensionAndRule
   public static InfinispanServerExtension getServers() {
      // Expose the default port in the container.
      System.setProperty(INFINISPAN_TEST_SERVER_EXPOSE_PORT, "11222");

      return InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
            .numServers(1)
            .runMode(ServerRunMode.CONTAINER)
            .mavenArtifacts(mavenArtifacts())
            .artifacts(artifacts())
            .property("infinispan.query.lucene.max-boolean-clauses", "1025")
            .build();
   }

   @RunWith(VertxRespNestRunner.class)
   public static class RedisClientTest extends io.vertx.test.redis.RedisClientTest {

      @Ignore @Test
      @Override
      public void testBrpoplpush(TestContext should) {
         super.testBrpoplpush(should);
      }

      @Ignore @Test
      @Override
      public void testEvalsha(TestContext should) {
         super.testEvalsha(should);
      }

      @Ignore @Test
      @Override
      public void testScriptload(TestContext should) {
         super.testScriptload(should);
      }

      @Ignore @Test
      @Override
      public void testEval(TestContext should) {
         super.testEval(should);
      }

      @Ignore @Test
      @Override
      public void testMove(TestContext should) {
         super.testMove(should);
      }

      @Ignore @Test
      @Override
      public void testSave(TestContext should) {
         super.testSave(should);
      }

      @Ignore @Test
      @Override
      public void testSync(TestContext should) {
         super.testSync(should);
      }

      @Ignore @Test
      @Override
      public void testPexpire(TestContext should) {
         super.testPexpire(should);
      }

      @Ignore @Test
      @Override
      public void testLastsave(TestContext should) {
         super.testLastsave(should);
      }

      @Ignore @Test
      @Override
      public void testBgrewriteaof(TestContext should) {
         super.testBgrewriteaof(should);
      }

      @Ignore @Test
      @Override
      public void testRandomkey(TestContext should) {
         super.testRandomkey(should);
      }

      @Ignore @Test
      @Override
      public void testConfigGet(TestContext should) {
         super.testConfigGet(should);
      }

      @Ignore @Test
      @Override
      public void testEvalshaNumKeysAndValuesDifferent(TestContext should) {
         super.testEvalshaNumKeysAndValuesDifferent(should);
      }

      @Ignore @Test
      @Override
      public void testMonitor(TestContext should) {
         super.testMonitor(should);
      }

      @Ignore @Test
      @Override
      public void testBgsave(TestContext should) {
         super.testBgsave(should);
      }

      @Ignore @Test
      @Override
      public void testGetbit(TestContext should) {
         super.testGetbit(should);
      }

      @Ignore @Test
      @Override
      public void testHsetnx(TestContext should) {
         super.testHsetnx(should);
      }

      @Ignore @Test
      @Override
      public void testObject(TestContext should) {
         super.testObject(should);
      }

      @Ignore @Test
      @Override
      public void testSelect(TestContext should) {
         super.testSelect(should);
      }

      @Ignore @Test
      @Override
      public void testSetbit(TestContext should) {
         super.testSetbit(should);
      }

      @Ignore @Test
      @Override
      public void testBitcount(TestContext should) {
         super.testBitcount(should);
      }
   }

   @RunWith(VertxRespNestRunner.class)
   public static class RedisClient7Test extends io.vertx.test.redis.RedisClient7Test { }

   @RunWith(VertxRespNestRunner.class)
   public static class RedisClient6Test extends io.vertx.test.redis.RedisClient6Test { }

   @RunWith(VertxRespNestRunner.class)
   public static class RedisClient6SecureTest extends io.vertx.test.redis.RedisClient6SecureTest { }

   @RunWith(VertxRespNestRunner.class)
   public static class RedisReconnectTest extends io.vertx.test.redis.RedisReconnectTest { }
}
