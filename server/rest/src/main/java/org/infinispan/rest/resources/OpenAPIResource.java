package org.infinispan.rest.resources;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.util.Version;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.InvocationRegistry;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.PathItem;
import org.infinispan.rest.framework.ResourceDescription;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;

import io.netty.handler.codec.http.HttpResponseStatus;

public class OpenAPIResource implements ResourceHandler {

   private final InvocationHelper invocationHelper;
   private final InvocationRegistry registry;
   private volatile Json openapi = null;

   public OpenAPIResource(InvocationHelper invocationHelper, InvocationRegistry registry) {
      this.invocationHelper = invocationHelper;
      this.registry = registry;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("openapi", "Generates the OpenAPI description")
            .invocation().method(Method.GET).path("/v2/openapi").handleWith(this::createResponse).anonymous(true)
            .create();
   }

   private CompletionStage<RestResponse> createResponse(RestRequest request) {
      if (openapi == null) {
         synchronized (this) {
            if (openapi == null) {
               OpenAPIDocument document = generateOpenAPIDocument();
               openapi = document.toJson();
            }
         }
      }

      return CompletableFuture.completedFuture(
            invocationHelper.newResponse(request)
                  .contentType(MediaType.APPLICATION_JSON)
                  .entity(openapi)
                  .status(HttpResponseStatus.OK)
                  .build()
      );
   }

   private OpenAPIDocument generateOpenAPIDocument() {
      Set<Path> paths = new HashSet<>();
      Set<ResourceDescription> resources = new HashSet<>();
      Set<String> control = new HashSet<>();
      registry.traverse((ignore, invocation) -> {
         for (String p : invocation.paths()) {
            // All paths in OpenAPI must start with `/`.
            if (p.charAt(0) != '/') p = "/" + p;
            if (!control.add(p)) continue;

            resources.add(invocation.resourceGroup());

            String name = invocation.getName() != null
                  ? invocation.getName()
                  : "-";

            List<Parameter> parameters = null;
            if (invocation.getAction() != null) {
               parameters = new ArrayList<>();

               Parameter parameter = new Parameter("action", ParameterIn.QUERY, true, new Schema(String.class));
               parameters.add(parameter);
            }

            for (String param : PathItem.retrieveAllPathVariables(p)) {
               Parameter parameter = new Parameter(param, ParameterIn.PATH, true, new Schema(String.class));
               if (parameters == null) parameters = new ArrayList<>();

               parameters.add(parameter);
            }

            ResponseContent rc = new ResponseContent("Response on success", HttpResponseStatus.OK, Map.of(MediaType.APPLICATION_JSON, new Schema(String.class)));

            Operation operation = new Operation(name, invocation.deprecated(), invocation.resourceGroup(), parameters, Collections.singleton(rc));
            for (Method method : invocation.methods()) {
               Path path = new Path(p, operation, method);
               paths.add(path);
            }
         }
      });

      return new OpenAPIDocument("3.0.3", Info.INFINISPAN, new Paths(paths), resources);
   }

   /**
    * This is the root document object of the OpenAPI document.
    *
    * @param openapi: A <b>required</b> field with the SemVer of the OpenAPI specification.
    * @param info: A <b>required</b> field that contains metadata about the API.
    * @param paths: A <b>required</b> property with the paths and operation available in the API.
    * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md">OpenAPI Specification</a>
    */
   private record OpenAPIDocument(
         String openapi,
         Info info,
         Paths paths,
         Collection<ResourceDescription> resources
   ) implements JsonSerialization {

      @Override
      public Json toJson() {
         Json tags = Json.array();
         for (ResourceDescription resource : resources) {
            Json tag = Json.object()
                  .set("name", resource.group())
                  .set("description", resource.description());
            tags.add(tag);
         }
         return Json.object()
               .set("openapi", openapi)
               .set("info", info)
               .set("paths", paths)
               .set("tags", tags);
      }
   }

   /**
    * The {@link Info} object holds metadata about the API.
    *
    * @param title: A <b>required</b> property with the title of the API.
    * @param description: A <i>optional</i> property with a description about the API.
    * @param license: A <i>optional</i> property with the license information about the software.
    * @param version: A <b>required</b> property with the software version.
    * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#info-object">Info object schema</a>
    */
   private record Info(
         String title,
         String description,
         License license,
         String version
   ) implements JsonSerialization {
      private static final Info INFINISPAN = new Info("Infinispan REST API", "OpenAPI description of Infinispan REST endpoint",
            License.APACHE_2, Version.getVersion());

      @Override
      public Json toJson() {
         return Json.object()
               .set("title", title)
               .set("description", description)
               .set("version", version);
      }
   }

   /**
    * License information for the exposed API.
    *
    * @param name: A <b>required</b> property with the license's name.
    * @param url: An <i>optional</i> property with a URL pointing to the license.
    * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#license-object">License object schema.</a>
    */
   private record License(
         String name,
         String url
   ) implements JsonSerialization {
      private static final License APACHE_2 = new License("Apache 2.0", "https://www.apache.org/licenses/LICENSE-2.0.html");

      @Override
      public Json toJson() {
         return Json.object()
               .set("name", name)
               .set("url", url);
      }
   }

   private record Paths(Collection<Path> paths) implements JsonSerialization {

      @Override
      public Json toJson() {
         Json json = Json.object();
         for (Path path : paths) {
            json.set(path.path(), path);
         }
         return json;
      }
   }

   /**
    * Holds information about the relative paths and operations.
    * <p>
    * This class holds information about each path and parameters. Holding all the information necessary to recreate the
    * tree of objects defined in the schema without additional objects.
    * </p>
    *
    * @param path: A <b>required</b> property. The relative path (starting with `/`) to an individual endpoint.
    * @param method: A <b>required</b> property holding the HTTP method information.
    * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#paths-object">Paths object schema.</a>
    */
   private record Path(
         String path,
         Operation operation,
         Method method
   ) implements JsonSerialization {

      @Override
      public Json toJson() {
         return Json.object(method.name().toLowerCase(), operation);
      }
   }

   private record Operation(
         String summary,
         boolean deprecated,
         ResourceDescription group,
         List<Parameter> parameters,
         Collection<ResponseContent> responses
   ) implements JsonSerialization {

      @Override
      public Json toJson() {
         Json params = Json.array();
         if (parameters != null) {
            for (Parameter parameter : parameters) {
               params.add(parameter);
            }
         }

         Json responses = Json.object();
         for (ResponseContent response : this.responses) {
            responses.set(String.valueOf(response.status.code()), response);
         }

         return Json.object()
               .set("summary", summary)
               .set("tags", Collections.singleton(group.group()))
               .set("deprecated", deprecated)
               .set("parameters", params)
               .set("responses", responses);
      }
   }

   private record ResponseContent(
         String description,
         HttpResponseStatus status,
         Map<MediaType, Schema> responses
   ) implements JsonSerialization {

      @Override
      public Json toJson() {
         Json content = Json.object();
         for (Map.Entry<MediaType, Schema> entry : responses.entrySet()) {
            content.set(entry.getKey().toString(), Json.object("schema", entry.getValue()));
         }
         return Json.object()
               .set("description", description)
               .set("content", content);
      }
   }

   private record Schema(Class<?> clazz) implements JsonSerialization {
      @Override
      public Json toJson() {
         Json json = Json.object();
         inspect(clazz, json);
         return json;
      }
   }

   private static void inspect(Class<?> clazz, Json json) {
      boolean done = true;
      if (clazz.isPrimitive()) {
         if (clazz.isAssignableFrom(boolean.class) || clazz.isAssignableFrom(Boolean.class)) {
            json.set("type", "boolean");
         } else {
            json.set("type", "number");
         }
      } else {
         if (clazz.isAssignableFrom(String.class) || clazz.isEnum()) {
            json.set("type", "string");
         } else if (clazz.isArray()) {
            json.set("type", "array");
         } else {
            if ((done = !isLocalClass(clazz))) {
               json.set("type", "object");
            }
         }
      }

      if (done) return;

      Json properties = Json.object();
      for (Field field : clazz.getDeclaredFields()) {
         System.out.println("Field: " + field);
         Json f = Json.object();
         inspect(field.getType(), f);
         properties.set(field.getName(), f);
      }

      Json internal = Json.object()
            .set("type", "object")
            .set("properties", properties);
      json.set(clazz.getSimpleName(), internal);
   }

   private static boolean isLocalClass(Class<?> clazz) {
      return clazz.getPackage().getName().startsWith("org.infinispan");
   }

   public static void main(String[] args) {
      Json str = Json.object();
      inspect(String.class, str);
      System.out.println(str);

      Json nested = Json.object();
      inspect(Parameter.class, nested);
      System.out.println(nested);

   }

   /**
    * Describe the parameters of a single operation.
    * <p>
    * The parameter is uniquely identified by the name and location.
    * </p>
    *
    * @param name: A <b>required</b> case-sensitive property.
    * @param in
    * @param required
    * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#parameter-object">Parameter object schema</a>
    */
   private record Parameter(
         String name,
         ParameterIn in,
         boolean required,
         Schema schema
   ) implements JsonSerialization {

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Parameter parameter = (Parameter) o;
         return Objects.equals(name, parameter.name) && in == parameter.in;
      }

      @Override
      public int hashCode() {
         return Objects.hash(name, in);
      }

      @Override
      public Json toJson() {
         return Json.object()
               .set("name", name)
               .set("in", in.toString())
               .set("required", required)
               .set("schema", schema);
      }
   }

   private enum ParameterIn {
      QUERY, HEADER, PATH, COOKIE;

      @Override
      public String toString() {
         return name().toLowerCase();
      }
   }
}
