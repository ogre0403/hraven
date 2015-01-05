/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package org.nchc.rest;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.hraven.AppSummary;
import com.twitter.hraven.Counter;
import com.twitter.hraven.CounterMap;

/**
 * Class that provides custom JSON bindings (where needed) for out object model.
 */
@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {
  private final ObjectMapper customMapper;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(ObjectMapperProvider.class);

  public ObjectMapperProvider() {
    customMapper = createCustomMapper();
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return customMapper;
  }

  public static ObjectMapper createCustomMapper() {
    ObjectMapper result = new ObjectMapper();
    result.configure(Feature.INDENT_OUTPUT, true);
    SimpleModule module = new SimpleModule("hRavenModule", new Version(0, 4, 0, null));
    addJobMappings(module);
    result.registerModule(module);
    return result;
  }

  private static SimpleModule addJobMappings(SimpleModule module) {
    module.addSerializer(Configuration.class, new ConfigurationSerializer());
    module.addSerializer(CounterMap.class, new CounterSerializer());
    module.addSerializer(AppSummary.class, new AppSummarySerializer());
    return module;
  }

  /**
   * Custom serializer for Configuration object. We don't want to serialize the classLoader.
   */
  public static class ConfigurationSerializer extends JsonSerializer<Configuration> {

    @Override
    public void serialize(Configuration conf, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {
      Iterator<Map.Entry<String, String>> keyValueIterator = conf.iterator();

      jsonGenerator.writeStartObject();

      // here's where we can filter out keys if we want
      while (keyValueIterator.hasNext()) {
        Map.Entry<String, String> kvp = keyValueIterator.next();

          jsonGenerator.writeFieldName(kvp.getKey());
          jsonGenerator.writeString(kvp.getValue());
      }
      jsonGenerator.writeEndObject();
    }
  }


  /**
   * Custom serializer for Configuration object. We don't want to serialize the classLoader.
   */
  public static class CounterSerializer extends JsonSerializer<CounterMap> {

    @Override
    public void serialize(CounterMap counterMap, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {

      jsonGenerator.writeStartObject();
      for (String group : counterMap.getGroups()) {
        jsonGenerator.writeFieldName(group);

        jsonGenerator.writeStartObject();
        Map<String, Counter> groupMap = counterMap.getGroup(group);
        for (String counterName : groupMap.keySet()) {
          Counter counter = groupMap.get(counterName);
          jsonGenerator.writeFieldName(counter.getKey());
          jsonGenerator.writeNumber(counter.getValue());
        }
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndObject();
    }
  }

  /**
   * Custom serializer for App object. We don't want to serialize the
   * classLoader. based on the parameters passed by caller, we determine which
   * fields to include in serialized response
   */
  public static class AppSummarySerializer extends JsonSerializer<AppSummary> {
    @Override
    public void serialize(AppSummary anApp, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(
            addJobMappings(new SimpleModule("hRavenModule", new Version(0, 4, 0, null))));
        om.writeValue(jsonGenerator, anApp);
    }
  }
}
