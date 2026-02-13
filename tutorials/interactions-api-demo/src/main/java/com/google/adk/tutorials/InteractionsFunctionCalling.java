/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.adk.tutorials;

import com.google.adk.agents.BaseAgent;
import com.google.adk.agents.LlmAgent;
import com.google.adk.models.Gemini;
import com.google.adk.tools.Annotations.Schema;
import com.google.adk.tools.FunctionTool;
import com.google.adk.web.AdkWebServer;
import java.util.Map;

/**
 * Function calling tutorial using the Interactions API.
 *
 * <p>Demonstrates how tools work with the Interactions API. The agent can call {@code getWeather}
 * and {@code getPopulation} functions. The Interactions API handles the function call / function
 * response loop using {@code previousInteractionId} chaining, so only the latest turn is sent on
 * each request.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@function-calling -pl tutorials/interactions-api-demo
 * </pre>
 */
public class InteractionsFunctionCalling {

  public static final BaseAgent ROOT_AGENT =
      LlmAgent.builder()
          .name("interactions_tool_agent")
          .model(Gemini.builder().modelName("gemini-2.5-flash").useInteractionsApi(true).build())
          .description("Agent that uses tools via the Interactions API.")
          .instruction(
              "You are a helpful agent that can look up weather and population information for"
                  + " cities. Use the available tools to answer user questions.")
          .tools(
              FunctionTool.create(InteractionsFunctionCalling.class, "getWeather"),
              FunctionTool.create(InteractionsFunctionCalling.class, "getPopulation"))
          .build();

  public static Map<String, String> getWeather(
      @Schema(name = "city", description = "The city to get weather for") String city) {
    Map<String, Map<String, String>> data =
        Map.of(
            "tokyo",
            Map.of("status", "success", "report", "Clear skies, 20°C (68°F)."),
            "paris",
            Map.of("status", "success", "report", "Partly cloudy, 15°C (59°F)."),
            "london",
            Map.of("status", "success", "report", "Rainy, 12°C (54°F)."));
    return data.getOrDefault(
        city.toLowerCase().trim(),
        Map.of("status", "error", "report", "Weather data not available for " + city + "."));
  }

  public static Map<String, String> getPopulation(
      @Schema(name = "city", description = "The city to get population for") String city) {
    Map<String, Map<String, String>> data =
        Map.of(
            "tokyo",
            Map.of("status", "success", "report", "Approximately 14 million."),
            "paris",
            Map.of("status", "success", "report", "Approximately 2.1 million."),
            "london",
            Map.of("status", "success", "report", "Approximately 9 million."));
    return data.getOrDefault(
        city.toLowerCase().trim(),
        Map.of("status", "error", "report", "Population data not available for " + city + "."));
  }

  public static void main(String[] args) {
    AdkWebServer.start(ROOT_AGENT);
  }
}
