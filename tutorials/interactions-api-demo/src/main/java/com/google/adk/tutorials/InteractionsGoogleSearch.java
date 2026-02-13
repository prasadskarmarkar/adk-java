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
import com.google.adk.tools.GoogleSearchTool;
import com.google.adk.web.AdkWebServer;

/**
 * Google Search tutorial using the Interactions API.
 *
 * <p>Demonstrates how the built-in {@link GoogleSearchTool} works with the Interactions API. The
 * agent can search the web to answer questions about current events and real-time information.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@google-search -pl tutorials/interactions-api-demo
 * </pre>
 */
public class InteractionsGoogleSearch {

  public static final BaseAgent ROOT_AGENT =
      LlmAgent.builder()
          .name("interactions_search_agent")
          .model(Gemini.builder().modelName("gemini-2.5-flash").useInteractionsApi(true).build())
          .description("Agent that searches the web via the Interactions API.")
          .instruction(
              "You are a helpful assistant with access to Google Search."
                  + " Use it to answer questions about current events and real-time information.")
          .tools(new GoogleSearchTool())
          .build();

  public static void main(String[] args) {
    AdkWebServer.start(ROOT_AGENT);
  }
}
