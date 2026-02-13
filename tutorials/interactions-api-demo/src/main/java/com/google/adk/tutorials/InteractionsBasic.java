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
import com.google.adk.web.AdkWebServer;

/**
 * Basic Interactions API tutorial.
 *
 * <p>Demonstrates the simplest usage of the Interactions API by enabling {@code
 * useInteractionsApi(true)} on the Gemini model. Instead of sending the full conversation history
 * on every request, interactions are chained via {@code previousInteractionId}, letting the server
 * maintain conversation state.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@basic -pl tutorials/interactions-api-demo
 * </pre>
 */
public class InteractionsBasic {

  public static final BaseAgent ROOT_AGENT =
      LlmAgent.builder()
          .name("interactions_basic_agent")
          .model(Gemini.builder().modelName("gemini-2.5-flash").useInteractionsApi(true).build())
          .description("Agent that answers questions using the Gemini Interactions API.")
          .instruction("You are a helpful assistant. Answer questions accurately and concisely.")
          .build();

  public static void main(String[] args) {
    AdkWebServer.start(ROOT_AGENT);
  }
}
