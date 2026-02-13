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
import com.google.adk.tools.BuiltInCodeExecutionTool;
import com.google.adk.web.AdkWebServer;

/**
 * Code execution tutorial using the Interactions API.
 *
 * <p>Demonstrates how built-in tools like {@link BuiltInCodeExecutionTool} work with the
 * Interactions API. The agent can execute Python code to answer computational questions. Code
 * execution parts ({@code executableCode} and {@code codeExecutionResult}) are converted from the
 * Interactions API response types into standard ADK parts.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@code-execution -pl tutorials/interactions-api-demo
 * </pre>
 */
public class InteractionsCodeExecution {

  public static final BaseAgent ROOT_AGENT =
      LlmAgent.builder()
          .name("interactions_code_agent")
          .model(Gemini.builder().modelName("gemini-2.5-flash").useInteractionsApi(true).build())
          .description("Agent that executes code via the Interactions API.")
          .instruction(
              "You are a helpful assistant that can run Python code to answer questions."
                  + " Use code execution for mathematical and computational questions.")
          .tools(new BuiltInCodeExecutionTool())
          .build();

  public static void main(String[] args) {
    AdkWebServer.start(ROOT_AGENT);
  }
}
