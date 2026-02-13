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

/**
 * Agent demonstrating the Gemini Interactions API with DatabaseSessionService.
 *
 * <p>This agent uses {@code Gemini.builder().useInteractionsApi(true)} to enable the Interactions
 * API. Instead of sending the full conversation history on every call, interactions are chained via
 * {@code previousInteractionId}, letting the Gemini server maintain conversation state.
 *
 * <p>This matches the Python ADK pattern:
 *
 * <pre>
 * Agent(
 *     model=Gemini(
 *         model="gemini-2.5-flash",
 *         use_interactions_api=True,
 *     ),
 *     name="my_agent",
 * )
 * </pre>
 */
public class SimpleQuestionAnswerAgent {

  private static final String MODEL = "gemini-2.5-flash";

  public static final BaseAgent ROOT_AGENT = createRootAgent();

  private static BaseAgent createRootAgent() {
    // Create a Gemini model with the Interactions API enabled.
    // This uses client.interactions.create() instead of client.models.generateContent().
    Gemini gemini = Gemini.builder().modelName(MODEL).useInteractionsApi(true).build();

    return LlmAgent.builder()
        .name("InteractionsApiAgent")
        .model(gemini)
        .description("Agent that answers questions using the Gemini Interactions API")
        .instruction("You are a helpful assistant that answers questions accurately and concisely.")
        .build();
  }
}
