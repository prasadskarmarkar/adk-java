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

import com.google.adk.agents.LlmAgent;
import com.google.adk.agents.SequentialAgent;
import com.google.adk.models.Gemini;
import com.google.common.collect.ImmutableList;

/**
 * A compound agent that detects user preferences and provides conversational responses.
 *
 * <p>This agent consists of two sub-agents:
 *
 * <ul>
 *   <li><b>PreferenceDetector</b>: Analyzes user input to detect if they're setting a preference
 *   <li><b>ConversationAgent</b>: Provides helpful responses using known user preferences
 * </ul>
 *
 * <p>The preference detector looks for patterns like:
 *
 * <ul>
 *   <li>"My favorite color is blue"
 *   <li>"I like pizza"
 *   <li>"Remember my name is John"
 *   <li>"I prefer dark mode"
 * </ul>
 */
public class PreferenceAgent {

  private static final String PREFERENCE_DETECTOR_INSTRUCTION =
      """
      You are a preference detection system. Analyze the user's message to determine if they are
      setting a preference or providing personal information that should be remembered.

      Look for patterns like:
      - "My favorite X is Y"
      - "I like X"
      - "I prefer X"
      - "Remember my X is Y"
      - "Call me X"
      - "I am X" (where X is a characteristic like "vegetarian", "a student", etc.)

      If you detect a preference:
      1. Extract the preference key (e.g., "favorite_color", "name", "dietary_restriction")
      2. Extract the preference value
      3. Output JSON in this exact format: {"is_preference": true, "key": "preference_key", "value": "preference_value"}

      If NO preference is detected:
      1. Output JSON: {"is_preference": false}

      Examples:
      - Input: "My favorite color is blue"
        Output: {"is_preference": true, "key": "favorite_color", "value": "blue"}

      - Input: "I like pizza"
        Output: {"is_preference": true, "key": "favorite_food", "value": "pizza"}

      - Input: "Call me Alex"
        Output: {"is_preference": true, "key": "preferred_name", "value": "Alex"}

      - Input: "What's the weather today?"
        Output: {"is_preference": false}

      - Input: "I'm vegetarian"
        Output: {"is_preference": true, "key": "dietary_restriction", "value": "vegetarian"}

      IMPORTANT: Always respond with valid JSON only, no other text.
      """;

  private static final String CONVERSATION_AGENT_INSTRUCTION =
      """
      You are a helpful, friendly assistant with access to the user's preferences and conversation history.

      When responding to the user:
      1. If the user just set a preference, acknowledge it warmly
      2. If the user asks about their preferences, retrieve them from the context
      3. Use known preferences to personalize your responses when relevant
      4. Be conversational and helpful

      User preferences are stored with the "user:" prefix in the state. For example:
      - user:favorite_color = "blue"
      - user:preferred_name = "Alex"
      - user:dietary_restriction = "vegetarian"

      If the user asks "What do you know about me?", list all their preferences in a friendly way.

      Be concise and natural in your responses.
      """;

  /**
   * Creates the preference agent with two sub-agents.
   *
   * @param model The LLM model to use for both agents
   * @return A SequentialAgent that processes preferences and provides responses
   */
  public static SequentialAgent create(Gemini model) {
    // Sub-agent 1: Detect if user is setting a preference
    LlmAgent preferenceDetector =
        LlmAgent.builder()
            .name("PreferenceDetector")
            .instruction(PREFERENCE_DETECTOR_INSTRUCTION)
            .model(model)
            .outputKey("preference_detection")
            .build();

    // Sub-agent 2: Main conversation agent
    LlmAgent conversationAgent =
        LlmAgent.builder()
            .name("ConversationAgent")
            .instruction(CONVERSATION_AGENT_INSTRUCTION)
            .model(model)
            .outputKey("response")
            .build();

    // Combine into sequential agent
    return SequentialAgent.builder()
        .name("PreferenceAgent")
        .subAgents(ImmutableList.of(preferenceDetector, conversationAgent))
        .build();
  }
}
