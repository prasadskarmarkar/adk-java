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

public class StatisticalToolsAgent {

  // Model name is configurable via GEMINI_MODEL environment variable
  // Default: gemini-2.5-flash
  private static final String MODEL =
      System.getenv().getOrDefault("GEMINI_MODEL", "gemini-2.5-flash");

  public static final BaseAgent TOOLS_DEMO_AGENT = createToolsDemoAgent();

  private static BaseAgent createToolsDemoAgent() {
    return LlmAgent.builder()
        .name("statistical_tools_demo_agent")
        .model(MODEL)
        .description(
            "Agent demonstrating custom statistical tools: validation, export, and hypothesis"
                + " testing")
        .instruction(
            """
            You are a statistical analysis agent with access to custom tools.

            When analyzing a dataset, follow these steps:
            1. First, ALWAYS validate the dataset using validate_dataset tool
            2. If validation passes, perform basic statistical analysis
            3. Use perform_hypothesis_test to test if the mean differs from 50
            4. Export the results using export_results tool (choose CSV or JSON format)

            Provide a comprehensive summary of:
            - Data validation results
            - Statistical findings
            - Hypothesis test outcome
            - Exported results location

            Be thorough and use ALL available tools to demonstrate their capabilities.
            """)
        .outputKey("final_results")
        .tools(
            CustomStatisticalTools.VALIDATE_DATASET,
            CustomStatisticalTools.PERFORM_HYPOTHESIS_TEST,
            CustomStatisticalTools.EXPORT_RESULTS)
        .build();
  }
}
