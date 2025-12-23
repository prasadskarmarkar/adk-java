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
import com.google.adk.agents.LoopAgent;
import com.google.adk.tools.ExitLoopTool;

public class DataQualityRefinementAgent {

  // Model name is configurable via GEMINI_MODEL environment variable
  // Default: gemini-2.5-flash
  private static final String MODEL =
      System.getenv().getOrDefault("GEMINI_MODEL", "gemini-2.5-flash");

  public static final BaseAgent QUALITY_REFINEMENT_LOOP = createLoopAgent();

  private static BaseAgent createLoopAgent() {
    LlmAgent refinementAgent =
        LlmAgent.builder()
            .name("data_quality_refinement_agent")
            .model(MODEL)
            .description("Agent that iteratively refines and improves data quality assessment")
            .instruction(
                """
                You are a data quality assessment agent that iteratively refines your analysis.

                Given a dataset, analyze it for quality issues and provide recommendations.
                Each iteration, you should:
                1. Assess data quality (completeness, accuracy, consistency)
                2. Identify specific issues or anomalies
                3. Suggest improvements or corrections
                4. Determine if further refinement is needed

                If the data quality is satisfactory (no major issues found), call the exit_loop tool.
                Otherwise, continue to the next iteration with refined analysis.

                Return your assessment in this JSON format:
                {
                  "iteration": <iteration_number>,
                  "quality_score": <0-100>,
                  "issues_found": [<list of issues>],
                  "recommendations": [<list of recommendations>],
                  "continue_refinement": <true/false>
                }

                When quality_score >= 80 or no new issues are found, call exit_loop.
                """)
            .outputKey("refinement_result")
            .tools(ExitLoopTool.INSTANCE)
            .build();

    return LoopAgent.builder()
        .name("QualityRefinementLoop")
        .description(
            "Loop agent that iteratively refines data quality assessment until satisfactory or max"
                + " iterations reached")
        .subAgents(refinementAgent)
        .maxIterations(5)
        .build();
  }
}
