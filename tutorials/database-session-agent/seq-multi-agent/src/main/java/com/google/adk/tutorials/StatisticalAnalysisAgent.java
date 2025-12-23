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
import com.google.adk.agents.SequentialAgent;

public class StatisticalAnalysisAgent {

  // Model name is configurable via GEMINI_MODEL environment variable
  // Default: gemini-2.5-flash
  private static final String MODEL =
      System.getenv().getOrDefault("GEMINI_MODEL", "gemini-2.5-flash");

  public static final BaseAgent STATISTICAL_ANALYSIS_PIPELINE = createStatisticalPipeline();

  private static BaseAgent createStatisticalPipeline() {
    LlmAgent centralTendencyAgent =
        LlmAgent.builder()
            .name("central_tendency_agent")
            .model(MODEL)
            .description("Agent that calculates measures of central tendency")
            .instruction(
                """
                You are a statistical agent that calculates measures of central tendency.

                Given a dataset (array of numbers), calculate:
                1. Mean (average)
                2. Median (middle value)
                3. Mode (most frequent value)

                Return your results in this JSON format:
                {
                  "mean": <value>,
                  "median": <value>,
                  "mode": <value or array if multiple modes>,
                  "sample_size": <count>
                }

                Be precise with calculations. If there is no mode, indicate "no_mode".
                """)
            .outputKey("central_stats")
            .build();

    LlmAgent dispersionAgent =
        LlmAgent.builder()
            .name("dispersion_agent")
            .model(MODEL)
            .description("Agent that calculates measures of dispersion")
            .instruction(
                """
                You are a statistical agent that calculates measures of dispersion.

                You will receive:
                1. The original dataset
                2. The central_stats from the previous agent (mean, median, mode)

                Calculate:
                1. Variance (average squared deviation from mean)
                2. Standard Deviation (square root of variance)
                3. Range (max - min)
                4. Coefficient of Variation (std dev / mean)

                Return your results in this JSON format:
                {
                  "variance": <value>,
                  "standard_deviation": <value>,
                  "range": <value>,
                  "coefficient_of_variation": <value>,
                  "min": <value>,
                  "max": <value>
                }

                Be precise with calculations.
                """)
            .outputKey("dispersion_stats")
            .build();

    LlmAgent outlierDetectorAgent =
        LlmAgent.builder()
            .name("outlier_detector_agent")
            .model(MODEL)
            .description("Agent that detects outliers using the Z-score method")
            .instruction(
                """
                You are a statistical agent that detects outliers using the Z-score method.

                You will receive:
                1. The original dataset
                2. central_stats (mean, median, mode)
                3. dispersion_stats (standard deviation, variance, etc.)

                For each data point, calculate the Z-score:
                Z = (value - mean) / standard_deviation

                Identify outliers as values where |Z| > 2 (beyond 2 standard deviations).

                Create a comprehensive summary in this format:

                📊 STATISTICAL ANALYSIS SUMMARY
                ================================

                Dataset: [original dataset]
                Sample Size: [count]

                Central Tendency:
                  Mean: [mean]
                  Median: [median]
                  Mode: [mode]

                Dispersion:
                  Standard Deviation: [std_dev]
                  Variance: [variance]
                  Range: [min] to [max]
                  Coefficient of Variation: [cv]

                Outlier Detection (Z-score method):
                  [List any outliers with their Z-scores]
                  [Or state "No outliers detected"]

                Conclusion:
                  [Brief assessment of the data distribution]

                ================================
                """)
            .outputKey("summary")
            .build();

    return SequentialAgent.builder()
        .name("StatisticalAnalysisPipeline")
        .subAgents(centralTendencyAgent, dispersionAgent, outlierDetectorAgent)
        .description(
            "Sequential pipeline that performs comprehensive statistical analysis: central"
                + " tendency, dispersion, and outlier detection")
        .build();
  }
}
