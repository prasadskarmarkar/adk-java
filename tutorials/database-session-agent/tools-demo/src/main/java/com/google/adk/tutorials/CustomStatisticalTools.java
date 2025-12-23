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

import com.google.adk.tools.Annotations.Schema;
import com.google.adk.tools.FunctionTool;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CustomStatisticalTools {

  public static final FunctionTool VALIDATE_DATASET =
      FunctionTool.create(CustomStatisticalTools.class, "validateDataset");

  public static final FunctionTool EXPORT_RESULTS =
      FunctionTool.create(CustomStatisticalTools.class, "exportResults");

  public static final FunctionTool PERFORM_HYPOTHESIS_TEST =
      FunctionTool.create(CustomStatisticalTools.class, "performHypothesisTest");

  @Schema(
      name = "validate_dataset",
      description =
          "Validates a numerical dataset by checking for invalid values, missing data, and basic constraints")
  public static Map<String, Object> validateDataset(
      @Schema(
              name = "dataset",
              description = "Array of numbers as a JSON string, e.g., '[10, 20, 30]'")
          String dataset) {

    try {
      List<Double> numbers = parseDataset(dataset);

      List<String> issues = new ArrayList<>();

      if (numbers.isEmpty()) {
        issues.add("Dataset is empty");
      }

      if (numbers.size() < 3) {
        issues.add("Dataset has fewer than 3 values - statistical analysis may be unreliable");
      }

      long nanCount = numbers.stream().filter(n -> n.isNaN() || n.isInfinite()).count();
      if (nanCount > 0) {
        issues.add(nanCount + " invalid values (NaN or Infinity) detected");
      }

      if (issues.isEmpty()) {
        return Map.of(
            "valid",
            true,
            "message",
            "Dataset is valid",
            "sample_size",
            numbers.size(),
            "data_range",
            String.format(
                "%.2f to %.2f",
                numbers.stream().mapToDouble(Double::doubleValue).min().orElse(0.0),
                numbers.stream().mapToDouble(Double::doubleValue).max().orElse(0.0)));
      } else {
        return Map.of("valid", false, "issues", issues, "sample_size", numbers.size());
      }
    } catch (Exception e) {
      return Map.of("valid", false, "error", "Failed to parse dataset: " + e.getMessage());
    }
  }

  @Schema(
      name = "export_results",
      description = "Exports statistical analysis results to a specified format (CSV or JSON)")
  public static Map<String, String> exportResults(
      @Schema(name = "format", description = "Export format: 'csv' or 'json'") String format,
      @Schema(name = "results", description = "Statistical results as a JSON object string")
          String results) {

    if (format.equalsIgnoreCase("csv")) {
      String csv =
          "metric,value\n"
              + "mean,55.0\n"
              + "median,55.0\n"
              + "std_dev,30.28\n"
              + "Note: This is a demo export";
      return Map.of(
          "status", "success",
          "format", "CSV",
          "content", csv,
          "filename", "stats_export.csv");
    } else if (format.equalsIgnoreCase("json")) {
      String json =
          "{\n"
              + "  \"mean\": 55.0,\n"
              + "  \"median\": 55.0,\n"
              + "  \"std_dev\": 30.28,\n"
              + "  \"note\": \"This is a demo export\"\n"
              + "}";
      return Map.of(
          "status", "success",
          "format", "JSON",
          "content", json,
          "filename", "stats_export.json");
    } else {
      return Map.of(
          "status", "error", "message", "Unsupported format: " + format + ". Use 'csv' or 'json'.");
    }
  }

  @Schema(
      name = "perform_hypothesis_test",
      description =
          "Performs a simple hypothesis test to check if the dataset mean differs significantly from a hypothesized value")
  public static Map<String, Object> performHypothesisTest(
      @Schema(
              name = "dataset",
              description = "Array of numbers as a JSON string, e.g., '[10, 20, 30]'")
          String dataset,
      @Schema(
              name = "hypothesized_mean",
              description = "The hypothesized population mean to test against")
          double hypothesizedMean,
      @Schema(name = "alpha", description = "Significance level (e.g., 0.05 for 95% confidence)")
          double alpha) {

    try {
      List<Double> numbers = parseDataset(dataset);

      if (numbers.size() < 2) {
        return Map.of("error", "Need at least 2 values for hypothesis testing");
      }

      double mean = numbers.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      double variance =
          numbers.stream().mapToDouble(n -> Math.pow(n - mean, 2)).sum() / (numbers.size() - 1);
      double stdDev = Math.sqrt(variance);
      double stdError = stdDev / Math.sqrt(numbers.size());

      double tStatistic = (mean - hypothesizedMean) / stdError;
      double pValue = 2.0 * (1.0 - 0.95);

      boolean reject = Math.abs(tStatistic) > 2.0;

      return Map.of(
          "test_type", "One-sample t-test",
          "null_hypothesis", "Mean = " + hypothesizedMean,
          "sample_mean", mean,
          "t_statistic", tStatistic,
          "p_value_approx", pValue,
          "alpha", alpha,
          "reject_null", reject,
          "conclusion",
              reject
                  ? "Reject null hypothesis - mean differs significantly from " + hypothesizedMean
                  : "Fail to reject null hypothesis - no significant difference from "
                      + hypothesizedMean,
          "note", "This is a simplified t-test for demonstration purposes");
    } catch (Exception e) {
      return Map.of("error", "Failed to perform hypothesis test: " + e.getMessage());
    }
  }

  private static List<Double> parseDataset(String dataset) {
    String cleaned = dataset.trim().replaceAll("[\\[\\]]", "");
    return Arrays.stream(cleaned.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Double::parseDouble)
        .collect(Collectors.toList());
  }

  private CustomStatisticalTools() {}
}
