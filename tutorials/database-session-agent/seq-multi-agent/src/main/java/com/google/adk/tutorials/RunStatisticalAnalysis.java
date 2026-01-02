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

import com.google.adk.agents.RunConfig;
import com.google.adk.artifacts.InMemoryArtifactService;
import com.google.adk.events.Event;
import com.google.adk.memory.InMemoryMemoryService;
import com.google.adk.runner.Runner;
import com.google.adk.sessions.DatabaseSessionService;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RunStatisticalAnalysis {

  private static final String APP_NAME = "statistical_analysis_app";
  private static final String USER_ID = "stats_user";

  private static String getDatabaseUrl() {
    String databaseUrl = System.getenv("DATABASE_URL");
    if (databaseUrl != null && !databaseUrl.isEmpty()) {
      System.out.println("Using DATABASE_URL from environment");
      return databaseUrl;
    }

    String h2File = System.getProperty("user.dir") + "/stats_analysis_db";
    String h2Url = "jdbc:h2:" + h2File + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1;USER=sa;PASSWORD=";
    System.out.println("Using H2 database (file-based): " + h2File + ".mv.db");
    return h2Url;
  }

  public static void main(String[] args) {
    System.out.println("╔════════════════════════════════════════════════════════════╗");
    System.out.println("║     STATISTICAL ANALYSIS PIPELINE TUTORIAL                 ║");
    System.out.println("║     with DatabaseSessionService                            ║");
    System.out.println("╚════════════════════════════════════════════════════════════╝");
    System.out.println();

    String dbUrl = getDatabaseUrl();
    System.out.println("Database: " + dbUrl);
    System.out.println("App Name: " + APP_NAME);
    System.out.println();

    // Use try-with-resources to ensure DatabaseSessionService is always closed
    try (DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl)) {
      System.out.println("✓ DatabaseSessionService initialized");

      Runner runner =
          new Runner(
              StatisticalAnalysisAgent.STATISTICAL_ANALYSIS_PIPELINE,
              APP_NAME,
              new InMemoryArtifactService(),
              sessionService,
              new InMemoryMemoryService());
      System.out.println("✓ Runner initialized");
      System.out.println();

      String sessionId = null;
      String[] datasets = null;

      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("--session-id") && i + 1 < args.length) {
          sessionId = args[i + 1];
          i++;
        } else if (datasets == null) {
          datasets = new String[] {args[i]};
        }
      }

      if (datasets == null) {
        System.out.println(
            "Usage: mvn compile exec:java -Dexec.args=\"[10, 20, 30, 40, 50] --session-id <session_id>\"");
        System.out.println();
        System.out.println(
            "Running with default test dataset: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]");
        System.out.println();
        datasets = new String[] {"[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]"};
      }

      runAnalysis(runner, datasets, sessionId);
    } // DatabaseSessionService automatically closed here
    System.exit(0);
  }

  private static void runAnalysis(Runner runner, String[] datasets, String providedSessionId) {
    String sessionId =
        providedSessionId != null ? providedSessionId : java.util.UUID.randomUUID().toString();

    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();

    initialState.put("app:config.max_outlier_threshold", 2.5);
    initialState.put("app:config.precision_digits", 4);
    initialState.put("app:config.enable_visualizations", true);
    initialState.put("app:version", "1.0.0");
    initialState.put("app:environment", "production");
    initialState.put("app:config.random_timeout_ms", (int) (Math.random() * 5000) + 1000);
    initialState.put("app:config.random_timestamp", System.currentTimeMillis());

    initialState.put("user:preferences.notification_enabled", true);
    initialState.put("user:preferences.theme", "dark");
    initialState.put("user:preferences.report_format", "detailed");
    initialState.put("user:analysis_history_count", 0);
    initialState.put("user:last_login", System.currentTimeMillis());
    initialState.put("user:preferences.random_batch_size", (int) (Math.random() * 100) + 10);
    initialState.put("user:preferences.random_id", java.util.UUID.randomUUID().toString());
    initialState.put("user:config.random_timeout_ms", (int) (Math.random() * 5000) + 1000);
    initialState.put("user:config.random_timestamp", System.currentTimeMillis());

    initialState.put("analysis_count", 0);
    initialState.put("session_start_time", System.currentTimeMillis());

    com.google.adk.sessions.Session existingSession =
        runner
            .sessionService()
            .getSession(runner.appName(), USER_ID, sessionId, java.util.Optional.empty())
            .blockingGet();

    if (existingSession == null) {
      System.out.println("Creating new session with default app and user config...");
      runner
          .sessionService()
          .createSession(runner.appName(), USER_ID, initialState, sessionId)
          .blockingGet();
      System.out.println("✓ Session created: " + sessionId);
      System.out.println("✓ Initial state configured:");
      System.out.println("  - App config: max_outlier_threshold=2.5, precision_digits=4");
      System.out.println("  - User preferences: theme=dark, report_format=detailed");
      System.out.println("  - Session state: analysis_count=0");
    } else {
      System.out.println("✓ Using existing session: " + sessionId);
      System.out.println("✓ Session state will be preserved from previous run");
    }
    System.out.println();

    for (int i = 0; i < datasets.length; i++) {
      String dataset = datasets[i];
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      System.out.println("Analysis " + (i + 1) + ": Dataset " + dataset);
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      System.out.println();

      List<Event> events = new ArrayList<>();
      Content userContent =
          Content.builder()
              .role("user")
              .parts(List.of(Part.fromText("Analyze this dataset: " + dataset)))
              .build();

      RunConfig runConfig = RunConfig.builder().build();
      for (Event event :
          runner.runAsync(USER_ID, sessionId, userContent, runConfig).blockingIterable()) {
        events.add(event);

        if (event.author().equals("outlier_detector_agent") && event.content().isPresent()) {
          event
              .content()
              .get()
              .parts()
              .ifPresent(
                  parts ->
                      parts.forEach(
                          part ->
                              part.text()
                                  .ifPresent(
                                      text -> {
                                        System.out.println(text);
                                        System.out.println();
                                      })));
        }
      }

      System.out.println("Events collected: " + events.size());
      System.out.println();
    }

    System.out.println("╔════════════════════════════════════════════════════════════╗");
    System.out.println("║  Analysis completed successfully!                         ║");
    System.out.println("║  Session ID: " + sessionId + "  ║");
    System.out.println("╚════════════════════════════════════════════════════════════╝");
    System.out.println();

    System.out.println("Verifying session retrieval from database...");
    com.google.adk.sessions.Session retrievedSession =
        runner
            .sessionService()
            .getSession(APP_NAME, USER_ID, sessionId, java.util.Optional.empty())
            .blockingGet();

    if (retrievedSession != null) {
      System.out.println("✓ Session retrieved successfully");
      System.out.println();
      long appStateCount =
          retrievedSession.state().keySet().stream().filter(key -> key.startsWith("app:")).count();
      System.out.println("  App State (" + appStateCount + " keys):");
      retrievedSession.state().entrySet().stream()
          .filter(entry -> entry.getKey().startsWith("app:"))
          .forEach(
              entry -> {
                System.out.println("    " + entry.getKey() + " = " + entry.getValue());
              });
      System.out.println();
      long userStateCount =
          retrievedSession.state().keySet().stream().filter(key -> key.startsWith("user:")).count();
      System.out.println("  User State (" + userStateCount + " keys):");
      retrievedSession.state().entrySet().stream()
          .filter(entry -> entry.getKey().startsWith("user:"))
          .forEach(
              entry -> {
                System.out.println("    " + entry.getKey() + " = " + entry.getValue());
              });
      System.out.println();
      long sessionStateCount =
          retrievedSession.state().keySet().stream()
              .filter(key -> !key.startsWith("app:") && !key.startsWith("user:"))
              .count();
      System.out.println("  Session State (" + sessionStateCount + " keys):");
      retrievedSession.state().entrySet().stream()
          .filter(
              entry -> !entry.getKey().startsWith("app:") && !entry.getKey().startsWith("user:"))
          .forEach(
              entry -> {
                System.out.println("    " + entry.getKey() + " = " + entry.getValue());
              });
    } else {
      System.out.println("✗ Failed to retrieve session");
    }
    System.out.println();

    System.out.println("💾 Session has been persisted to the database!");
    System.out.println("   - All agent executions saved");
    System.out.println("   - State shared via outputKey (central_stats, dispersion_stats)");
    System.out.println("   - App/User state successfully retrieved");
    System.out.println();
  }
}
