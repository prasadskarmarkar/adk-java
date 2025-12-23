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

public class RunToolsDemo {

  private static final String APP_NAME = "statistical_tools_demo_app";
  private static final String USER_ID = "tools_user";

  private static String getDatabaseUrl() {
    String databaseUrl = System.getenv("DATABASE_URL");
    if (databaseUrl != null && !databaseUrl.isEmpty()) {
      System.out.println("Using DATABASE_URL from environment");
      return databaseUrl;
    }

    String h2File = System.getProperty("user.dir") + "/tools_demo_db";
    String h2Url = "jdbc:h2:" + h2File + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1;USER=sa;PASSWORD=";
    System.out.println("Using H2 database (file-based): " + h2File + ".mv.db");
    return h2Url;
  }

  public static void main(String[] args) {
    System.out.println("╔════════════════════════════════════════════════════════════╗");
    System.out.println("║     CUSTOM TOOLS DEMO: Statistical FunctionTools          ║");
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
              StatisticalToolsAgent.TOOLS_DEMO_AGENT,
              APP_NAME,
              new InMemoryArtifactService(),
              sessionService,
              new InMemoryMemoryService());
      System.out.println("✓ Runner initialized with 3 custom FunctionTools");
      System.out.println("  - validate_dataset");
      System.out.println("  - perform_hypothesis_test");
      System.out.println("  - export_results");
      System.out.println();

      if (args.length == 0) {
        System.out.println("Usage: mvn compile exec:java -Dexec.args=\"[10, 20, 30, 40, 50]\"");
        System.out.println();
        System.out.println(
            "Running with default test dataset: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]");
        System.out.println();
        runAnalysis(runner, new String[] {"[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]"});
      } else {
        runAnalysis(runner, args);
      }
    } // DatabaseSessionService automatically closed here
    System.exit(0);
  }

  private static void runAnalysis(Runner runner, String[] datasets) {
    String sessionId = java.util.UUID.randomUUID().toString();

    System.out.println("Creating session...");
    runner.sessionService().createSession(runner.appName(), USER_ID, null, sessionId).blockingGet();
    System.out.println("✓ Session created: " + sessionId);
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
              .parts(
                  List.of(
                      Part.fromText("Analyze this dataset using all available tools: " + dataset)))
              .build();

      RunConfig runConfig = RunConfig.builder().build();
      int toolCallCount = 0;

      for (Event event :
          runner.runAsync(USER_ID, sessionId, userContent, runConfig).blockingIterable()) {
        events.add(event);

        if (event.author().equals("statistical_tools_demo_agent")) {
          if (event.content().isPresent()) {
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
                                          if (!text.trim().isEmpty()) {
                                            System.out.println(text);
                                            System.out.println();
                                          }
                                        })));
          }

          if (!event.functionCalls().isEmpty()) {
            toolCallCount += event.functionCalls().size();
            event
                .functionCalls()
                .forEach(
                    call -> {
                      System.out.println("🔧 Tool Called: " + call.name());
                    });
          }
        }
      }

      System.out.println();
      System.out.println("Events collected: " + events.size());
      System.out.println("Tool calls made: " + toolCallCount);
      System.out.println();
    }

    System.out.println("╔════════════════════════════════════════════════════════════╗");
    System.out.println("║  Tools demo completed!                                     ║");
    System.out.println("║  Session ID: " + sessionId + "  ║");
    System.out.println("╚════════════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.println("💾 Session has been persisted to the database!");
    System.out.println("   - All tool calls saved");
    System.out.println("   - Tool responses tracked");
    System.out.println("   - Run again to see session restoration");
    System.out.println();
  }
}
