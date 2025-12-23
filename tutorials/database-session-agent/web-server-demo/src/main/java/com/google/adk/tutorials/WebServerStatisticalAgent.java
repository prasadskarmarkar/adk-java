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
import com.google.adk.sessions.BaseSessionService;
import com.google.adk.sessions.DatabaseSessionService;
import com.google.adk.web.AdkWebServer;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootApplication(
    scanBasePackages = {"com.google.adk.tutorials", "com.google.adk.web"},
    exclude = {AdkWebServer.class})
public class WebServerStatisticalAgent extends AdkWebServer {

  private static final Logger log = LoggerFactory.getLogger(WebServerStatisticalAgent.class);

  // Model name is configurable via GEMINI_MODEL environment variable
  // Default: gemini-2.5-flash
  private static final String MODEL =
      System.getenv().getOrDefault("GEMINI_MODEL", "gemini-2.5-flash");

  public static final BaseAgent STATISTICAL_AGENT =
      LlmAgent.builder()
          .name("statistical_analyst")
          .model(MODEL)
          .description("Agent that performs statistical analysis on datasets")
          .instruction(
              """
              You are a helpful statistical analyst agent.

              When the user provides a dataset (array of numbers), analyze it and provide:

              1. **Central Tendency**: Mean, Median, Mode
              2. **Dispersion**: Variance, Standard Deviation, Range
              3. **Outliers**: Identify outliers using z-score
              4. **Summary**: Brief interpretation

              Present results in a clear, well-formatted manner.
              """)
          .build();

  /**
   * Provides the DatabaseSessionService as a Spring-managed bean.
   *
   * <p><b>Note on Resource Management:</b> DatabaseSessionService implements AutoCloseable, but
   * when managed by Spring as a bean, Spring automatically handles the lifecycle. The
   * {@code @Bean(destroyMethod = "close")} annotation would explicitly configure this, but Spring
   * auto-detects AutoCloseable beans and calls close() during application shutdown.
   *
   * <p>For non-Spring applications, use try-with-resources pattern as shown in other tutorials.
   */
  @Override
  @Bean
  @Primary
  public BaseSessionService sessionService() {
    String dbUrl = System.getenv("DATABASE_URL");
    if (dbUrl == null || dbUrl.isEmpty()) {
      dbUrl = "jdbc:h2:./web_server_sessions";
      log.info("DATABASE_URL not set. Using H2: {}", dbUrl);
    } else {
      log.info("Using database: {}", dbUrl.replaceAll("password=[^&]*", "password=***"));
    }

    Map<String, Object> properties = new HashMap<>();
    properties.put("hibernate.show_sql", "false");

    log.info("Creating DatabaseSessionService (lifecycle managed by Spring)");
    return new DatabaseSessionService(dbUrl, properties);
  }

  public static void main(String[] args) {
    System.out.println("╔══════════════════════════════════════════════════════╗");
    System.out.println("║  Statistical Web Server - DatabaseSessionService    ║");
    System.out.println("╚══════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.println("🚀 Starting web server with persistent sessions...");
    System.out.println("🌐 Open: http://localhost:8080");
    System.out.println();

    AdkWebServer.start(STATISTICAL_AGENT);
  }
}
