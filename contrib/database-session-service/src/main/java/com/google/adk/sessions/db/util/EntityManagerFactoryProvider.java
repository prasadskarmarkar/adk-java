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
package com.google.adk.sessions.db.util;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.PersistenceException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider for creating an EntityManagerFactory for the DatabaseSessionService.
 *
 * <p><b>Important:</b> Each call creates a NEW EntityManagerFactory with its own connection pool.
 * For optimal performance, create ONE {@link com.google.adk.sessions.DatabaseSessionService
 * DatabaseSessionService} per database and reuse it throughout your application lifecycle.
 *
 * <p><b>Best Practice:</b>
 *
 * <pre>{@code
 * // Good: Create once, reuse everywhere
 * try (DatabaseSessionService service = new DatabaseSessionService(dbUrl)) {
 *   Runner runner1 = new Runner(agent1, appName, artifacts, service, memory);
 *   Runner runner2 = new Runner(agent2, appName, artifacts, service, memory);
 *   // Both runners share the same connection pool
 * }
 *
 * // Bad: Creates multiple connection pools unnecessarily
 * DatabaseSessionService service1 = new DatabaseSessionService(dbUrl); // Pool #1
 * DatabaseSessionService service2 = new DatabaseSessionService(dbUrl); // Pool #2 - wasteful!
 * }</pre>
 *
 * <p><b>Multiple Databases:</b> Different databases need different services:
 *
 * <pre>{@code
 * try (DatabaseSessionService postgresService = new DatabaseSessionService(postgresUrl);
 *      DatabaseSessionService mysqlService = new DatabaseSessionService(mysqlUrl)) {
 *   // Each has its own connection pool - this is correct
 * }
 * }</pre>
 *
 * <p><b>Configuration Precedence:</b> Properties are merged in this order (highest to lowest):
 *
 * <ol>
 *   <li>Runtime properties passed to DatabaseSessionService constructor (highest priority)
 *   <li>META-INF/persistence.xml properties (lower priority)
 *   <li>Hibernate defaults (lowest priority)
 * </ol>
 */
public class EntityManagerFactoryProvider {

  private static final Logger logger = LoggerFactory.getLogger(EntityManagerFactoryProvider.class);
  private static final String PERSISTENCE_UNIT_NAME = "adk-sessions";

  /**
   * Creates an EntityManagerFactory with the specified database URL and configuration.
   *
   * <p>This method loads configuration from META-INF/persistence.xml and merges it with the
   * provided properties. Runtime properties override persistence.xml values.
   *
   * @param dbUrl The database URL to connect to (must not be null)
   * @param properties Additional properties for the EntityManagerFactory (must not be null, but can
   *     be empty)
   * @return A configured EntityManagerFactory
   * @throws NullPointerException if dbUrl or properties is null
   * @throws PersistenceException if the persistence unit cannot be created
   */
  public static EntityManagerFactory createEntityManagerFactory(
      String dbUrl, Map<String, Object> properties) {

    Objects.requireNonNull(dbUrl, "dbUrl cannot be null");
    Objects.requireNonNull(properties, "properties cannot be null");

    Map<String, Object> config = new HashMap<>(properties);

    // Set JDBC URL if not already provided
    if (!config.containsKey("jakarta.persistence.jdbc.url")) {
      config.put("jakarta.persistence.jdbc.url", dbUrl);
    }

    // Detect and set database dialect if not already provided
    if (!config.containsKey("hibernate.dialect")) {
      String dialect = DatabaseDialectDetector.detectDialect(dbUrl);
      config.put("hibernate.dialect", dialect);
      logger.debug("Auto-detected database dialect: {}", dialect);
    }

    // Create the EntityManagerFactory
    // All other defaults (connection pool, timeouts, isolation level, etc.) are configured in
    // META-INF/persistence.xml
    logger.debug(
        "Creating EntityManagerFactory for persistence unit '{}' with JDBC URL: {}",
        PERSISTENCE_UNIT_NAME,
        dbUrl);

    EntityManagerFactory emf =
        Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME, config);

    logger.info("EntityManagerFactory created successfully for database: {}", dbUrl);

    return emf;
  }
}
