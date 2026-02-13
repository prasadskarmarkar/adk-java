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

import com.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility to export DatabaseSessionService tables to CSV files.
 *
 * <p>This exports all ADK tables (adk_sessions, adk_events, adk_user_state, adk_app_state) to CSV
 * files for inspection and analysis.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@export-db
 * </pre>
 */
public class ExportDatabaseToCsv {

  private static final String DB_HOST = System.getenv().getOrDefault("DB_HOST", "localhost");
  private static final String DB_PORT = System.getenv().getOrDefault("DB_PORT", "5432");
  private static final String DB_NAME = System.getenv().getOrDefault("DB_NAME", "adk_test");
  private static final String DB_USER = System.getenv().getOrDefault("DB_USER", "adk_user");
  private static final String DB_PASSWORD =
      System.getenv().getOrDefault("DB_PASSWORD", "adk_password");
  private static final String DB_URL =
      String.format(
          "jdbc:postgresql://%s:%s/%s?user=%s&password=%s",
          DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD);
  private static final String OUTPUT_DIR = "./csv_exports";

  public static void main(String[] args) {
    System.out.println("=== Exporting Database to CSV ===");
    System.out.println("Database: " + DB_URL);
    System.out.println("Output Directory: " + OUTPUT_DIR);
    System.out.println();

    try (Connection conn = DriverManager.getConnection(DB_URL)) {
      DatabaseMetaData metaData = conn.getMetaData();
      ResultSet tables = metaData.getTables(null, null, "%", new String[] {"TABLE"});

      List<String> tableNames = new ArrayList<>();
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        // Skip Flyway migration tracking table
        if (!tableName.startsWith("flyway_")) {
          tableNames.add(tableName);
        }
      }

      System.out.println("Found " + tableNames.size() + " tables: " + tableNames);
      System.out.println();

      File outputDirFile = new File(OUTPUT_DIR);
      if (!outputDirFile.exists()) {
        outputDirFile.mkdirs();
      }

      List<String> exportedFiles = new ArrayList<>();
      for (String tableName : tableNames) {
        String csvPath = exportTableToCsv(conn, tableName, OUTPUT_DIR);
        exportedFiles.add(csvPath);
      }

      System.out.println();
      System.out.println("=== Export Complete ===");
      System.out.println("Exported files:");
      for (String filePath : exportedFiles) {
        System.out.println("  - " + filePath);
      }

    } catch (Exception e) {
      System.err.println("Error exporting database: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static String exportTableToCsv(Connection conn, String tableName, String outputDir)
      throws Exception {
    String csvPath = outputDir + "/" + tableName + ".csv";

    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        CSVWriter csvWriter = new CSVWriter(new FileWriter(csvPath))) {

      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      // Write headers
      String[] headers = new String[columnCount];
      for (int i = 1; i <= columnCount; i++) {
        headers[i - 1] = metaData.getColumnName(i);
      }
      csvWriter.writeNext(headers);

      // Write rows
      int rowCount = 0;
      while (rs.next()) {
        String[] row = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
          Object value = rs.getObject(i);
          row[i - 1] = value != null ? value.toString() : "";
        }
        csvWriter.writeNext(row);
        rowCount++;
      }

      System.out.println("✓ Exported " + rowCount + " rows from " + tableName + " to " + csvPath);
      return csvPath;
    }
  }
}
