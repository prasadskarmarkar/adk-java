## Database Session Service for ADK

This module provides a production-ready database-backed session service for the ADK (Agent Development Kit) that uses JPA/Hibernate, HikariCP connection pooling, and Flyway migrations for persistent storage of session data.

## Features

- **Persistent Storage**: Store sessions, events, and state in relational databases
- **Multi-Database Support**: PostgreSQL, MySQL, H2, SQLite, and other JDBC-compliant databases
- **Automatic Migrations**: Flyway database schema versioning and zero-downtime deployments
- **High Performance**: HikariCP connection pooling for optimal database performance
- **Thread-Safe**: Pessimistic locking for concurrent session updates
- **Dialect-Aware**: Optimized JSON storage (JSONB for PostgreSQL, CLOB for others)
- **Event Filtering**: Efficient pagination and filtering of session events

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependencies>
    <!-- ADK Core -->
    <dependency>
        <groupId>com.google.adk</groupId>
        <artifactId>google-adk</artifactId>
        <version>0.4.1-SNAPSHOT</version>
    </dependency>
    
    <!-- Database Session Service -->
    <dependency>
        <groupId>com.google.adk</groupId>
        <artifactId>google-adk-database-session-service</artifactId>
        <version>0.4.1-SNAPSHOT</version>
    </dependency>
    
    <!-- Add your database driver -->
    <!-- For PostgreSQL: -->
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>42.7.2</version>
    </dependency>
    
    <!-- For MySQL: -->
    <!--
    <dependency>
        <groupId>com.mysql</groupId>
        <artifactId>mysql-connector-j</artifactId>
        <version>8.3.0</version>
    </dependency>
    -->
</dependencies>
```

### Gradle

```gradle
dependencies {
    // ADK Core
    implementation 'com.google.adk:google-adk:0.4.1-SNAPSHOT'
    
    // Database Session Service
    implementation 'com.google.adk:google-adk-database-session-service:0.4.1-SNAPSHOT'
    
    // Add your database driver
    // For PostgreSQL:
    implementation 'org.postgresql:postgresql:42.7.2'
    
    // For MySQL:
    // implementation 'com.mysql:mysql-connector-j:8.3.0'
}
```

## Usage

### Basic Setup

```java
import com.google.adk.sessions.DatabaseSessionService;
import com.google.adk.runner.Runner;
import com.google.adk.artifacts.InMemoryArtifactService;
import com.google.adk.memory.InMemoryMemoryService;

public class MyApplication {
    public static void main(String[] args) {
        // Database URL with credentials
        String dbUrl = System.getenv("DATABASE_URL");
        // Example: jdbc:postgresql://localhost:5432/adk_sessions?user=myuser&password=mypass
        
        DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl);
        
        Runner runner = new Runner(
            myAgent,
            "my-app",
            new InMemoryArtifactService(),
            sessionService,
            new InMemoryMemoryService()
        );
        
        // Use the runner...
    }
}
```

## Resource Management

**Important:** `DatabaseSessionService` implements `AutoCloseable` and **must** be closed when no longer needed to release database connection pools.

### Try-With-Resources (Recommended)

```java
try (DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl)) {
    Runner runner = new Runner(
        myAgent,
        "my-app",
        new InMemoryArtifactService(),
        sessionService,
        new InMemoryMemoryService()
    );
    
    // use runner...
} // sessionService automatically closed here
```

### Spring Framework Integration

```java
@Configuration
public class AdkConfig {
    
    @Bean(destroyMethod = "close")
    public DatabaseSessionService sessionService() {
        String dbUrl = System.getenv("DATABASE_URL");
        return new DatabaseSessionService(dbUrl);
    }
}
```

### Manual Management

If you cannot use try-with-resources, ensure `close()` is called:

```java
DatabaseSessionService sessionService = null;
try {
    sessionService = new DatabaseSessionService(dbUrl);
    // use service
} finally {
    if (sessionService != null) {
        sessionService.close();
    }
}
```

### What Happens If You Don't Close?

- Database connection pool remains open
- JVM may not exit cleanly
- Connection leaks in long-running applications
- After calling `close()`, all operations throw `IllegalStateException`

### Database URL Formats

#### PostgreSQL
```
jdbc:postgresql://host:5432/dbname?user=username&password=pass&ssl=true&sslmode=verify-full
```

#### MySQL
```
jdbc:mysql://host:3306/dbname?user=username&password=pass&useSSL=true&requireSSL=true
```

#### H2 (in-memory, for testing)
```
jdbc:h2:mem:testdb
```

#### SQLite
```
jdbc:sqlite:./sessions.db
```

### Advanced Configuration

You can pass additional Hibernate configuration properties:

```java
Map<String, Object> config = new HashMap<>();
config.put("hibernate.show_sql", "true");
config.put("hibernate.format_sql", "true");
config.put("db.username", "myuser");
config.put("db.password", "mypass");

DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl, config);
```

### Environment Variables

For production deployments, load database credentials from environment variables:

```java
String dbUrl = System.getenv("DATABASE_URL");
DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl);
```

## Database Schema

The service automatically creates and migrates the following tables using Flyway:

- `sessions` - Session metadata and state
- `events` - Session events (agent responses, tool calls, etc.)
- `app_state` - Application-level state (per app)
- `user_state` - User-level state (per user/app)

Schema migrations are applied automatically on first connection.

## Migration Management

### Baseline Existing Database

If you have an existing database, you can baseline it to skip initial migrations:

```bash
export FLYWAY_BASELINE_ON_MIGRATE=true
```

Or via system property:
```bash
java -DFLYWAY_BASELINE_ON_MIGRATE=true -jar myapp.jar
```

### Multi-Pod Deployments

The service handles concurrent migration attempts gracefully with lock retry logic. Only one pod will execute migrations while others wait and validate.

## Performance Tuning

### HikariCP Connection Pool

Adjust pool settings via Hibernate properties:

```java
config.put("hibernate.hikari.maximumPoolSize", "20");
config.put("hibernate.hikari.minimumIdle", "5");
config.put("hibernate.hikari.connectionTimeout", "30000");
```

### Query Optimization

The service uses pessimistic locking for concurrent updates. For read-heavy workloads, consider using read replicas or caching.

## Testing

See [docs/testing.md](docs/testing.md) for comprehensive testing guide.

### Unit Tests (H2 In-Memory)

For unit tests, use H2 in-memory database:

```java
String testDbUrl = "jdbc:h2:mem:testdb";
DatabaseSessionService sessionService = new DatabaseSessionService(testDbUrl);
```

### Integration Tests (PostgreSQL/MySQL)

Integration tests verify DatabaseSessionService with real PostgreSQL and MySQL databases.

#### Prerequisites

Start the test databases using Docker:

```bash
docker-compose -f scripts/docker-compose.test.yml up -d
```

This starts:
- **PostgreSQL 16** on `localhost:5433`
- **MySQL 8.0** on `localhost:3307`

#### Running Tests

```bash
# Run all integration tests
mvn test -Dgroups=integration

# Run specific database tests
mvn test -Dtest=PostgreSQLIntegrationTest
mvn test -Dtest=MySQLIntegrationTest
mvn test -Dtest=PostgreSQLAgentIntegrationTest
mvn test -Dtest=MySQLAgentIntegrationTest
```

Tests are automatically skipped if databases aren't running (using `assumeTrue()`).

#### Cleanup

```bash
docker-compose -f scripts/docker-compose.test.yml down
```

## Troubleshooting

### Migration Lock Issues

If migrations fail with lock errors in multi-pod scenarios:
1. The service automatically retries and validates
2. If persistent, manually release Flyway lock:
   ```sql
   DELETE FROM flyway_schema_history WHERE success = false;
   ```

### Connection Pool Exhausted

Increase pool size:
```java
config.put("hibernate.hikari.maximumPoolSize", "30");
```

### Slow Queries

Enable SQL logging to identify slow queries:
```java
config.put("hibernate.show_sql", "true");
config.put("hibernate.format_sql", "true");
```

## Comparison with InMemorySessionService

| Feature | InMemorySessionService | DatabaseSessionService |
|---------|------------------------|------------------------|
| Persistence | ❌ | ✅ |
| Multi-node support | ❌ | ✅ |
| Horizontal scaling | ❌ | ✅ |
| Production-ready | ❌ | ✅ |
| Setup complexity | Low | Medium |
| Performance | Very fast | Fast (with connection pooling) |

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](../../LICENSE) file for details.
