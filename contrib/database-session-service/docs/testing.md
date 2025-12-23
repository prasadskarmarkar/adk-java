# Testing Guide for Database Session Service

This guide explains how to run tests for the Database Session Service module, including both unit tests and integration tests with PostgreSQL and MySQL.

## Table of Contents

- [Quick Start](#quick-start)
- [Unit Tests](#unit-tests)
- [Integration Tests](#integration-tests)
- [Database Setup](#database-setup)
- [Running Specific Tests](#running-specific-tests)
- [Troubleshooting](#troubleshooting)

## Quick Start

```bash
# Run all unit tests (no external dependencies required)
mvn test

# Run all tests including integration tests (requires databases)
mvn test -Dgroups=integration
```

## Unit Tests

Unit tests run without external dependencies like databases or external services. They use in-memory databases (H2) and mocked services.

### Running Unit Tests

```bash
# Run all unit tests in all modules
mvn test

# Run unit tests for a specific module
mvn test -pl core

# Run a specific test class
mvn test -Dtest=DatabaseSessionServiceTest

# Run a specific test method
mvn test -Dtest=DatabaseSessionServiceTest#testCreateSession
```

### Unit Test Categories

| Category | Example Tests | What They Test |
|----------|--------------|----------------|
| **Agent Logic** | `SequentialAgentTest`, `ParallelAgentTest` | Agent execution, workflow |
| **Session Storage** | `DatabaseSessionServiceTest` | Session CRUD with H2 |
| **Tools** | `FunctionToolTest`, `AgentToolTest` | Tool execution, validation |
| **Runner** | `RunnerTest` | Complete workflow with mocks |

## Integration Tests

Integration tests verify functionality with real external dependencies like MySQL and PostgreSQL databases. These tests are tagged with `@Tag("integration")` and are skipped by default.

### Prerequisites

Integration tests require Docker and docker-compose to be installed.

### Database Setup

#### Start All Test Databases

```bash
# Start both MySQL and PostgreSQL test databases
docker-compose -f scripts/docker-compose.test.yml up -d

# Verify databases are healthy
docker-compose -f scripts/docker-compose.test.yml ps
```

You should see:

```
NAME                 IMAGE         STATUS
adk-mysql-test       mysql:8.0     Up (healthy)
adk-postgres-test    postgres:16   Up (healthy)
```

#### Start Individual Databases

```bash
# Start only MySQL
docker-compose -f scripts/docker-compose.test.yml up -d mysql-test

# Start only PostgreSQL
docker-compose -f scripts/docker-compose.test.yml up -d postgres-test
```

#### Stop Databases

```bash
# Stop all test databases
docker-compose -f scripts/docker-compose.test.yml down

# Stop and remove volumes (clean slate)
docker-compose -f scripts/docker-compose.test.yml down -v
```

### Database Configuration

The integration tests use standardized database configurations defined in `TestDatabaseConfig.java`:

| Database | Host | Port | Database | User | Password |
|----------|------|------|----------|------|----------|
| MySQL | localhost | 3307 | adk_test | adk_user | adk_password |
| PostgreSQL | localhost | 5433 | adk_test | adk_user | adk_password |

> **Note**: Ports 3307 and 5433 are used to avoid conflicts with default MySQL (3306) and PostgreSQL (5432) installations.

### Running Integration Tests

```bash
# Run all integration tests (requires databases to be running)
mvn test -Dgroups=integration

# Run only MySQL integration tests
mvn test -Dtest=MySQL*IntegrationTest

# Run only PostgreSQL integration tests
mvn test -Dtest=PostgreSQL*IntegrationTest

# Run specific integration test
mvn test -Dtest=MySQLAgentIntegrationTest
```

### Integration Test Categories

| Test Suite | Database | What It Tests |
|------------|----------|--------------|
| **MySQLIntegrationTest** | MySQL 8.0 | Session CRUD, Flyway migrations, LONGTEXT storage |
| **PostgreSQLIntegrationTest** | PostgreSQL 16 | Session CRUD, Flyway migrations, JSONB storage |
| **MySQLAgentIntegrationTest** | MySQL 8.0 | Agent execution with database-backed sessions |
| **PostgreSQLAgentIntegrationTest** | PostgreSQL 16 | Agent execution with database-backed sessions |

### What If Databases Are Not Available?

Integration tests will be automatically **skipped** with a helpful message if the required database is not available:

```
MySQL test database not available. Start it with: docker-compose -f scripts/docker-compose.test.yml up -d mysql-test
```

This allows developers to run `mvn test` without errors even if databases are not running.

## Running Specific Tests

### By Module

```bash
# Run tests in core module only
mvn test -pl core

# Run tests in dev module only
mvn test -pl dev
```

### By Test Name Pattern

```bash
# Run all Agent tests
mvn test -Dtest=*Agent*Test

# Run all Session tests
mvn test -Dtest=*Session*Test

# Run all MySQL tests
mvn test -Dtest=MySQL*
```

### By Category/Tag

```bash
# Run only integration tests
mvn test -Dgroups=integration

# Run everything EXCEPT integration tests (default)
mvn test
```

## Troubleshooting

### Database Connection Issues

**Problem**: Tests fail with connection errors

**Solution**:
```bash
# Check if databases are running
docker-compose -f scripts/docker-compose.test.yml ps

# Check logs
docker-compose -f scripts/docker-compose.test.yml logs mysql-test
docker-compose -f scripts/docker-compose.test.yml logs postgres-test

# Restart databases
docker-compose -f scripts/docker-compose.test.yml restart
```

### Port Conflicts

**Problem**: Database won't start due to port already in use

**Solution**:
```bash
# Check what's using the port
lsof -i :3307  # MySQL
lsof -i :5433  # PostgreSQL

# Either stop the conflicting service or modify docker-compose.test.yml ports
```

### Schema Migration Issues

**Problem**: Flyway migration errors

**Solution**:
```bash
# Clean and recreate databases
docker-compose -f scripts/docker-compose.test.yml down -v
docker-compose -f scripts/docker-compose.test.yml up -d

# Wait for health checks
sleep 10
```

### Tests Hang or Timeout

**Problem**: Tests never complete

**Solution**:
```bash
# Check database health
docker-compose -f docker-compose.test.yml ps

# Increase Maven test timeout
mvn test -Dsurefire.timeout=300
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: '17'
      - name: Run unit tests
        run: mvn test

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: '17'
      - name: Start test databases
        run: docker-compose -f scripts/docker-compose.test.yml up -d
      - name: Wait for databases
        run: sleep 20
      - name: Run integration tests
        run: mvn test -Dgroups=integration
      - name: Stop databases
        run: docker-compose -f scripts/docker-compose.test.yml down
```

## Best Practices

1. **Always run unit tests first**: They're fast and catch most issues
   ```bash
   mvn test
   ```

2. **Run integration tests before committing**: Ensure database compatibility
   ```bash
   docker-compose -f scripts/docker-compose.test.yml up -d
   mvn test -Dgroups=integration
   ```

3. **Clean up after testing**: Stop databases to free resources
   ```bash
   docker-compose -f scripts/docker-compose.test.yml down
   ```

4. **Use test patterns**: Run relevant test subsets during development
   ```bash
   mvn test -Dtest=*Agent*Test
   ```

## Additional Resources

- [JUnit 5 Documentation](https://junit.org/junit5/docs/current/user-guide/)
- [Maven Surefire Plugin](https://maven.apache.org/surefire/maven-surefire-plugin/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Flyway Documentation](https://flywaydb.org/documentation/)
