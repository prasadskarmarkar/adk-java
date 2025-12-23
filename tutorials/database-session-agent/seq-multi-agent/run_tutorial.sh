#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

show_usage() {
  cat << EOF
Usage: $0 [OPTIONS] [DATASET]

Run the Sequential Pipeline tutorial with DatabaseSessionService.

OPTIONS:
  -h, --help              Show this help message
  -k, --api-key KEY       Set Gemini API key (default: from GEMINI_API_KEY env var)
  --db TYPE               Database type: h2, postgres, or mysql (default: postgres)
  -d, --database URL      Set custom database URL (overrides --db)
  --skip-docker           Skip Docker setup (use existing database)
  --validate              Run validation queries after tutorial completes

DATASET:
  Comma-separated numbers in brackets (default: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
  
EXAMPLES:
  # Default dataset
  $0
  
  # Custom dataset
  $0 "[10, 20, 30, 40, 50]"
  
  # With H2 database
  $0 --db h2
  
  # With PostgreSQL and validation
  $0 --db postgres --validate
  
  # Custom dataset with outliers
  $0 "[10, 12, 11, 13, 12, 11, 200, 10, 12]"
  
  # MySQL with custom dataset
  $0 --db mysql "[5, 10, 15, 20, 25, 100]"

ENVIRONMENT VARIABLES:
  GEMINI_API_KEY          Your Gemini API key (required unless --api-key is set)
  DATABASE_URL            Override default database URL

EOF
  exit 0
}

SKIP_DOCKER=false
DB_TYPE="postgres"
VALIDATE=false
API_KEY="${GEMINI_API_KEY}"
DATABASE_URL=""
DATASET=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      show_usage
      ;;
    -k|--api-key)
      API_KEY="$2"
      shift 2
      ;;
    -d|--database)
      DATABASE_URL="$2"
      shift 2
      ;;
    --db)
      DB_TYPE="$2"
      shift 2
      ;;
    --skip-docker)
      SKIP_DOCKER=true
      shift
      ;;
    --validate)
      VALIDATE=true
      shift
      ;;
    *)
      DATASET="$1"
      shift
      ;;
  esac
done

echo "╔════════════════════════════════════════════════════════════╗"
echo "║   Statistical Analysis Pipeline with DatabaseSessionService║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

if [ -z "$API_KEY" ]; then
  echo "❌ Error: GEMINI_API_KEY not set"
  echo "   Set it via environment variable or use --api-key flag"
  echo "   Example: export GEMINI_API_KEY=your_key_here"
  exit 1
fi

if [ "$DB_TYPE" = "h2" ]; then
  echo "📦 Using H2 embedded database (file-based)"
  echo "   Location: ./stats_analysis_db.mv.db"
  DATABASE_URL=""
  echo ""
elif [ "$SKIP_DOCKER" = false ]; then
  if [ "$DB_TYPE" = "postgres" ]; then
    echo "📦 Setting up PostgreSQL database..."
    echo ""
    CONTAINER_NAME="postgres-adk"
    
    if ! docker ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
      echo "Creating new PostgreSQL container with persistent volume..."
      docker run -d \
        --name $CONTAINER_NAME \
        -e POSTGRES_DB=adk_sessions \
        -e POSTGRES_USER=adk_user \
        -e POSTGRES_PASSWORD=adk_password \
        -p 5432:5432 \
        -v postgres-adk-data:/var/lib/postgresql/data \
        postgres:latest
      
      echo "Waiting for PostgreSQL to start..."
      sleep 5
      echo "✓ PostgreSQL container created with persistent volume"
    else
      if ! docker ps --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        echo "Starting existing PostgreSQL container..."
        docker start $CONTAINER_NAME
        sleep 3
        echo "✓ PostgreSQL container started"
      else
        echo "✓ PostgreSQL container already running"
      fi
    fi
    
    if [ -z "$DATABASE_URL" ]; then
      DATABASE_URL="jdbc:postgresql://localhost:5432/adk_sessions?user=adk_user&password=adk_password"
    fi
    
    echo ""
    echo "📊 Database Info:"
    echo "  Type: PostgreSQL"
    echo "  Container: $CONTAINER_NAME"
    echo "  Database: adk_sessions"
    echo "  User: adk_user"
    echo "  Volume: postgres-adk-data (persistent)"
    echo ""
  elif [ "$DB_TYPE" = "mysql" ]; then
    echo "📦 Setting up MySQL database..."
    echo ""
    CONTAINER_NAME="mysql-adk"
    
    if ! docker ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
      echo "Creating new MySQL container with persistent volume..."
      docker run -d \
        --name $CONTAINER_NAME \
        -e MYSQL_ROOT_PASSWORD=root_password \
        -e MYSQL_DATABASE=adk_sessions \
        -e MYSQL_USER=adk_user \
        -e MYSQL_PASSWORD=adk_password \
        -p 3306:3306 \
        -v mysql-adk-data:/var/lib/mysql \
        mysql:8.0
      
      echo "Waiting for MySQL to start..."
      sleep 10
      echo "✓ MySQL container created with persistent volume"
    else
      if ! docker ps --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        echo "Starting existing MySQL container..."
        docker start $CONTAINER_NAME
        sleep 5
        echo "✓ MySQL container started"
      else
        echo "✓ MySQL container already running"
      fi
    fi
    
    if [ -z "$DATABASE_URL" ]; then
      DATABASE_URL="jdbc:mysql://localhost:3306/adk_sessions?user=adk_user&password=adk_password"
    fi
    
    echo ""
    echo "📊 Database Info:"
    echo "  Type: MySQL"
    echo "  Container: $CONTAINER_NAME"
    echo "  Database: adk_sessions"
    echo "  User: adk_user"
    echo "  Volume: mysql-adk-data (persistent)"
    echo ""
  else
    echo "❌ Error: Unsupported database type: $DB_TYPE"
    echo "   Supported types: h2, postgres, mysql"
    exit 1
  fi
fi

echo "🔧 Configuring environment..."
unset GOOGLE_GENAI_USE_VERTEXAI
unset GOOGLE_CLOUD_PROJECT
unset GOOGLE_APPLICATION_CREDENTIALS
export GEMINI_API_KEY="$API_KEY"
if [ -n "$DATABASE_URL" ]; then
  export DATABASE_URL="$DATABASE_URL"
fi
echo "✓ Environment configured"
echo ""

if [ -n "$DATASET" ]; then
  echo "📊 Dataset: $DATASET"
  echo ""
fi

echo "🚀 Running tutorial..."
echo ""
cd "$SCRIPT_DIR"

if [ -n "$DATASET" ]; then
  mvn compile exec:java -Dexec.args="\"$DATASET\""
else
  mvn compile exec:java
fi

if [ "$VALIDATE" = true ]; then
  echo ""
  echo "╔════════════════════════════════════════════════════════════╗"
  echo "║  Validating Results                                        ║"
  echo "╚════════════════════════════════════════════════════════════╝"
  echo ""
  
  DB_USER="adk_user"
  DB_NAME="adk_sessions"
  
  if [ "$DB_TYPE" = "h2" ]; then
    echo "⚠️  H2 validation requires H2 Console (see README.md)"
    echo "   Database file: ./stats_analysis_db.mv.db"
    echo "   Connect with: jdbc:h2:./stats_analysis_db"
  elif [ "$DB_TYPE" = "postgres" ]; then
    echo "📊 Recent Sessions (PostgreSQL):"
    psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT id, app_name, user_id, create_time FROM sessions ORDER BY create_time DESC LIMIT 3;" 2>/dev/null || echo "   Unable to connect to PostgreSQL"
    
    echo ""
    echo "📝 Event Counts by Session:"
    psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT session_id, COUNT(*) as total_events FROM events GROUP BY session_id ORDER BY MAX(timestamp) DESC LIMIT 3;" 2>/dev/null
    
    echo ""
    echo "🎯 Latest Analysis Results:"
    psql -U "$DB_USER" -d "$DB_NAME" -c "
SELECT 
  event_data->>'author' as author,
  to_char(timestamp, 'HH24:MI:SS') as time,
  event_data->'content'->'parts'->0->>'text' as message
FROM events 
WHERE session_id = (SELECT id FROM sessions ORDER BY create_time DESC LIMIT 1)
ORDER BY timestamp;" 2>/dev/null
  elif [ "$DB_TYPE" = "mysql" ]; then
    echo "📊 Recent Sessions (MySQL):"
    docker exec mysql-adk mysql -u"$DB_USER" -padk_password -D"$DB_NAME" -e "SELECT id, app_name, user_id, create_time FROM sessions ORDER BY create_time DESC LIMIT 3;" 2>/dev/null || echo "   Unable to connect to MySQL"
    
    echo ""
    echo "📝 Event Counts by Session:"
    docker exec mysql-adk mysql -u"$DB_USER" -padk_password -D"$DB_NAME" -e "SELECT session_id, COUNT(*) as total_events FROM events GROUP BY session_id ORDER BY MAX(timestamp) DESC LIMIT 3;" 2>/dev/null
    
    echo ""
    echo "🎯 Latest Analysis Results:"
    docker exec mysql-adk mysql -u"$DB_USER" -padk_password -D"$DB_NAME" -e "
SELECT 
  JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author')) as author,
  DATE_FORMAT(timestamp, '%H:%i:%s') as time,
  JSON_PRETTY(event_data) as event_data
FROM events 
WHERE session_id = (SELECT id FROM sessions ORDER BY create_time DESC LIMIT 1)
ORDER BY timestamp;" 2>/dev/null
  fi
  
  echo ""
  echo "✅ Validation complete!"
  echo ""
fi
