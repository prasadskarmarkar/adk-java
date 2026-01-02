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
  --db TYPE               Database type: h2, postgres, mysql, or spanner (default: postgres)
  -d, --database URL      Set custom database URL (overrides --db)
  --validate              Run validation queries after tutorial completes
  --session-id ID         Use specific session ID instead of generating random UUID

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
  
  # Spanner emulator
  $0 --db spanner
  
  # Resume existing session
  $0 --session-id "existing-session-uuid"

ENVIRONMENT VARIABLES:
  GEMINI_API_KEY          Your Gemini API key (required unless --api-key is set)
  DATABASE_URL            Override default database URL

EOF
  exit 0
}

DB_TYPE="postgres"
VALIDATE=false
API_KEY="${GEMINI_API_KEY}"
DATABASE_URL=""
DATASET=""
SESSION_ID=""

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
    --validate)
      VALIDATE=true
      shift
      ;;
    --session-id)
      SESSION_ID="$2"
      shift 2
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

# Validate database type
if [[ ! "$DB_TYPE" =~ ^(h2|postgres|mysql|spanner)$ ]]; then
  echo "❌ Error: Unsupported database type: $DB_TYPE"
  echo "   Supported types: h2, postgres, mysql, spanner"
  exit 1
fi

if [ "$DB_TYPE" = "h2" ]; then
  echo "📦 Using H2 embedded database (file-based)"
  echo "   Location: ./stats_analysis_db.mv.db"
  DATABASE_URL=""
  echo ""
elif [ "$DB_TYPE" = "postgres" ]; then
  echo "📦 Connecting to PostgreSQL database..."
  echo ""
  
  if ! lsof -i :5432 > /dev/null 2>&1; then
    echo "❌ Error: PostgreSQL is not running on port 5432"
    echo "   Please start PostgreSQL and ensure it's listening on port 5432"
    exit 1
  fi
  
  echo "✓ PostgreSQL running on port 5432"
  
  if [ -z "$DATABASE_URL" ]; then
    DATABASE_URL="jdbc:postgresql://localhost:5432/adk_sessions?user=adk_user&password=adk_password"
  fi
  
  echo ""
  echo "📊 Database Info:"
  echo "  Type: PostgreSQL"
  echo "  Database: adk_sessions"
  echo "  User: adk_user"
  echo ""
elif [ "$DB_TYPE" = "mysql" ]; then
  echo "📦 Connecting to MySQL database..."
  echo ""
  
  if ! lsof -i :3306 > /dev/null 2>&1; then
    echo "❌ Error: MySQL is not running on port 3306"
    echo "   Please start MySQL and ensure it's listening on port 3306"
    exit 1
  fi
  
  echo "✓ MySQL running on port 3306"
  
  if [ -z "$DATABASE_URL" ]; then
    DATABASE_URL="jdbc:mysql://localhost:3306/adk_sessions?user=adk_user&password=adk_password"
  fi
  
  echo ""
  echo "📊 Database Info:"
  echo "  Type: MySQL"
  echo "  Database: adk_sessions"
  echo "  User: adk_user"
  echo ""
elif [ "$DB_TYPE" = "spanner" ]; then
  echo "📦 Connecting to Spanner emulator..."
  echo ""
  
  if ! lsof -i :9010 > /dev/null 2>&1; then
    echo "❌ Error: Spanner emulator is not running on port 9010"
    echo "   Please start Spanner emulator"
    echo "   Example: docker run -d -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator"
    exit 1
  fi
  
  echo "✓ Spanner emulator running on port 9010"
  
  if [ -z "$DATABASE_URL" ]; then
    DATABASE_URL="jdbc:cloudspanner://localhost:9010/projects/test-project/instances/test-instance/databases/adk-sessions?autoConfigEmulator=true"
  fi
  
  echo ""
  echo "📊 Database Info:"
  echo "  Type: Spanner (emulator)"
  echo "  Database: adk-sessions"
  echo "  Instance: test-instance"
  echo "  Project: test-project"
  echo ""
else
  echo "❌ Error: Unsupported database type: $DB_TYPE"
  echo "   Supported types: h2, postgres, mysql, spanner"
  exit 1
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

if [ -n "$SESSION_ID" ]; then
  echo "🔑 Session ID: $SESSION_ID"
  echo ""
fi

echo "🚀 Running tutorial..."
echo ""
cd "$SCRIPT_DIR"

EXEC_ARGS=""
if [ -n "$DATASET" ]; then
  EXEC_ARGS="\"$DATASET\""
fi
if [ -n "$SESSION_ID" ]; then
  EXEC_ARGS="$EXEC_ARGS --session-id \"$SESSION_ID\""
fi

if [ -n "$EXEC_ARGS" ]; then
  mvn compile exec:java -Dexec.args="$EXEC_ARGS"
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
    LATEST_SESSION=$(psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT id FROM sessions ORDER BY create_time DESC LIMIT 1;" 2>/dev/null | xargs)
    
    if [ -z "$LATEST_SESSION" ]; then
      echo "❌ No sessions found in database"
    else
      echo "📊 Validating Session: $LATEST_SESSION"
      echo ""
      
      # Get counts
      SESSION_COUNT=$(psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM sessions WHERE id = '$LATEST_SESSION';" 2>/dev/null | xargs)
      EVENT_COUNT=$(psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM events WHERE session_id = '$LATEST_SESSION';" 2>/dev/null | xargs)
      APP_STATE_COUNT=$(psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM app_states WHERE app_name = (SELECT app_name FROM sessions WHERE id = '$LATEST_SESSION');" 2>/dev/null | xargs)
      USER_STATE_COUNT=$(psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM user_states WHERE app_name = (SELECT app_name FROM sessions WHERE id = '$LATEST_SESSION') AND user_id = (SELECT user_id FROM sessions WHERE id = '$LATEST_SESSION');" 2>/dev/null | xargs)
      
      echo "📋 Summary:"
      echo "  Sessions:    $SESSION_COUNT"
      echo "  Events:      $EVENT_COUNT"
      echo "  App States:  $APP_STATE_COUNT"
      echo "  User States: $USER_STATE_COUNT"
      echo ""
      
      echo "📝 Events by Author:"
      psql -U "$DB_USER" -d "$DB_NAME" -c "
SELECT 
  event_data->>'author' as author,
  COUNT(*) as event_count
FROM events 
WHERE session_id = '$LATEST_SESSION'
GROUP BY event_data->>'author'
ORDER BY MIN(timestamp);
" 2>/dev/null
    fi
  elif [ "$DB_TYPE" = "mysql" ]; then
    # Detect if we need to use docker exec or direct mysql client
    MYSQL_CMD="mysql"
    if ! command -v mysql &> /dev/null; then
      # Try to find MySQL container
      MYSQL_CONTAINER=$(docker ps --filter "ancestor=mysql:8.0" --filter "status=running" --format "{{.Names}}" 2>/dev/null | head -n 1)
      if [ -z "$MYSQL_CONTAINER" ]; then
        echo "❌ MySQL client not found and no running MySQL container detected"
        echo "   Please install MySQL client or ensure MySQL Docker container is running"
      else
        MYSQL_CMD="docker exec $MYSQL_CONTAINER mysql"
        echo "ℹ️  Using MySQL via Docker container: $MYSQL_CONTAINER"
        echo ""
      fi
    fi
    
    LATEST_SESSION=$($MYSQL_CMD -u"$DB_USER" -padk_password -D"$DB_NAME" -N -e "SELECT id FROM sessions ORDER BY create_time DESC LIMIT 1;" 2>/dev/null)
    
    if [ -z "$LATEST_SESSION" ]; then
      echo "❌ No sessions found in database"
    else
      echo "📊 Validating Session: $LATEST_SESSION"
      echo ""
      
      # Get counts
      SESSION_COUNT=$($MYSQL_CMD -u"$DB_USER" -padk_password -D"$DB_NAME" -N -e "SELECT COUNT(*) FROM sessions WHERE id = '$LATEST_SESSION';" 2>/dev/null)
      EVENT_COUNT=$($MYSQL_CMD -u"$DB_USER" -padk_password -D"$DB_NAME" -N -e "SELECT COUNT(*) FROM events WHERE session_id = '$LATEST_SESSION';" 2>/dev/null)
      APP_STATE_COUNT=$($MYSQL_CMD -u"$DB_USER" -padk_password -D"$DB_NAME" -N -e "SELECT COUNT(*) FROM app_states WHERE app_name = (SELECT app_name FROM sessions WHERE id = '$LATEST_SESSION');" 2>/dev/null)
      USER_STATE_COUNT=$($MYSQL_CMD -u"$DB_USER" -padk_password -D"$DB_NAME" -N -e "SELECT COUNT(*) FROM user_states WHERE app_name = (SELECT app_name FROM sessions WHERE id = '$LATEST_SESSION') AND user_id = (SELECT user_id FROM sessions WHERE id = '$LATEST_SESSION');" 2>/dev/null)
      
      echo "📋 Summary:"
      echo "  Sessions:    $SESSION_COUNT"
      echo "  Events:      $EVENT_COUNT"
      echo "  App States:  $APP_STATE_COUNT"
      echo "  User States: $USER_STATE_COUNT"
      echo ""
      
      echo "📝 Events by Author:"
      $MYSQL_CMD -u"$DB_USER" -padk_password -D"$DB_NAME" -e "
SELECT 
  JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author')) as author,
  COUNT(*) as event_count
FROM events 
WHERE session_id = '$LATEST_SESSION'
GROUP BY JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))
ORDER BY MIN(timestamp);
" 2>&1 | grep -v "insecure"
    fi
  elif [ "$DB_TYPE" = "spanner" ]; then
    echo "⚠️  Spanner validation not yet implemented"
  fi
  
  echo ""
  echo "✅ Validation complete!"
  echo ""
fi
