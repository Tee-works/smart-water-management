#!/bin/bash

# Complete setup script for Smart Water Management with all enhancements
set -e

echo "🚀 Smart Water Management - Complete Setup"
echo "=========================================="


# Create the create-multiple-dbs.sh script for Postgres
echo "📝 Creating database initialization script..."
cat > scripts/create-multiple-dbs.sh << 'EOF'
#!/bin/bash
set -e
set -u

function create_user_and_database() {
    local database=$1
    echo "  Creating database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_user_and_database $db
    done
    echo "Multiple databases created"
fi
EOF
chmod +x scripts/create-multiple-dbs.sh

# Create Airflow connections initialization
echo "📝 Creating Airflow connections script..."
cat > dags/connections_setup.py << 'EOF'
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'setup_connections',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['setup']
) as dag:
    
    create_postgres_conn = BashOperator(
        task_id='create_postgres_connection',
        bash_command="""
        airflow connections add 'water_postgres' \
            --conn-type 'postgres' \
            --conn-host 'postgres' \
            --conn-schema 'water_analytics' \
            --conn-login 'dataeng' \
            --conn-password 'pipeline123' \
            --conn-port 5432
        """
    )
    
    create_s3_conn = BashOperator(
        task_id='create_s3_connection',
        bash_command="""
        airflow connections add 'minio_s3' \
            --conn-type 's3' \
            --conn-extra '{
                "aws_access_key_id": "minioadmin",
                "aws_secret_access_key": "minioadmin123",
                "host": "http://minio:9000"
            }'
        """
    )
EOF

# Copy DAGs to the dags directory
echo "📝 Setting up Airflow DAGs..."
cp src/*_dag.py dags/ 2>/dev/null || echo "No DAG files found in src/"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << 'EOF'
# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin123

# Database
DB_HOST=localhost
DB_PORT=5433
DB_NAME=water_analytics
DB_USER=dataeng
DB_PASSWORD=pipeline123

# API Keys
OPENWEATHER_API_KEY=your_api_key_here

# AWS (optional)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
EOF
fi

# Create a comprehensive startup script
echo "📝 Creating startup script..."
cat > start_services.sh << 'EOF'
#!/bin/bash

echo "🚀 Starting Smart Water Management Services"
echo "=========================================="

# Function to check if service is ready
check_service() {
    local service=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    echo -n "Waiting for $service..."
    while ! nc -z localhost $port 2>/dev/null; do
        if [ $attempt -eq $max_attempts ]; then
            echo " ❌ Failed to start"
            return 1
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    echo " ✅ Ready"
    return 0
}

# Start base services
echo "1️⃣ Starting base services..."
docker-compose up -d postgres redis minio

# Wait for services
check_service "PostgreSQL" 5433
check_service "Redis" 6380
check_service "MinIO" 9000

# Start visualization services
echo "2️⃣ Starting visualization services..."
docker-compose up -d pgadmin grafana jupyter

# Start Airflow (separate compose)
echo "3️⃣ Starting Airflow..."
docker-compose -f docker-compose.airflow.yml up -d

# Wait for Airflow
check_service "Airflow" 8080

echo ""
echo "✅ All services started!"
echo ""
echo "🌐 Access URLs:"
echo "  - Dashboard: http://localhost:5000"
echo "  - Airflow: http://localhost:8080 (admin/admin123)"
echo "  - pgAdmin: http://localhost:5050 (admin@waterproject.com/admin123)"
echo "  - Grafana: http://localhost:3000 (admin/admin123)"
echo "  - MinIO: http://localhost:9001 (minioadmin/minioadmin123)"
echo "  - Jupyter: http://localhost:8888"
echo ""
echo "📊 To view logs: docker-compose logs -f [service-name]"
echo "🛑 To stop all: docker-compose down && docker-compose -f docker-compose.airflow.yml down"
EOF
chmod +x start_services.sh

# Create data pipeline runner
echo "📝 Creating data pipeline runner..."
cat > run_pipeline.sh << 'EOF'
#!/bin/bash

echo "🌊 Running Water Management Data Pipeline"
echo "========================================"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install requirements if needed
pip install -r requirements.txt -q

# Run pipeline steps
echo ""
echo "1️⃣ Running enhanced data ingestion..."
python src/enhanced_production_ingestion.py

echo ""
echo "2️⃣ Loading to data warehouse..."
python src/enhanced_warehouse_loader.py

echo ""
echo "3️⃣ Running Spark analytics..."
python src/enhanced_spark_etl.py

echo ""
echo "4️⃣ Starting dashboard..."
python src/app.py &
DASHBOARD_PID=$!

echo ""
echo "✅ Pipeline complete!"
echo "📊 Dashboard running at http://localhost:5000 (PID: $DASHBOARD_PID)"
echo "Press Ctrl+C to stop the dashboard"

# Wait for interrupt
wait $DASHBOARD_PID
EOF
chmod +x run_pipeline.sh

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Update .env with your API keys"
echo "2. Start all services: ./start_services.sh"
echo "3. Run the pipeline: ./run_pipeline.sh"
echo ""
echo "🚀 For production deployment:"
echo "- Services will auto-restart on failure"
echo "- Airflow will handle scheduled ingestion"
echo "- Grafana dashboards for monitoring"
echo "- pgAdmin for database management"
