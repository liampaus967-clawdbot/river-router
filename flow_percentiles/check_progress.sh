#!/bin/bash
# Check ETL progress

echo "=== ETL Progress ==="
echo ""

# Check if process is running
PID=$(pgrep -f "fetch_retrospective.py" | head -1)
if [ -n "$PID" ]; then
    echo "✅ Process running (PID: $PID)"
    ps -p $PID -o %mem,%cpu,etime --no-headers | awk '{print "   Memory: "$1"%, CPU: "$2"%, Runtime: "$3}'
else
    echo "❌ Process not running"
fi
echo ""

# Last few log lines
echo "=== Recent Log ==="
tail -10 ~/river-router-api/flow_percentiles/etl_vermont.log 2>/dev/null
echo ""

# Database row count
echo "=== Database ==="
PGPASSWORD=driftingInVermont psql -h driftwise-west.cfs02ime4lxt.us-west-2.rds.amazonaws.com -U postgres -d gisdata -t -c "
SELECT 
    'flow_history: ' || COUNT(*)::text || ' rows, ' || 
    COALESCE(MIN(date)::text, 'N/A') || ' to ' || COALESCE(MAX(date)::text, 'N/A')
FROM flow_history;
" 2>/dev/null

# Estimate progress
ROWS=$(PGPASSWORD=driftingInVermont psql -h driftwise-west.cfs02ime4lxt.us-west-2.rds.amazonaws.com -U postgres -d gisdata -t -c "SELECT COUNT(*) FROM flow_history;" 2>/dev/null | tr -d ' ')
TARGET=50500000  # ~51M expected
if [ -n "$ROWS" ] && [ "$ROWS" -gt 0 ]; then
    PCT=$((ROWS * 100 / TARGET))
    echo "   Progress: ~${PCT}% (${ROWS} / ~51M rows)"
fi
