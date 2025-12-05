#!/usr/bin/env python3
"""
Monitoring and Metrics Collection for ETL Pipeline
Tracks performance, errors, and data quality metrics
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class ETLMonitor:
    """
    Monitors ETL pipeline execution and collects metrics
    """
    
    def __init__(self, db_path: str = "etl_metrics.db"):
        """Initialize the monitor with a metrics database"""
        self.db_path = db_path
        self._init_metrics_db()
    
    def _init_metrics_db(self):
        """Initialize the metrics database"""
        conn = sqlite3.connect(self.db_path)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etl_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT NOT NULL,
                end_time TEXT,
                duration_seconds REAL,
                status TEXT,
                error_message TEXT,
                movies_processed INTEGER DEFAULT 0,
                movies_inserted INTEGER DEFAULT 0,
                movies_updated INTEGER DEFAULT 0,
                movies_skipped INTEGER DEFAULT 0,
                shows_processed INTEGER DEFAULT 0,
                shows_inserted INTEGER DEFAULT 0,
                shows_updated INTEGER DEFAULT 0,
                shows_skipped INTEGER DEFAULT 0,
                api_calls INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etl_errors (
                error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                timestamp TEXT NOT NULL,
                error_type TEXT,
                error_message TEXT,
                traceback TEXT,
                FOREIGN KEY (run_id) REFERENCES etl_runs(run_id)
            )
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_etl_runs_start_time 
            ON etl_runs(start_time)
        """)
        
        conn.commit()
        conn.close()
    
    def start_run(self) -> int:
        """Record the start of an ETL run"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "INSERT INTO etl_runs (start_time, status) VALUES (?, ?)",
            (datetime.now().isoformat(), 'running')
        )
        run_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return run_id
    
    def end_run(self, run_id: int, stats: Dict, status: str = 'success', 
                error_message: Optional[str] = None):
        """Record the completion of an ETL run"""
        conn = sqlite3.connect(self.db_path)
        
        # Get start time to calculate duration
        row = conn.execute(
            "SELECT start_time FROM etl_runs WHERE run_id = ?",
            (run_id,)
        ).fetchone()
        
        if row:
            start_time = datetime.fromisoformat(row[0])
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
        else:
            duration = None
            end_time = datetime.now()
        
        conn.execute("""
            UPDATE etl_runs SET
                end_time = ?,
                duration_seconds = ?,
                status = ?,
                error_message = ?,
                movies_processed = ?,
                movies_inserted = ?,
                movies_updated = ?,
                movies_skipped = ?,
                shows_processed = ?,
                shows_inserted = ?,
                shows_updated = ?,
                shows_skipped = ?,
                api_calls = ?,
                errors = ?
            WHERE run_id = ?
        """, (
            end_time.isoformat(),
            duration,
            status,
            error_message,
            stats.get('movies_processed', 0),
            stats.get('movies_inserted', 0),
            stats.get('movies_updated', 0),
            stats.get('movies_skipped', 0),
            stats.get('shows_processed', 0),
            stats.get('shows_inserted', 0),
            stats.get('shows_updated', 0),
            stats.get('shows_skipped', 0),
            stats.get('api_calls', 0),
            stats.get('errors', 0),
            run_id
        ))
        
        conn.commit()
        conn.close()
    
    def log_error(self, run_id: int, error_type: str, error_message: str, 
                  traceback: Optional[str] = None):
        """Log an error that occurred during ETL"""
        conn = sqlite3.connect(self.db_path)
        
        conn.execute("""
            INSERT INTO etl_errors (run_id, timestamp, error_type, error_message, traceback)
            VALUES (?, ?, ?, ?, ?)
        """, (
            run_id,
            datetime.now().isoformat(),
            error_type,
            error_message,
            traceback
        ))
        
        conn.commit()
        conn.close()
    
    def get_recent_runs(self, limit: int = 10) -> List[Dict]:
        """Get recent ETL runs"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        rows = conn.execute("""
            SELECT * FROM etl_runs
            ORDER BY start_time DESC
            LIMIT ?
        """, (limit,)).fetchall()
        
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_statistics(self, days: int = 7) -> Dict:
        """Get aggregate statistics for recent runs"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        row = conn.execute("""
            SELECT
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                AVG(duration_seconds) as avg_duration,
                SUM(movies_processed) as total_movies_processed,
                SUM(shows_processed) as total_shows_processed,
                SUM(api_calls) as total_api_calls,
                SUM(errors) as total_errors
            FROM etl_runs
            WHERE start_time >= ?
        """, (cutoff,)).fetchone()
        
        conn.close()
        
        if row:
            return dict(row)
        return {}
    
    def get_error_summary(self, days: int = 7) -> List[Dict]:
        """Get summary of recent errors"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        rows = conn.execute("""
            SELECT
                error_type,
                COUNT(*) as count,
                MAX(timestamp) as last_occurrence
            FROM etl_errors
            WHERE timestamp >= ?
            GROUP BY error_type
            ORDER BY count DESC
        """, (cutoff,)).fetchall()
        
        conn.close()
        
        return [dict(row) for row in rows]
    
    def export_metrics(self, output_file: str = "etl_metrics.json"):
        """Export metrics to JSON file"""
        metrics = {
            'recent_runs': self.get_recent_runs(20),
            'statistics_7d': self.get_statistics(7),
            'statistics_30d': self.get_statistics(30),
            'error_summary': self.get_error_summary(7),
            'exported_at': datetime.now().isoformat()
        }
        
        with open(output_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        return output_file


def generate_report(monitor: ETLMonitor, output_file: str = "etl_report.html"):
    """Generate an HTML report of ETL metrics"""
    recent_runs = monitor.get_recent_runs(10)
    stats_7d = monitor.get_statistics(7)
    stats_30d = monitor.get_statistics(30)
    errors = monitor.get_error_summary(7)
    
    # Build table rows separately to avoid nested f-string issues
    runs_rows = []
    for run in recent_runs:
        run_id = run.get("run_id", "")
        start_time = run.get("start_time", "")[:19] if run.get("start_time") else ""
        duration = f"{run.get('duration_seconds', 0):.1f}s"
        status = run.get("status", "")
        status_class = f"status-{status}"
        movies = run.get("movies_processed", 0)
        shows = run.get("shows_processed", 0)
        api_calls = run.get("api_calls", 0)
        runs_rows.append(f'''
                <tr>
                    <td>{run_id}</td>
                    <td>{start_time}</td>
                    <td>{duration}</td>
                    <td class="{status_class}">{status.upper()}</td>
                    <td>{movies}</td>
                    <td>{shows}</td>
                    <td>{api_calls}</td>
                </tr>''')
    
    error_rows = []
    for error in errors:
        error_type = error.get("error_type", "")
        count = error.get("count", 0)
        last_occurrence = error.get("last_occurrence", "")[:19] if error.get("last_occurrence") else ""
        error_rows.append(f'''
                <tr>
                    <td>{error_type}</td>
                    <td>{count}</td>
                    <td>{last_occurrence}</td>
                </tr>''')
    
    error_table = '<p>No errors recorded</p>' if not errors else f'''
        <table>
            <thead>
                <tr>
                    <th>Error Type</th>
                    <th>Count</th>
                    <th>Last Occurrence</th>
                </tr>
            </thead>
            <tbody>
                {''.join(error_rows)}
            </tbody>
        </table>
        '''
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipeline Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 40px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        h1, h2 {{
            color: #333;
        }}
        .metric-card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .metric-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .metric {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
            text-align: center;
        }}
        .metric-value {{
            font-size: 32px;
            font-weight: bold;
            color: #007bff;
        }}
        .metric-label {{
            font-size: 14px;
            color: #666;
            margin-top: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #007bff;
            color: white;
        }}
        .status-success {{
            color: green;
            font-weight: bold;
        }}
        .status-failed {{
            color: red;
            font-weight: bold;
        }}
        .timestamp {{
            color: #666;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <h1>📊 TMDb ETL Pipeline Report</h1>
    <p class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <div class="metric-card">
        <h2>7-Day Summary</h2>
        <div class="metric-grid">
            <div class="metric">
                <div class="metric-value">{stats_7d.get('total_runs', 0)}</div>
                <div class="metric-label">Total Runs</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats_7d.get('successful_runs', 0)}</div>
                <div class="metric-label">Successful</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats_7d.get('failed_runs', 0)}</div>
                <div class="metric-label">Failed</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats_7d.get('avg_duration', 0):.1f}s</div>
                <div class="metric-label">Avg Duration</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats_7d.get('total_movies_processed', 0)}</div>
                <div class="metric-label">Movies Processed</div>
            </div>
            <div class="metric">
                <div class="metric-value">{stats_7d.get('total_shows_processed', 0)}</div>
                <div class="metric-label">Shows Processed</div>
            </div>
        </div>
    </div>
    
    <div class="metric-card">
        <h2>Recent ETL Runs</h2>
        <table>
            <thead>
                <tr>
                    <th>Run ID</th>
                    <th>Start Time</th>
                    <th>Duration</th>
                    <th>Status</th>
                    <th>Movies</th>
                    <th>Shows</th>
                    <th>API Calls</th>
                </tr>
            </thead>
            <tbody>
                {''.join(runs_rows)}
            </tbody>
        </table>
    </div>
    
    <div class="metric-card">
        <h2>Error Summary (Last 7 Days)</h2>
        {error_table}
    </div>
</body>
</html>
    """
    
    with open(output_file, 'w') as f:
        f.write(html)
    
    return output_file


if __name__ == "__main__":
    # Example usage
    monitor = ETLMonitor()
    
    print("Generating ETL metrics report...")
    report_file = generate_report(monitor)
    print(f"Report generated: {report_file}")
    
    metrics_file = monitor.export_metrics()
    print(f"Metrics exported: {metrics_file}")

