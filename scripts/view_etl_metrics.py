#!/usr/bin/env python3
"""
Quick script to view ETL metrics from the command line
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.monitoring import ETLMonitor, generate_report


def main():
    """Display ETL metrics"""
    monitor = ETLMonitor()
    
    print("\n" + "=" * 80)
    print("TMDb ETL Pipeline Metrics")
    print("=" * 80 + "\n")
    
    # Recent runs
    print("📊 Recent ETL Runs (Last 10)")
    print("-" * 80)
    recent = monitor.get_recent_runs(10)
    
    if not recent:
        print("No runs recorded yet.\n")
    else:
        for run in recent:
            status_icon = "✅" if run['status'] == 'success' else "❌"
            print(f"{status_icon} Run #{run['run_id']}")
            print(f"   Started: {run['start_time'][:19]}")
            print(f"   Duration: {run['duration_seconds']:.1f}s")
            print(f"   Status: {run['status'].upper()}")
            print(f"   Movies: {run['movies_processed']} processed, "
                  f"{run['movies_inserted']} inserted, "
                  f"{run['movies_updated']} updated")
            print(f"   Shows: {run['shows_processed']} processed, "
                  f"{run['shows_inserted']} inserted, "
                  f"{run['shows_updated']} updated")
            print(f"   API Calls: {run['api_calls']}")
            if run['errors'] > 0:
                print(f"   ⚠️  Errors: {run['errors']}")
            print()
    
    # 7-day statistics
    print("📈 7-Day Statistics")
    print("-" * 80)
    stats_7d = monitor.get_statistics(7)
    
    if stats_7d.get('total_runs', 0) > 0:
        success_rate = (stats_7d['successful_runs'] / stats_7d['total_runs']) * 100
        print(f"Total Runs: {stats_7d['total_runs']}")
        print(f"Success Rate: {success_rate:.1f}% "
              f"({stats_7d['successful_runs']} successful, {stats_7d['failed_runs']} failed)")
        print(f"Avg Duration: {stats_7d['avg_duration']:.1f}s")
        print(f"Total Movies Processed: {stats_7d['total_movies_processed']}")
        print(f"Total Shows Processed: {stats_7d['total_shows_processed']}")
        print(f"Total API Calls: {stats_7d['total_api_calls']}")
        
        if stats_7d['total_errors'] > 0:
            print(f"⚠️  Total Errors: {stats_7d['total_errors']}")
    else:
        print("No runs in the last 7 days.")
    
    print()
    
    # Error summary
    print("⚠️  Error Summary (Last 7 Days)")
    print("-" * 80)
    errors = monitor.get_error_summary(7)
    
    if not errors:
        print("No errors recorded! 🎉\n")
    else:
        for error in errors:
            print(f"❌ {error['error_type']}")
            print(f"   Count: {error['count']}")
            print(f"   Last: {error['last_occurrence'][:19]}")
            print()
    
    print("=" * 80)
    
    # Generate HTML report
    print("\nGenerating detailed HTML report...")
    report_file = generate_report(monitor)
    print(f"✅ Report saved to: {report_file}")
    
    # Export metrics
    metrics_file = monitor.export_metrics()
    print(f"✅ Metrics exported to: {metrics_file}")
    
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()

