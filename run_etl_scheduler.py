#!/usr/bin/env python3
"""
Standalone runner for the TMDb ETL Scheduler
Can be used to run the scheduler as a daemon or one-off job
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from etl.scheduler import ETLScheduler


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print("\n\nReceived shutdown signal. Stopping scheduler...")
    sys.exit(0)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="TMDb Automated ETL Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run scheduler continuously with default config
  python run_etl_scheduler.py
  
  # Run ETL once and exit
  python run_etl_scheduler.py --run-once
  
  # Use custom configuration file
  python run_etl_scheduler.py --config my_config.yaml
  
  # Run as background daemon (Unix-like systems)
  nohup python run_etl_scheduler.py > etl.log 2>&1 &
        """
    )
    
    parser.add_argument(
        '--config',
        default='etl_config.yaml',
        help='Path to configuration file (default: etl_config.yaml)'
    )
    
    parser.add_argument(
        '--run-once',
        action='store_true',
        help='Run ETL once and exit (no continuous scheduling)'
    )
    
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current scheduler status'
    )
    
    parser.add_argument(
        '--validate-config',
        action='store_true',
        help='Validate configuration file and exit'
    )
    
    args = parser.parse_args()
    
    # Validate configuration
    if args.validate_config:
        try:
            import yaml
            with open(args.config, 'r') as f:
                config = yaml.safe_load(f)
            print(f"[OK] Configuration file '{args.config}' is valid")
            print("\nConfiguration summary:")
            print(f"  Schedule: Every {config['schedule'].get('interval_hours', 'N/A')} hours")
            print(f"  Movies per run: {config['data_limits'].get('movies', 'N/A')}")
            print(f"  Shows per run: {config['data_limits'].get('shows', 'N/A')}")
            print(f"  Database: {config['database'].get('path', 'N/A')}")
            return 0
        except Exception as e:
            print(f"[ERROR] Configuration validation failed: {e}")
            return 1
    
    # Check if config file exists
    if not Path(args.config).exists():
        print(f"Error: Configuration file '{args.config}' not found")
        print("Please create a configuration file or specify a different path with --config")
        return 1
    
    # Check for API key
    from dotenv import load_dotenv
    load_dotenv()
    
    if not os.getenv('TMDB_API_KEY'):
        print("Error: TMDB_API_KEY not found in environment")
        print("Please set it in your .env file or environment variables")
        return 1
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Create scheduler instance
        scheduler = ETLScheduler(config_path=args.config)
        
        if args.status:
            # Show status and exit
            status = scheduler.get_status()
            print("\nETL Scheduler Status:")
            print(f"  Running: {status['running']}")
            print(f"  Last run: {status['last_run_time'] or 'Never'}")
            print(f"  Last status: {status['last_run_status']}")
            print(f"  Total runs: {status['total_runs']}")
            print(f"  Next run: {status['next_run_time'] or 'Not scheduled'}")
            return 0
        
        if args.run_once:
            # Run once and exit
            print("Running ETL once (no continuous scheduling)...\n")
            scheduler.run_etl_job()
            print("\nETL job complete. Exiting.")
            return 0
        
        # Start continuous scheduler
        scheduler.start()
        
        print("\n" + "=" * 80)
        print("TMDb ETL Scheduler is now running")
        print("=" * 80)
        print(f"\nConfiguration: {args.config}")
        
        jobs = scheduler.scheduler.get_jobs()
        if jobs:
            print(f"Next scheduled run: {jobs[0].next_run_time}")
        
        print("\nPress Ctrl+C to stop the scheduler")
        print("=" * 80 + "\n")
        
        # Keep running
        import time
        while True:
            time.sleep(60)
    
    except KeyboardInterrupt:
        print("\n\nShutdown requested by user")
        if 'scheduler' in locals():
            scheduler.stop()
        return 0
    
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

