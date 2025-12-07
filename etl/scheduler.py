#!/usr/bin/env python3
"""
Automated ETL Scheduler using APScheduler
Continuously fetches and updates TMDb data on a configurable schedule
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.tmdb_etl_service import TMDbETLService
from etl.monitoring import ETLMonitor
from etl.kpi_service import KPIService


class ETLScheduler:
    """
    Manages automated scheduling and execution of TMDb ETL pipeline
    """
    
    def __init__(self, config_path: str = "etl_config.yaml"):
        """Initialize the scheduler with configuration"""
        load_dotenv()
        
        self.config = self._load_config(config_path)
        self.scheduler = BackgroundScheduler(
            timezone=self.config['schedule'].get('timezone', 'UTC')
        )
        self.etl_service: Optional[TMDbETLService] = None
        self.kpi_service: Optional[KPIService] = None
        self.last_run_time: Optional[datetime] = None
        self.last_run_status: str = "Never run"
        self.run_count: int = 0
        
        # Initialize monitoring
        self.monitor: Optional[ETLMonitor] = None
        if self.config.get('monitoring', {}).get('enable_metrics', True):
            self.monitor = ETLMonitor()
        
        self._setup_logging()
        self.logger.info("ETL Scheduler initialized")
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    def _setup_logging(self):
        """Configure logging for the ETL scheduler"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'etl_scheduler.log')
        
        # Create logger
        self.logger = logging.getLogger('ETLScheduler')
        self.logger.setLevel(log_level)
        
        # File handler with rotation
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=log_config.get('max_bytes', 10485760),
            backupCount=log_config.get('backup_count', 5)
        )
        file_handler.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def run_etl_job(self):
        """Execute the ETL pipeline"""
        self.run_count += 1
        run_id = f"RUN-{self.run_count}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        self.logger.info(f"=" * 80)
        self.logger.info(f"Starting ETL job: {run_id}")
        self.logger.info(f"=" * 80)
        
        start_time = time.time()
        
        # Start monitoring
        monitor_run_id = None
        if self.monitor:
            monitor_run_id = self.monitor.start_run()
        
        try:
            # Initialize ETL service
            if self.etl_service is None:
                self.etl_service = TMDbETLService(self.config)
            
            # Run the ETL pipeline
            stats = self.etl_service.run_full_etl()
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Log results
            self.logger.info(f"ETL job {run_id} completed successfully")
            self.logger.info(f"Execution time: {execution_time:.2f} seconds")
            self.logger.info(f"Statistics: {stats}")
            
            self.last_run_time = datetime.now()
            self.last_run_status = "Success"
            
            # Record metrics
            if self.monitor and monitor_run_id:
                self.monitor.end_run(monitor_run_id, stats, status='success')
            
            # Run KPI computation if enabled
            if self.config.get('kpi', {}).get('enabled', True):
                self.logger.info("Running KPI precomputation...")
                try:
                    if self.kpi_service is None:
                        self.kpi_service = KPIService(self.config)
                    kpi_stats = self.kpi_service.run_kpi_computation()
                    self.logger.info(f"KPI computation completed: {kpi_stats}")
                except Exception as kpi_error:
                    self.logger.error(f"KPI computation failed: {kpi_error}")
            
            # Optional: Run cleanup/optimization
            if self.config.get('database', {}).get('vacuum_on_completion', False):
                self.logger.info("Running database VACUUM...")
                self.etl_service.vacuum_database()
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"ETL job {run_id} failed after {execution_time:.2f} seconds")
            self.logger.error(f"Error: {str(e)}", exc_info=True)
            
            self.last_run_time = datetime.now()
            self.last_run_status = f"Failed: {str(e)}"
            
            # Record failure in metrics
            if self.monitor and monitor_run_id:
                import traceback
                stats = getattr(self.etl_service, 'stats', {}) if self.etl_service else {}
                self.monitor.end_run(monitor_run_id, stats, status='failed', error_message=str(e))
                self.monitor.log_error(monitor_run_id, type(e).__name__, str(e), traceback.format_exc())
            
            # Send alert if configured
            self._send_alert(run_id, str(e))
        
        self.logger.info(f"=" * 80)
        self.logger.info(f"ETL job {run_id} finished")
        self.logger.info(f"=" * 80 + "\n")
    
    def _send_alert(self, run_id: str, error_message: str):
        """Send email alert on ETL failure"""
        if not self.config.get('monitoring', {}).get('email_alerts', False):
            return
        
        try:
            import smtplib
            from email.mime.text import MIMEText
            
            smtp_config = self.config.get('monitoring', {})
            
            msg = MIMEText(f"""
            ETL Job Failed
            
            Run ID: {run_id}
            Time: {datetime.now()}
            Error: {error_message}
            
            Please check the logs for more details.
            """)
            
            msg['Subject'] = f'ETL Alert: Job {run_id} Failed'
            msg['From'] = smtp_config.get('alert_email')
            msg['To'] = smtp_config.get('alert_email')
            
            with smtplib.SMTP(
                smtp_config.get('smtp_host'), 
                smtp_config.get('smtp_port')
            ) as server:
                server.starttls()
                server.send_message(msg)
            
            self.logger.info("Alert email sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to send alert email: {e}")
    
    def start(self):
        """Start the scheduler"""
        schedule_config = self.config['schedule']
        
        # Determine trigger type
        if 'cron' in schedule_config:
            # Use cron-style scheduling
            cron_config = schedule_config['cron']
            trigger = CronTrigger(
                hour=cron_config.get('hour', 0),
                minute=cron_config.get('minute', 0),
                day_of_week=cron_config.get('day_of_week', '*'),
                timezone=schedule_config.get('timezone', 'UTC')
            )
            self.logger.info(f"Scheduled ETL with cron: {cron_config}")
        else:
            # Use interval-based scheduling
            interval_hours = schedule_config.get('interval_hours', 24)
            trigger = IntervalTrigger(
                hours=interval_hours,
                timezone=schedule_config.get('timezone', 'UTC')
            )
            self.logger.info(f"Scheduled ETL to run every {interval_hours} hours")
        
        # Add the job
        self.scheduler.add_job(
            self.run_etl_job,
            trigger=trigger,
            id='tmdb_etl_job',
            name='TMDb ETL Pipeline',
            replace_existing=True
        )
        
        # Start the scheduler
        self.scheduler.start()
        self.logger.info("ETL Scheduler started successfully")
        
        # Run immediately if configured
        if schedule_config.get('run_on_startup', False):
            self.logger.info("Running initial ETL on startup...")
            self.run_etl_job()
    
    def stop(self):
        """Stop the scheduler gracefully"""
        self.logger.info("Stopping ETL Scheduler...")
        self.scheduler.shutdown()
        self.logger.info("ETL Scheduler stopped")
    
    def get_status(self) -> dict:
        """Get current scheduler status"""
        return {
            'running': self.scheduler.running,
            'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
            'last_run_status': self.last_run_status,
            'total_runs': self.run_count,
            'next_run_time': self.scheduler.get_jobs()[0].next_run_time.isoformat() 
                            if self.scheduler.get_jobs() else None
        }


def main():
    """Main entry point for standalone execution"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="TMDb Automated ETL Scheduler"
    )
    parser.add_argument(
        '--config',
        default='etl_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--run-once',
        action='store_true',
        help='Run ETL once and exit (no scheduling)'
    )
    args = parser.parse_args()
    
    scheduler = ETLScheduler(config_path=args.config)
    
    if args.run_once:
        # Run once and exit
        scheduler.logger.info("Running ETL once (no scheduling)")
        scheduler.run_etl_job()
        return
    
    # Start scheduler and keep running
    try:
        scheduler.start()
        
        # Keep the main thread alive
        print("\nETL Scheduler is running. Press Ctrl+C to stop.\n")
        print(f"Next run scheduled for: {scheduler.scheduler.get_jobs()[0].next_run_time}")
        
        while True:
            time.sleep(60)  # Check every minute
            
    except (KeyboardInterrupt, SystemExit):
        print("\nShutting down scheduler...")
        scheduler.stop()
        print("Scheduler stopped. Goodbye!")


if __name__ == "__main__":
    main()

