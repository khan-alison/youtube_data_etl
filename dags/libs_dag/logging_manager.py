from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple, Any
from datetime import datetime
import mysql.connector as connector
from contextlib import contextmanager
from helper.logger import LoggerSimple
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

logger = LoggerSimple.get_logger(__name__)


@dataclass
class TaskLog:
    """Data class for task logs"""
    job_id: str
    step_function: str
    data_time: dict
    finish_events: List[dict]
    trigger_events: List[dict]
    conditions: List[dict]
    priority: int
    step_function_input: dict
    is_disabled: bool
    tags: List[str]
    created_at: datetime = None

    def to_dict(self) -> dict:
        data = asdict(self)
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        return data


class DatabaseConnection:
    def __init__(self):
        self.config = {
            'host': os.getenv('LOGGING_DB_HOST', 'yt-mariadb'),
            'user': os.getenv('LOGGING_DB_USER', 'admin'),
            'password': os.getenv('LOGGING_DB_PASSWORD', 'admin'),
            'database': os.getenv('LOGGING_DB_DATABASE', 'metastore_db')
        }

    @contextmanager
    def get_connection(self):
        """Context manager for database connection."""
        conn = connector.connect(**self.config)
        try:
            yield conn
        finally:
            conn.close()


class LoggingManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggingManager, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        self.db = DatabaseConnection()
        self._init_database()
        self.initialized = True
        logger.info("Logging manager initialized successfully.")

    def _init_database(self):
        """Initialize database schema if not exists."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS task_logs (
            id INT AUTO_INCREMENT,
            job_id VARCHAR(255) NOT NULL,
            step_function VARCHAR(255) NOT NULL,
            data_time LONGTEXT,
            finish_events LONGTEXT,
            trigger_events LONGTEXT,
            conditions LONGTEXT,
            priority INT,
            step_function_input LONGTEXT,
            is_disabled BOOLEAN,
            tags LONGTEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY idx_job_id (job_id),
            KEY idx_created_at (created_at),
            KEY idx_step_function (step_function)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_query)
                conn.commit()
            logger.info("Database schema initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize database schema: {str(e)}")
            return

    def log_task(self, task_log: TaskLog) -> bool:
        """
        Log a task execution to the database
        Args:
            task_log: TaskLog instance containing execution details
        Returns:
            bool: Success status of logging operation
        """
        try:
            insert_query = """
            INSERT INTO task_logs (
                job_id,
                step_function,
                data_time,
                finish_events,
                trigger_events,
                conditions,
                priority,
                step_function_input,
                is_disabled,
                tags
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            task_data = task_log.to_dict()

            values = (
                task_data['job_id'],
                task_data['step_function'],
                json.dumps(task_data['data_time']),
                json.dumps(task_data['finish_events']),
                json.dumps(task_data['trigger_events']),
                json.dumps(task_data['conditions']),
                task_data['priority'],
                json.dumps(task_data['step_function_input']),
                task_data['is_disabled'],
                json.dumps(task_data['tags'])
            )
            logger.info(f"Executing SQL query: {insert_query}")
            logger.info(f"With values: {values}")

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(insert_query, values)
                conn.commit()

            logger.info(f"Successfully logged task: {task_data['job_id']}.")
            return True

        except Exception as e:
            logger.error(f"Failed to log task: {str(e)}")
            raise

    def get_task_logs(
            self,
            job_id: Optional[str] = None,
            step_function: Optional[str] = None,
            time_range: Optional[Tuple[datetime, datetime]] = None,
            tags: Optional[List[str]] = None,
            status: Optional[str] = None,
            limit: Optional[int] = 100) -> List[Dict]:
        """
        Retrieve task logs with optional filtering
        Args:
            job_id: Filter by job ID
            step_function: Filter by step function name
            time_range: Tuple of (start_time, end_time)
            tags: List of tags to filter by
            status: Filter by task status
            limit: Maximum number of records to return
        Returns:
            List[Dict]: List of matching task logs
        """
        try:
            query = "SELECT * FROM task_logs WHERE 1=1"
            params = []

            if job_id:
                query += " AND job_id = %s"
                params.append(job_id)

            if step_function:
                query += " AND step_function = %s"
                params.append(step_function)

            if time_range:
                start_time, end_time = time_range
                query += " AND created_at BETWEEN %s AND %s"
                params.extend([start_time, end_time])

            if tags:
                query += " AND JSON_CONTAINS(tags. %s)"
                params.append(json.dumps(tags))

            if status:
                query += " AND JSON_CONTAINS(finish_events, %s, '$[*].status')"
                params.append(json.dumps(status))

            query += " ORDER BY created_at DESC"

            if limit:
                query += f" LIMIT %s"
                params.append(limit)

            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, params)
                results = cursor.fetchall()

                for row in results:
                    for field in ['data_time', 'finish_events', 'trigger_events', 'conditions', 'step_function_input', 'tags']:
                        row[field] = self._safe_json_parse(row[field])
                logger.info(f"Retrived {len(results)} task logs")
                return results
        except Exception as e:
            logger.error(f"Failed to retrieve task logs: {str(e)}")
            return []

    def check_execution_conditions(self, task_conditions: List[dict]) -> bool:
        """
        Check if a task satisfies all the given conditions.
        Args:
            task_conditions: List of condition dictionaries
            Example format:
            [
                {
                    "type": "dependency",
                    "job_id": "previous_task_id",
                    "status": "success",
                    "within_hours": 24
                }
            ]
        Returns:
            bool: Whether all conditions are met
        """
        try:
            for condition in task_conditions:
                if condition['type'] == 'dependency':
                    dependent_logs = self.get_task_logs(
                        job_id=condition['job_id'], time_range=(
                            datetime.now() -
                            timedelta(hours=condition['within_hours']),
                            datetime.now()
                        ),
                        limit=1
                    )
                    if not dependent_logs:
                        logger.info(
                            f"No logs found for dependent task: {condition['job_id']}")
                        return False
            logger.info("All execution conditions met.")
            return True
        except Exception as e:
            logger.error(f"Error checking execution conditions: {str(e)}")
            return False

    def get_task_statistics(
        self,
        job_id: str,
        time_range: Tuple[datetime, datetime]
    ) -> Dict[str, Any]:
        """
        Get statistics for a specific task over a time period
        Args:
            job_id: Task identifier
            time_range: Time period to analyze
        Returns:
            Dict containing task statistics
        """
        try:
            logs = self.get_task_logs(
                job_id=job_id,
                time_range=time_range
            )

            total_executions = len(logs)
            successful = sum(1 for log in logs
                             if any(event['status'] == 'success'
                                    for event in log['finish_events']))
            failed = total_executions - successful

            statistics = {
                'total_executions': total_executions,
                'successful_executions': successful,
                'failed_executions': failed,
                'success_rate': (successful / total_executions * 100) if total_executions > 0 else 0,
                'time_period': {
                    'start': time_range[0].isoformat(),
                    'end': time_range[1].isoformat()
                }
            }

            logger.info(f"Generated statistics for task {job_id}")
            return statistics

        except Exception as e:
            logger.error(f"Error generating task statistics: {str(e)}")
            return {}

    @staticmethod
    def _safe_json_parse(data: Any) -> Any:
        """
        Safely parse JSON strings
        """
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return data
        return data
