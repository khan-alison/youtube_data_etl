import logging
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple, Any
from datetime import datetime
import pendulum
import mysql.connector as connector
from contextlib import contextmanager
from helper.logger import LoggerSimple
import os
from dotenv import load_dotenv
import json

load_dotenv()

logger = LoggerSimple.get_logger(__name__)


def convert_datetime(obj):
    """Convert Pendulum datetime to standard datetime"""
    if isinstance(obj, pendulum.DateTime):
        return obj.in_timezone('UTC').naive()
    if isinstance(obj, datetime):
        return obj
    return obj


@dataclass
class ExecutionLog:
    dag_id: str
    execution_date: datetime
    run_id: str
    status: str
    created_at: Optional[datetime] = None

    def to_tuple(self) -> tuple:
        """Convert to tuple with datetime conversion"""
        return (
            self.dag_id,
            convert_datetime(self.execution_date),
            self.run_id,
            self.status,
            convert_datetime(
                self.created_at) if self.created_at else datetime.utcnow()
        )


@dataclass
class TaskRun:
    execution_id: int
    task_id: str
    source_system: str
    database_name: str
    table_name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = 'PENDING'

    def to_tuple(self) -> tuple:
        """Convert to tuple with datetime conversion"""
        return (
            self.execution_id,
            self.task_id,
            self.source_system,
            self.database_name,
            self.table_name,
            convert_datetime(self.start_time) if self.start_time else None,
            convert_datetime(self.end_time) if self.end_time else None,
            self.status
        )


@dataclass
class TaskEvent:
    task_run_id: int
    event_type: str
    event_details: Dict
    status: str
    event_time: Optional[datetime] = None


@dataclass
class TaskConfig:
    task_run_id: int
    bucket_name: str
    object_key: str
    config_path: str
    parameters: Dict


@dataclass
class TableRun:
    execution_id: int
    task_run_id: int
    task_id: str
    table_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = 'RUNNING'


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
        return cls._instance

    def __init__(self):
        self.db = DatabaseConnection()
        logger.info("Logging manager initialized successfully.")

    def log_execution(self, execution_log: ExecutionLog) -> int:
        insert_query = """
        INSERT INTO execution_logs (dag_id, execution_date, run_id, status, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """
        execution_date = execution_log.execution_date
        if isinstance(execution_date, pendulum.DateTime):
            execution_date = execution_date.in_timezone(
                'UTC').replace(tzinfo=None)
        elif isinstance(execution_date, datetime):
            execution_date = execution_date.replace(tzinfo=None)

        created_at = execution_log.created_at or datetime.utcnow()
        if isinstance(created_at, pendulum.DateTime):
            created_at = created_at.in_timezone('UTC').replace(tzinfo=None)
        elif isinstance(created_at, datetime):
            created_at = created_at.replace(tzinfo=None)

        values = (
            execution_log.dag_id,
            execution_date,
            execution_log.run_id,
            execution_log.status,
            created_at
        )
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(insert_query, values)
            conn.commit()
            execution_id = cursor.lastrowid
        logger.info(f"Logged execution: {execution_id}")
        return execution_id

    def update_execution_status(self, execution_id: int, status: str):
        update_query = "UPDATE execution_logs SET status = %s WHERE id = %s"
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(update_query, (status, execution_id))
            conn.commit()
        logger.info(f"Updated execution {execution_id} status to {status}")

    def log_task_run(self, task_run: TaskRun) -> int:
        insert_query = """
        INSERT INTO task_runs (execution_id, task_id, source_system, database_name, table_name, start_time, end_time, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            task_run.execution_id,
            task_run.task_id,
            task_run.source_system,
            task_run.database_name,
            task_run.table_name,
            task_run.start_time,
            task_run.end_time,
            task_run.status
        )
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(insert_query, values)
            conn.commit()
            task_run_id = cursor.lastrowid
        logger.info(f"Logged task run: {task_run_id}")
        return task_run_id

    def update_task_run(self, task_run_id: int, end_time: datetime, status: str):
        update_query = "UPDATE task_runs SET end_time = %s, status = %s WHERE id = %s"
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(update_query, (end_time, status, task_run_id))
            conn.commit()
        logger.info(
            f"Updated task run {task_run_id} with end_time and status {status}")

    def log_task_event(self, task_event: TaskEvent) -> int:
        insert_query = """
        INSERT INTO task_events (task_run_id, event_type, event_details, event_time, status)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            task_event.task_run_id,
            task_event.event_type,
            json.dumps(task_event.event_details),
            task_event.event_time or datetime.utcnow(),
            task_event.status
        )
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(insert_query, values)
            conn.commit()
            event_id = cursor.lastrowid
        logger.info(f"Logged task event: {event_id}")
        return event_id

    def log_task_config(self, task_config: TaskConfig) -> int:
        insert_query = """
        INSERT INTO task_configs (task_run_id, bucket_name, object_key, config_path, parameters)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            task_config.task_run_id,
            task_config.bucket_name,
            task_config.object_key,
            task_config.config_path,
            json.dumps(task_config.parameters)
        )
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(insert_query, values)
            conn.commit()
            config_id = cursor.lastrowid
        logger.info(f"Logged task config: {config_id}")
        return config_id

    def log_table_run(self, table_run: TableRun) -> int:
        insert_query = """
        INSERT INTO table_runs (execution_id, task_run_id, task_id, table_name, start_time, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            table_run.execution_id,
            table_run.task_run_id,
            table_run.task_id,
            table_run.table_name,
            table_run.start_time,
            table_run.status
        )
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(insert_query, values)
            conn.commit()
            table_run_id = cursor.lastrowid
        logger.info(f"Logged table run: {table_run_id}")
        return table_run_id

    def update_table_run(self, table_run_id: int, end_time: datetime, status: str):
        update_query = "UPDATE table_runs SET end_time = %s, status = %s WHERE id = %s"
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(update_query, (end_time, status, table_run_id))
            conn.commit()
        logger.info(
            f"Updated table run {table_run_id} with end_time and status {status}")

    def get_table_status(self, table_name: str) -> str:
        """
        Get the latest status of a table from table_runs
        Returns: Latest status ('SUCCESS', 'FAILED', 'RUNNING')
        """

        try:
            query = """
                SELECT status
                FROM table_runs
                WHERE table_name = %s
                ORDER BY start_time DESC
                LIMIT 1
            """
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (table_name,))
                result = cursor.fetchone()

                if result:
                    return result[0]
                logger.warning(f"No status found for table {table_name}")
                return None
        except Exception as e:
            logger.error(f"Error getting table status: {str(e)}")
            return
