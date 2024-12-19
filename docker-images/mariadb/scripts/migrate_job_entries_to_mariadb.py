import os
import json
import mysql.connector
from mysql.connector import Error
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "user": "admin",
    "password": "admin",
    "database": "metastore_db"
}


def insert_conditional_mapping(cursor, data):
    """
    Insert data into conditional_mapping table.
    """
    query = """
    INSERT INTO conditional_mapping (
        dataset_name,
        dependency_events,
        trigger_event,
        dependencies,
        validations,
        output_path
    ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        data["dataset_name"],
        json.dumps(data["dependency_events"]),
        data["trigger_event"],
        json.dumps(data["dependencies"]),
        json.dumps(data.get("validations", None)),
        data["output"]["path"],
    ))


def migrate_job_entries(folder_path):
    """
    Parse JSON files and insert data into MariaDB
    """
    try:
        # Fix: Pass DB_CONFIG as keyword arguments
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    logger.info(f"Processing file: {file_path}")
                    try:
                        with open(file_path, "r") as f:
                            data = json.load(f)
                        insert_conditional_mapping(cursor, data)
                        logger.info(f"Successfully migrated {file}")
                    except Exception as e:
                        logger.error(f"Failed to migrate {file}: {str(e)}")

        # Commit changes to the database
        connection.commit()

    except Error as e:
        logger.error(f"Error connecting to MariaDB: {str(e)}")
        raise
    finally:
        # Close connection if established
        if "connection" in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MariaDB connection closed.")


if __name__ == "__main__":
    JOB_ENTRIES_PATH = "./job_entries/g2i"
    migrate_job_entries(JOB_ENTRIES_PATH)
