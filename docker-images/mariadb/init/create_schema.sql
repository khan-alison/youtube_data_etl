USE metastore_db;

CREATE TABLE IF NOT EXISTS execution_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    INDEX idx_dag_date (dag_id, execution_date),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS task_runs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    execution_id BIGINT NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    source_system VARCHAR(100) NOT NULL,
    database_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    status VARCHAR(50) NOT NULL,
    FOREIGN KEY (execution_id) REFERENCES execution_logs(id),
    INDEX idx_task_system (task_id, source_system),
    INDEX idx_task_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS task_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_run_id BIGINT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_details JSON NOT NULL,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    FOREIGN KEY (task_run_id) REFERENCES task_runs(id),
    INDEX idx_event_type (event_type),
    INDEX idx_event_time (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS task_configs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_run_id BIGINT NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    config_path VARCHAR(1024) NOT NULL,
    parameters JSON NOT NULL,
    FOREIGN KEY (task_run_id) REFERENCES task_runs(id),
    INDEX idx_bucket (bucket_name),
    INDEX idx_object (object_key(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS dataset_runs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    execution_id BIGINT NOT NULL,
    task_run_id BIGINT NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    dataset VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'RUNNING',
    FOREIGN KEY (execution_id) REFERENCES execution_logs(id),
    FOREIGN KEY (task_run_id) REFERENCES task_runs(id),
    INDEX idx_task_id (task_id),
    INDEX idx_table_name (dataset),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE datasets (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_system VARCHAR(100) NOT NULL,
    database_name VARCHAR(100) NOT NULL,
    dataset_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (source_system, database_name, dataset_name)
);

CREATE TABLE dataset_inputs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dataset_id BIGINT NOT NULL,
    format VARCHAR(50) NOT NULL,
    path VARCHAR(2048) NOT NULL,
    options JSON NOT NULL,
    cache BOOLEAN DEFAULT FALSE,
    data_persistence VARCHAR(50),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
);

CREATE TABLE dataset_dependencies (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dataset_id BIGINT NOT NULL,
    dependency_name VARCHAR(255) NOT NULL,
    s3_path VARCHAR(2048) NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
);

CREATE TABLE dataset_transformations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dataset_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    params JSON NOT NULL,
    target VARCHAR(255),
    multi_output BOOLEAN DEFAULT FALSE,
    show_sample BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
);

CREATE TABLE dataset_transformation_outputs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transformation_id BIGINT NOT NULL,
    output_name VARCHAR(255) NOT NULL,
    FOREIGN KEY (transformation_id) REFERENCES dataset_transformations(id)
);

CREATE TABLE dataset_outputs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dataset_id BIGINT NOT NULL,
    dataframe_name VARCHAR(255) NOT NULL,
    format VARCHAR(50) NOT NULL,
    path VARCHAR(2048) NOT NULL,
    history_path VARCHAR(2048),
    mode VARCHAR(50) NOT NULL,
    partition_by JSON NOT NULL,
    store_type VARCHAR(50) NOT NULL,
    primary_key JSON NOT NULL,
    tracking_columns JSON,
    finish_event VARCHAR(255),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
);

CREATE TABLE dataset_validations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dataset_id BIGINT NOT NULL,
    validation_type VARCHAR(100) NOT NULL,
    params JSON NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
);

CREATE TABLE dataset_join_keys (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dataset_id BIGINT NOT NULL,
    left_table VARCHAR(255) NOT NULL,
    left_column VARCHAR(255) NOT NULL,
    right_table VARCHAR(255) NOT NULL,
    right_column VARCHAR(255) NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
);