from logging.config import fileConfig
import os
from dotenv import load_dotenv

# Carrega explicitamente o arquivo .env da raiz do projeto
load_dotenv()

from sqlalchemy import engine_from_config, create_engine, pool
from alembic import context

# Importe a Base onde estão os seus modelos
from src.database.models import Base  

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Alembic migra o schema analítico (panorama_db), não o banco de metadados do Airflow.
# Roda a partir do venv no host, por isso o default de host é 'localhost' (porta 5432 mapeada).
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")
db_host = os.getenv("POSTGRES_HOST", "localhost")
db_port = os.getenv("POSTGRES_PORT", "5432")

db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

config.set_main_option("sqlalchemy.url", db_url)

target_metadata = Base.metadata

# Lista de tabelas do Airflow para o Alembic ignorar completamente
AIRFLOW_TABLES = {
    'ab_permission', 'ab_register_user', 'ab_role', 'ab_permission_view',
    'ab_permission_view_role', 'ab_user', 'ab_user_role', 'callback_request',
    'connection', 'dag', 'dag_code', 'dag_owner_attributes', 'dag_pickle',
    'dag_run', 'dag_run_note', 'dag_schedule_dataset_reference', 'dag_tag',
    'dag_warning', 'dagrun_dataset_event', 'dataset', 'dataset_dag_run_queue',
    'dataset_event', 'import_error', 'job', 'log', 'log_template',
    'rendered_task_instance_fields', 'serialized_dag', 'session', 'sla_miss',
    'slot_pool', 'task_fail', 'task_instance', 'task_instance_note', 'trigger'
}

def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and name in AIRFLOW_TABLES:
        return False
    return True

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()
def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, 
            target_metadata=target_metadata,
            include_object=include_object  # <--- ADICIONE ESTA LINHA AQUI
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()