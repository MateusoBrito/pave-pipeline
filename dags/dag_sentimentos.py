from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from common.datasets import TOPICOS_INFERIDOS

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_classificacao_sentimentos',
    default_args=default_args,
    description='Classifica sentimento dos comentários pendentes - roda depois de '
                 'dag_inferencia (não num cron próprio): sentimento vem depois da '
                 'atribuição de tópico, e precisa rodar de novo a cada ciclo pros '
                 'documentos novos, não só uma vez por dia.',
    schedule=[TOPICOS_INFERIDOS],
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=['nlp', 'sentimento'],
) as dag:

    classificar_sentimentos = BashOperator(
        task_id='run_classificador_bertimbau',
        bash_command="cd /opt/airflow && PYTHONPATH=. python3 pipelines/nlp/classificar_sentimentos.py --data-inicio {{ ds }}"
    )

    classificar_sentimentos