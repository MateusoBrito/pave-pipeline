import sys
import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.database.postgres import get_session
from src.database.models import CandidatoHashtag

COLLECTION_TO_FONTE = {
    "youtube_comments": "youtube",
    "reddit": "reddit",
    "meta": "meta",
}

TEXT_COLUMNS = ["text_raw"]

def processar_todas_hashtags(session, base_data_dir: str):
    base_path = Path(base_data_dir)
    
    if not base_path.exists():
        print(f"Erro: Diretório de dados não encontrado: {base_data_dir}")
        return

    for collection_dir in base_path.iterdir():
        if not collection_dir.is_dir():
            continue
            
        fonte_codigo = COLLECTION_TO_FONTE.get(collection_dir.name)
        if not fonte_codigo:
            continue
            
        for parquet_file in collection_dir.glob("*.parquet"):
            entidade_codigo = parquet_file.stem 

            print(f"[{fonte_codigo}] Extraindo hashtags de: {entidade_codigo}...")
            
            try:
                df = pd.read_parquet(parquet_file)
            except Exception as e:
                print(f"Erro ao ler {parquet_file}: {e}")
                continue
            
            colunas_presentes = [col for col in TEXT_COLUMNS if col in df.columns]
            if not colunas_presentes:
                print(f"  -> Nenhuma coluna de texto válida encontrada. Colunas atuais: {df.columns}")
                continue
            
            texto_combinado = df[colunas_presentes].astype(str).agg(' '.join, axis=1)
            
            hashtags_series = texto_combinado.str.lower().str.findall(r'#\w+')
            df_exploded = hashtags_series.explode().dropna()
            
            if df_exploded.empty:
                print(f"  -> Nenhuma hashtag encontrada.")
                continue
                
            contagem = df_exploded.value_counts().reset_index()
            contagem.columns = ['hashtag', 'contagem']
            top_hashtags = contagem.head(50)
            
            for _, row in top_hashtags.iterrows():
                stmt = (
                    pg_insert(CandidatoHashtag)
                    .values(
                        fonte_codigo=fonte_codigo,
                        entidade_codigo=entidade_codigo,
                        hashtag=row['hashtag'],
                        contagem=row['contagem']
                    )
                    .on_conflict_do_update(
                        index_elements=['fonte_codigo', 'entidade_codigo', 'hashtag'],
                        set_=dict(contagem=row['contagem'])
                    )
                )
                session.execute(stmt)
            
            session.commit()
            print(f"  -> Sucesso: {len(top_hashtags)} hashtags salvas/atualizadas!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrai hashtags dos Parquets e salva no BD.")
    parser.add_argument("--data-dir", default="/opt/airflow/pave-tm/data", help="Diretório raiz dos parquets")
    args = parser.parse_args()
    
    data_dir = args.data_dir
    if not Path(data_dir).exists():
        data_dir = "/home/labpi/pave-tm/data"

    sess = get_session()
    processar_todas_hashtags(sess, data_dir)
    sess.close()