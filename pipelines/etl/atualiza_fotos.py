"""
==================================================================================
ATUALIZA FOTOS DAS ENTIDADES - pipelines/etl/atualiza_fotos.py
==================================================================================
Lê o diretório `data/fotos` e atualiza a coluna `foto` na tabela `entidade`
com o caminho formatado para a API (ex: /fotos/lula.jpg).
==================================================================================
"""
import sys
import logging
from pathlib import Path

from sqlalchemy import update

# Adiciona a raiz do projeto ao sys.path para importar os módulos
sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.database.postgres import get_session
from src.database.models import Entidade

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("atualiza_fotos")

FOTOS_DIR = Path(__file__).resolve().parents[2] / "data" / "fotos"

# Define como a API vai enxergar esse caminho na URL. 
# Ex: se a API vai servir em "http://api.com/fotos/lula.jpg", deixe "/fotos/".
PREFIXO_API = "/fotos/"

def atualizar_fotos(session):
    if not FOTOS_DIR.exists() or not FOTOS_DIR.is_dir():
        logger.error("Diretório de fotos não encontrado: %s", FOTOS_DIR)
        return

    fotos_atualizadas = 0

    for filepath in FOTOS_DIR.iterdir():
        if filepath.is_file():
            filename = filepath.name          # 'lula.jpg'
            codigo = filepath.stem            # 'lula'

            # ?v=<mtime> força o navegador a buscar de novo quando o arquivo é
            # substituído (mesmo nome/URL) - sem isso, atualizar a foto de um
            # candidato já visitado antes pode continuar mostrando a versão em cache.
            versao = int(filepath.stat().st_mtime)

            # Monta o caminho final que vai pro banco: /fotos/lula.jpg?v=169...
            caminho_api = f"{PREFIXO_API}{filename}?v={versao}"

            logger.info("Atualizando %s -> %s", codigo, caminho_api)

            stmt = (
                update(Entidade)
                .where(Entidade.codigo == codigo)
                .values(foto=caminho_api)
            )
            
            resultado = session.execute(stmt)
            
            if resultado.rowcount > 0:
                fotos_atualizadas += 1
            else:
                logger.warning("Nenhum candidato encontrado com o código '%s'", codigo)

    session.commit()
    logger.info("Concluído! %d fotos vinculadas no banco de dados.", fotos_atualizadas)


def run():
    session = get_session()
    try:
        atualizar_fotos(session)
    except Exception as e:
        logger.error("Erro ao atualizar fotos: %s", e)
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    run()