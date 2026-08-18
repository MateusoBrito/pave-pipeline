"""
Script de AUDITORIA - roda fora do Airflow, só pra você inspecionar
rapidamente o que já está salvo no MongoDB depois de uma execução da DAG.

Uso:
    python3 auditar_reddit.py

Requer as mesmas variáveis de ambiente do coletor:
    MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD
    (e opcionalmente MONGO_HOST, MONGO_PORT, MONGO_DATABASE, MONGO_COLLECTION)
"""

import os
from pathlib import Path
from urllib.parse import quote_plus
from pymongo import MongoClient


def _carregar_env_do_projeto():
    """
    Carrega variáveis do arquivo .env do projeto automaticamente, para não
    depender de `export` manual toda vez que abrir um terminal novo.

    Procura um arquivo `.env` subindo a partir da pasta deste script (ex:
    pipelines/.env, depois a raiz do projeto ../.env, etc), até achar um ou
    chegar na raiz do sistema de arquivos. Só preenche variáveis que ainda
    NÃO estão definidas no ambiente (não sobrescreve um `export` manual que
    você já tenha feito).
    """
    diretorio = Path(__file__).resolve().parent
    for _ in range(5):  # sobe no máximo 5 níveis de pasta procurando o .env
        candidato = diretorio / ".env"
        if candidato.is_file():
            for linha in candidato.read_text().splitlines():
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, _, valor = linha.partition("=")
                chave = chave.strip()
                valor = valor.strip().strip('"').strip("'")
                os.environ.setdefault(chave, valor)
            print(f"[info] Variáveis carregadas de: {candidato}")
            return
        if diretorio.parent == diretorio:
            break
        diretorio = diretorio.parent
    print("[info] Nenhum arquivo .env encontrado automaticamente (ok se você já usou 'export').")


_carregar_env_do_projeto()

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "panorama")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "reddit")

if not MONGO_USER or not MONGO_PASSWORD:
    raise SystemExit(
        "ERRO: as variáveis de ambiente MONGO_INITDB_ROOT_USERNAME e "
        "MONGO_INITDB_ROOT_PASSWORD não estão definidas neste terminal.\n"
        "Elas só existem automaticamente DENTRO do container do Airflow.\n"
        "Rodando fora do Docker, exporte-as manualmente antes de rodar o script, ex:\n"
        "  export MONGO_INITDB_ROOT_USERNAME=seu_usuario\n"
        "  export MONGO_INITDB_ROOT_PASSWORD=sua_senha\n"
        "  export MONGO_HOST=localhost   # ou o host correto se não for local\n"
        "(Os valores reais estão no seu arquivo .env do projeto.)"
    )

MONGO_URI = f"mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:{MONGO_PORT}/"

print(f"Conectando em {MONGO_HOST}:{MONGO_PORT} | database='{MONGO_DATABASE}' | colecao='{MONGO_COLLECTION}'...")

try:
    cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    cliente.admin.command("ping")  # força uma tentativa de conexão real agora, com erro claro se falhar
except Exception as erro:
    raise SystemExit(
        f"ERRO ao conectar no MongoDB em {MONGO_HOST}:{MONGO_PORT}: {erro}\n\n"
        "Causas mais comuns:\n"
        "  1. Usuário/senha errados (confira o .env do projeto).\n"
        "  2. MONGO_HOST='localhost' mas você está rodando FORA da rede Docker "
        "     -- nesse caso o host correto pode ser 'localhost' mesmo (já que a porta "
        "     27017 está exposta no docker-compose), mas confirme com "
        "     `docker compose ps` que o container 'panorama-mongodb' está rodando "
        "     e com a porta publicada.\n"
        "  3. Firewall ou porta não publicada corretamente."
    )

colecao = cliente[MONGO_DATABASE][MONGO_COLLECTION]

total = colecao.count_documents({})
print(f"\n=== TOTAL de documentos na coleção '{MONGO_COLLECTION}': {total} ===\n")

print("--- Contagem por subreddit x tipo x termo de busca ---")
pipeline = [
    {
        "$group": {
            "_id": {
                "subreddit": "$_subreddit_busca",
                "tipo": "$_tipo_documento",
                "termo": "$_termo_busca",
            },
            "quantidade": {"$sum": 1},
        }
    },
    {"$sort": {"_id.subreddit": 1, "_id.termo": 1, "_id.tipo": 1}},
]
for linha in colecao.aggregate(pipeline):
    d = linha["_id"]
    print(f"  r/{d.get('subreddit')} | {d.get('tipo')} | termo='{d.get('termo')}' -> {linha['quantidade']}")

print("\n--- Exemplo de POST salvo ---")
post_exemplo = colecao.find_one({"_tipo_documento": "post"})
if post_exemplo:
    print(f"  id: {post_exemplo.get('id')}")
    print(f"  termo_busca: {post_exemplo.get('_termo_busca')}")
    print(f"  subreddit: {post_exemplo.get('_subreddit_busca')}")
    print(f"  title: {str(post_exemplo.get('title'))[:120]}")
else:
    print("  Nenhum post encontrado.")

print("\n--- Exemplo de COMENTÁRIO salvo ---")
comentario_exemplo = colecao.find_one({"_tipo_documento": "comentario"})
if comentario_exemplo:
    print(f"  id: {comentario_exemplo.get('id')}")
    print(f"  termo_busca: {comentario_exemplo.get('_termo_busca')}")
    print(f"  subreddit: {comentario_exemplo.get('_subreddit_busca')}")
    print(f"  body: {str(comentario_exemplo.get('body'))[:120]}")
else:
    print("  Nenhum comentário encontrado.")

print("\n--- Índices ativos na coleção ---")
for idx in colecao.list_indexes():
    print(f"  {idx['name']}: {idx['key']}")

cliente.close()