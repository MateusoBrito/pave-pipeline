#!/bin/bash
# ==================================================================================
# BACKFILL HISTÓRICO DINÂMICO - Reddit (Mai/2026 até Hoje)
# Localização: backfill/reddit/run_backfill.sh
# ==================================================================================

# 1. Garante que o script rode a partir da raiz do projeto, não importa de onde seja chamado
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAIZ_PROJETO="/home/labpi/pave-pipeline"
cd "$RAIZ_PROJETO"

# Adiciona a raiz do projeto ao PYTHONPATH para permitir a importação de módulos com 'python3 -m'
export PYTHONPATH="$RAIZ_PROJETO:$PYTHONPATH"

# 2. Caminhos relativos a partir da raiz do projeto
YAML_PATH="config/entities.yaml"
LOG_DIR="/home/labpi/pave-pipeline/pipelines/collectors/backfill/reddit"

# Cria o diretório específico de logs do backfill caso não exista
mkdir -p "$LOG_DIR"

DATA_INICIO="2026-05-01"
DATA_FIM=$(date +%Y-%m-%d)

set -e

echo "=== Iniciando backfill do Reddit ($DATA_INICIO até $DATA_FIM) ==="

# Extrai as combinações do YAML usando Python embutido
COMBINACOES=$(python3 -c "
import yaml
import sys

try:
    with open('$YAML_PATH', 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
        subs = data.get('reddit', {}).get('subreddits', [])
        termos = data.get('reddit', {}).get('termo_busca', [])
        
        for s in subs:
            for t in termos:
                print(f'{s}|{t}')
except Exception as e:
    print(f'Erro ao ler YAML: {e}', file=sys.stderr)
    sys.exit(1)
")

echo "=== COMBINAÇÕES QUE SERÃO EXECUTADAS ==="
echo "$COMBINACOES" | while IFS='|' read -r s t; do
    echo "  - Subreddit: r/$s | Termo: $t"
done
echo "========================================="

TOTAL=$(echo "$COMBINACOES" | wc -l)
ATUAL=1

# Loop de execução sequencial
while IFS='|' read -r SUBREDDIT TERMO; do
    if [ -z "$SUBREDDIT" ] || [ -z "$TERMO" ]; then continue; fi

    echo ">>> [$ATUAL/$TOTAL] Coletando r/${SUBREDDIT} - termo: ${TERMO}"
    
    # Substitui espaços por underline para gerar o nome limpo do arquivo de log
    TERMO_SLUG=$(echo "$TERMO" | tr ' ' '_')
    LOG_FILE="${LOG_DIR}/backfill_${SUBREDDIT}_${TERMO_SLUG}.log"

    python3 -u -m pipelines.collectors.reddit_collector \
        --subreddit "${SUBREDDIT}" \
        --termo-busca "${TERMO}" \
        --data-inicio "$DATA_INICIO" \
        --data-fim "$DATA_FIM" | tee -a "$LOG_FILE"
        
    ATUAL=$((ATUAL + 1))
done <<< "$COMBINACOES"

echo "=== Backfill concluído! ==="