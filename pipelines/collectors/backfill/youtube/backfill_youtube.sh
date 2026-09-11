#!/bin/bash
# ==================================================================================
# BACKFILL HISTÓRICO DINÂMICO - YouTube (Mai/2026 até Hoje)
# Localização: backfill/youtube/run_backfill.sh
# ==================================================================================

# 1. Garante que o script rode a partir da raiz do projeto, não importa de onde seja chamado
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAIZ_PROJETO="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$RAIZ_PROJETO"

# Adiciona a raiz do projeto ao PYTHONPATH para permitir a importação de módulos com 'python3 -m'
export PYTHONPATH="$RAIZ_PROJETO:$PYTHONPATH"

# 2. Caminhos relativos a partir da raiz do projeto
YAML_PATH="config/entities.yaml"
LOG_DIR="/home/labpi/pave-pipeline/pipelines/collectors/backfill/youtube"

# Cria o diretório específico de logs do backfill caso não exista
mkdir -p "$LOG_DIR"

DATA_INICIO="2026-05-01"
DATA_FIM=$(date +%Y-%m-%d)

set -e

echo "=== Iniciando backfill do YouTube ($DATA_INICIO até $DATA_FIM) ==="

# Extrai os nomes dos canais do YAML usando Python embutido
CANAIS=$(python3 -c "
import yaml
import sys

try:
    with open('$YAML_PATH', 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
        canais = data.get('youtube', {}).get('canais', [])
        
        for c in canais:
            nome = c.get('nome')
            if nome:
                print(nome)
except Exception as e:
    print(f'Erro ao ler YAML: {e}', file=sys.stderr)
    sys.exit(1)
")

echo "=== CANAIS QUE SERÃO EXECUTADOS ==="
echo "$CANAIS" | while read -r canal; do
    echo "  - Canal: $canal"
done
echo "========================================="

TOTAL=$(echo "$CANAIS" | wc -l)
ATUAL=1

# Loop de execução sequencial
while read -r CANAL; do
    if [ -z "$CANAL" ]; then continue; fi

    echo ">>> [$ATUAL/$TOTAL] Coletando canal: ${CANAL}"
    
    # Substitui espaços por underline para gerar o nome limpo do arquivo de log do backfill
    CANAL_SLUG=$(echo "$CANAL" | tr ' ' '_')
    LOG_FILE="${LOG_DIR}/backfill_youtube_${CANAL_SLUG}.log"

    python3 -u -m pipelines.collectors.youtube_collector \
        --entidade "${CANAL}" \
        --data-inicio "$DATA_INICIO" \
        --data-fim "$DATA_FIM" | tee -a "$LOG_FILE"
        
    ATUAL=$((ATUAL + 1))
done <<< "$CANAIS"

echo "=== Backfill do YouTube concluído! ==="