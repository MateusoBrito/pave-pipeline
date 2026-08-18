#!/bin/bash
# ==================================================================================
# BACKFILL HISTÓRICO - Reddit (Jan/2026 até o início do que a DAG diária já cobre)
# ==================================================================================
# Roda as 4 combinações (subreddit x candidato) UMA DE CADA VEZ (o && garante
# que só começa a próxima depois que a anterior termina), para não sobrecarregar
# a API pública do Arctic Shift.
#
# Uso:
#   cd ~/pave-pipeline/pipelines/collectors
#   chmod +x backfill_jan_ago.sh
#   nohup ./backfill_jan_ago.sh > backfill_jan_ago.log 2>&1 &
#
# Acompanhar o progresso:
#   tail -f backfill_jan_ago.log
#
# >>> Se quiser mudar o período coberto, ajuste as duas datas abaixo <<<
DATA_INICIO="2026-01-01"
DATA_FIM="2026-08-01"   # deve bater com o início do que a DAG diária já cobre, sem sobrepor nem deixar buraco

set -e  # para o script inteiro se algum comando falhar (evita continuar com erro silencioso)

echo "=== Iniciando backfill de $DATA_INICIO até $DATA_FIM ==="

echo ">>> [1/4] r/brasil - Lula"
python3 reddit_collector.py --subreddit brasil --termo-busca "Lula" \
    --data-inicio "$DATA_INICIO" --data-fim "$DATA_FIM"

echo ">>> [2/4] r/brasil - Bolsonaro"
python3 reddit_collector.py --subreddit brasil --termo-busca "Bolsonaro" \
    --data-inicio "$DATA_INICIO" --data-fim "$DATA_FIM"

echo ">>> [3/4] r/brasilivre - Lula"
python3 reddit_collector.py --subreddit brasilivre --termo-busca "Lula" \
    --data-inicio "$DATA_INICIO" --data-fim "$DATA_FIM"

echo ">>> [4/4] r/brasilivre - Bolsonaro"
python3 reddit_collector.py --subreddit brasilivre --termo-busca "Bolsonaro" \
    --data-inicio "$DATA_INICIO" --data-fim "$DATA_FIM"

echo "=== Backfill concluído! Rode 'python3 ../auditar_reddit.py' para conferir. ==="