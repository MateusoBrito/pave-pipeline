# Monitoramento do pipeline

Gerado em: `2026-09-06T20:53:42.959723+00:00`

## Resumo por plataforma

| Plataforma | MongoDB | PostgreSQL | Sem pré-processar | Ausentes no PostgreSQL | Sem alvo | Sem ID | Tudo carregado |
|---|---:|---:|---:|---:|---:|---:|:---:|
| reddit | 20238 | 20132 | 65 | 53 | 53 | 0 | NÃO |
| meta | 3077 | 3077 | 0 | 0 | 0 | 0 | SIM |
| youtube | 477628 | 470422 | 7206 | 7206 | 0 | 0 | NÃO |

## Quantidade diária coletada

A data usa o timestamp UTC do `_id` do MongoDB, equivalente ao momento de coleta.

| Data UTC | Plataforma | Documentos no MongoDB |
|---|---|---:|
| 2026-08-17 | reddit | 3385 |
| 2026-08-18 | reddit | 2017 |
| 2026-08-19 | reddit | 402 |
| 2026-08-20 | reddit | 360 |
| 2026-08-21 | reddit | 390 |
| 2026-08-22 | reddit | 215 |
| 2026-08-23 | reddit | 156 |
| 2026-08-24 | reddit | 90 |
| 2026-08-25 | reddit | 324 |
| 2026-08-26 | reddit | 382 |
| 2026-08-27 | reddit | 2865 |
| 2026-08-28 | reddit | 377 |
| 2026-08-29 | reddit | 460 |
| 2026-08-30 | reddit | 93 |
| 2026-08-31 | reddit | 116 |
| 2026-09-01 | reddit | 64 |
| 2026-09-05 | reddit | 8332 |
| 2026-09-06 | reddit | 210 |
| 2026-08-18 | meta | 978 |
| 2026-08-21 | meta | 19 |
| 2026-08-22 | meta | 1 |
| 2026-08-23 | meta | 70 |
| 2026-08-24 | meta | 46 |
| 2026-08-25 | meta | 20 |
| 2026-08-26 | meta | 57 |
| 2026-08-27 | meta | 107 |
| 2026-08-28 | meta | 67 |
| 2026-08-29 | meta | 55 |
| 2026-08-30 | meta | 66 |
| 2026-08-31 | meta | 13 |
| 2026-09-01 | meta | 16 |
| 2026-09-04 | meta | 1304 |
| 2026-09-05 | meta | 186 |
| 2026-09-06 | meta | 72 |
| 2026-08-13 | youtube | 28240 |
| 2026-08-14 | youtube | 2474 |
| 2026-08-15 | youtube | 1196 |
| 2026-08-16 | youtube | 535 |
| 2026-08-17 | youtube | 9798 |
| 2026-08-18 | youtube | 2786 |
| 2026-08-19 | youtube | 732 |
| 2026-08-20 | youtube | 1517 |
| 2026-08-23 | youtube | 13209 |
| 2026-08-24 | youtube | 358973 |
| 2026-08-25 | youtube | 193 |
| 2026-08-27 | youtube | 4451 |
| 2026-08-28 | youtube | 4714 |
| 2026-08-29 | youtube | 4476 |
| 2026-08-30 | youtube | 7219 |
| 2026-08-31 | youtube | 2133 |
| 2026-09-01 | youtube | 5517 |
| 2026-09-05 | youtube | 1573 |
| 2026-09-06 | youtube | 27892 |

## Critérios

- `Sem pré-processar`: documento sem o campo `processed_text` no MongoDB.
- `Ausentes no PostgreSQL`: chave `(alvo_coleta_id, id_nativo)` presente no MongoDB e ausente na tabela `documento`.
- `Sem alvo`: documento cujo canal/termo não encontrou um `alvo_coleta` no PostgreSQL.
- `Tudo carregado`: não há documentos sem alvo, sem ID ou ausentes no PostgreSQL.
