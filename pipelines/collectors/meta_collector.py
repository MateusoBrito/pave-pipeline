import requests
import time
import csv
import os

#   export META_ACCESS_TOKEN="seu_token_aqui"    (Linux/Mac)
#   setx META_ACCESS_TOKEN "seu_token_aqui"      (Windows)

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

if not ACCESS_TOKEN:
    raise SystemExit(
        "Defina a variável de ambiente META_ACCESS_TOKEN antes de rodar o script."
    )

GRAPH_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"
ADS_URL = f"{BASE_URL}/ads_archive"

PERFIS = {
    "Lula": "267949976607343",
    "Flavio Bolsonaro": "156951837773645",
}

FIELDS = ",".join([
    "page_name",
    "page_id",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "ad_creative_bodies",
    "ad_creative_link_titles",
    "spend",
    "impressions",
])


def coletar_dados(page_id):
    params = {
        "access_token": ACCESS_TOKEN,
        "search_page_ids": f'["{page_id}"]',
        "ad_type": "POLITICAL_AND_ISSUE_ADS",
        "ad_reached_countries": '["BR"]',
        "fields": FIELDS,
        "limit": 100,
    }

    dados_totais = []
    next_url = ADS_URL
    first = True

    while next_url:
        if first:
            response = requests.get(next_url, params=params, timeout=30)
            first = False
        else:
            response = requests.get(next_url, timeout=30)

        if response.status_code != 200:
            print(f"Erro {response.status_code} para page_id={page_id}: {response.text}")
            break

        data = response.json()

        if "error" in data:
            print(f"Erro da API para page_id={page_id}: {data['error']}")
            break

        pagina = data.get("data", [])
        dados_totais.extend(pagina)
        print(f"[{page_id}] +{len(pagina)} anúncios (total: {len(dados_totais)})")

        next_url = data.get("paging", {}).get("next")
        time.sleep(1)  # respeita rate limit

    return dados_totais


def salvar_csv(dados, nome_arquivo):
    if not dados:
        print(f"Nenhum dado para salvar em {nome_arquivo}")
        return

    # Une todas as chaves possíveis (nem todo anúncio tem todos os campos)
    campos = set()
    for item in dados:
        campos.update(item.keys())
    campos = sorted(campos)

    with open(nome_arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        for item in dados:
            writer.writerow(item)

    print(f"Salvo: {nome_arquivo} ({len(dados)} registros)")


def main():
    for nome, page_id in PERFIS.items():
        print(f"\n=== Coletando anúncios: {nome} (page_id={page_id}) ===")
        dados = coletar_dados(page_id)
        nome_arquivo = f"ads_{nome.lower().replace(' ', '_')}.csv"
        salvar_csv(dados, nome_arquivo)


if __name__ == "__main__":
    main()