from dotenv import load_dotenv
import os

load_dotenv()

# API
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not YOUTUBE_API_KEY:
    raise RuntimeError("YOUTUBE_API_KEY não encontrada no .env")

# YouTube
CHANNEL_IDS = [
    "UCvO2BExvkAbGMsTGnEnI_Ng",
    "UCl2HptoHv6PjZMQAwTdA--Q"
]
ANO = 2026
ARQUIVO_SAIDA_VIDEOS = f"data/videos_{ANO}.csv"
ARQUIVO_SAIDA_COMENTARIOS = f"data/comentarios_{ANO}.csv"
LIMITE_COMENTARIOS_VIDEO = 3000