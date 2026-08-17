from typing import List
from pathlib import Path
import numpy as np
import torch

from sentence_transformers import SentenceTransformer

def generate_embeddings(
    documents: List[str],
    #cache_path: Path,
    model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"
) -> np.ndarray:
    print("2. Gerando embeddings...")

    # if cache_path.exists():
    #     print(f"   -> Carregando da cache: {cache_path}")
    #     return np.load(cache_path)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"   -> Dispositivo: {device}")

    model = SentenceTransformer(model_name, device=device)
    embeddings = model.encode(
        documents,
        batch_size=128,
        show_progress_bar=True,
        convert_to_numpy=True
    )

    # cache_path.parent.mkdir(parents=True, exist_ok=True)
    # np.save(cache_path, embeddings)
    # print(f"   -> Cache salva em: {cache_path}")

    return embeddings