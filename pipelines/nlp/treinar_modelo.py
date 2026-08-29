import os
import re
import sys
import random
import numpy as np
import pandas as pd
import torch

from torch.nn import CrossEntropyLoss
from datasets import Dataset
from sklearn.model_selection import StratifiedKFold
from sklearn.utils.class_weight import compute_class_weight
from sklearn.metrics import f1_score, accuracy_score
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    Trainer,
    TrainingArguments,
    set_seed
)

# 1. CONFIGURAÇÕES GLOBAIS E SEEDS
def fixar_todas_as_seeds(seed=42):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    os.environ['CUBLAS_WORKSPACE_CONFIG'] = ':4096:8'
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    torch.use_deterministic_algorithms(True, warn_only=True)
    set_seed(seed)
    print(f"Todas as seeds globais foram fixadas")

fixar_todas_as_seeds(42)

# Configuração de GPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Rodando no dispositivo: {device}")
if torch.cuda.is_available():
    print(f"Placa de vídeo: {torch.cuda.get_device_name(0)}\n")

# Blindagem contra bug do Hugging Face / Torchvision
if "torchvision" in sys.modules:
    del sys.modules["torchvision"]

# 2. FUNÇÕES BASE E CLASSES
def preprocess_transformer(text):
    text = str(text)
    text = text.strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text

class WeightedTrainer(Trainer):
    def compute_loss(
        self,
        model,
        inputs,
        return_outputs=False,
        num_items_in_batch=None
    ):
        labels = inputs.get("labels")
        outputs = model(**inputs)
        logits = outputs.get("logits")

        loss_fct = CrossEntropyLoss(
            weight=class_weights.to(logits.device)
        )

        loss = loss_fct(logits, labels)
        return (loss, outputs) if return_outputs else loss

# 3. CARREGAMENTO E PREPARAÇÃO DOS DADOS
print("\nCarregando o dataset...")
caminho_csv = "comentarios_classificados.csv"

try:
    df = pd.read_csv(caminho_csv)
except FileNotFoundError:
    print(f"O arquivo '{caminho_csv}' não foi encontrado.")
    sys.exit(1)

# Verifica comentários PENDENTES
quantidade_pendentes = (df['classificacao'] == 'PENDENTE').sum()
print(f"Total de comentários PENDENTES encontrados: {quantidade_pendentes}")

# 1. Filtra removendo comentários não desempatados
if quantidade_pendentes > 0:
    print("Removendo comentários pendentes para o treinamento...")
    df = df[df['classificacao'] != 'PENDENTE'].copy()
else:
    print("Nenhum empate pendente na base.")

print("Executando limpeza de textos...")
# 2. Limpeza de links
df['textClean'] = df['texto'].str.replace(r"http\S+|www\S+", " ", regex=True)

# 3. Aplica a função de limpeza
df['textClean'] = df['textClean'].apply(preprocess_transformer)

# 4. Codificação dos rótulos
label_map = {"Negativo": 0, "Neutro": 1, "Positivo": 2}
df['feeling'] = df['classificacao'].map(label_map)

# 4. PARTICIONAMENTO DA BASE (K-FOLD)
print("\nCriando os splits do K-Fold (5 folds)...")
X = df['textClean']
y = df['feeling']

# Configura o K-Fold para 5 divisões
skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

# Gera a lista de splits
splits = list(skf.split(X, y))

print(f"Base dividida com sucesso em {len(splits)} Folds")
print(f"Total de comentários válidos na base final: {len(X)}\n")

# 5. TREINAMENTO E AVALIAÇÃO (BERTimbau)
print("Iniciando Validação Cruzada (5 Folds) para o BERTimbau...\n")

nome_modelo = "neuralmind/bert-base-portuguese-cased"
bert_tokenizer = AutoTokenizer.from_pretrained(nome_modelo)

def tokenize_bert(batch):
    return bert_tokenizer(batch["text"], truncation=True, padding=True, max_length=128)

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    f1 = f1_score(labels, predictions, average='macro')
    acc = accuracy_score(labels, predictions)
    return {"f1": f1, "accuracy": acc}

# Cofrinhos para o resultado final
f1_scores_bert = []
acc_scores_bert = []

# Variável global dos pesos
class_weights = None

# Laço do K-Fold
for fold, (train_idx, test_idx) in enumerate(splits):
    print(f"\n{'='*15} Iniciando Fold {fold + 1}/5 {'='*15}")

    # Separar os dados
    X_train_fold, X_test_fold = X.iloc[train_idx], X.iloc[test_idx]
    y_train_fold, y_test_fold = y.iloc[train_idx], y.iloc[test_idx]

    # Calcular pesos e atualizar variável global
    classes = np.unique(y_train_fold)
    weights = compute_class_weight(class_weight='balanced', classes=classes, y=y_train_fold)
    class_weights = torch.tensor(weights, dtype=torch.float)

    # Criar e Tokenizar Datasets
    train_dataset = Dataset.from_pandas(pd.DataFrame({'text': X_train_fold, 'label': y_train_fold}), preserve_index=False)
    test_dataset = Dataset.from_pandas(pd.DataFrame({'text': X_test_fold, 'label': y_test_fold}), preserve_index=False)

    train_dataset = train_dataset.map(tokenize_bert, batched=True)
    test_dataset = test_dataset.map(tokenize_bert, batched=True)

    train_dataset.set_format(type="torch", columns=["input_ids", "attention_mask", "label"])
    test_dataset.set_format(type="torch", columns=["input_ids", "attention_mask", "label"])

    # Semente matemática recarregada ANTES do modelo
    set_seed(42)
    model = AutoModelForSequenceClassification.from_pretrained(nome_modelo, num_labels=3).to(device)

    # Hiperparâmetros encontrados na busca do Optuna
    training_args = TrainingArguments(
        output_dir=f"./results_bert_fold_{fold}",
        learning_rate=1.827226177606625e-05,
        per_device_train_batch_size=8,
        num_train_epochs=4,
        weight_decay=0.02404167763981929,
        logging_steps=50,
        save_strategy="no",
        eval_strategy="no",
        seed=42,
        disable_tqdm=True,
        fp16=True if torch.cuda.is_available() else False
    )

    trainer = WeightedTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=test_dataset,
        compute_metrics=compute_metrics
    )

    # Treinar e Avaliar
    trainer.train()
    preds_output = trainer.predict(test_dataset)
    preds = np.argmax(preds_output.predictions, axis=1)
    y_test_real = preds_output.label_ids

    f1 = f1_score(y_test_real, preds, average='macro')
    acc = accuracy_score(y_test_real, preds)

    print(f"Fold {fold + 1} Concluído -> F1 Macro: {f1:.4f} | Acurácia: {acc:.4f}")

    # Atualiza cofrinhos
    f1_scores_bert.append(f1)
    acc_scores_bert.append(acc)

# 6. RESULTADO FINAL
print("\n" + "=" * 45)
print("RESULTADO FINAL DO K-FOLD (BERTimbau)")
print(f"F1 Macro Médio: {np.mean(f1_scores_bert):.4f} (+/- {np.std(f1_scores_bert):.4f})")
print(f"Acurácia Média: {np.mean(acc_scores_bert):.4f} (+/- {np.std(acc_scores_bert):.4f})")
print("=" * 45)