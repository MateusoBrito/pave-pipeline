import os
import re
import sys
import torch
import numpy as np
import pandas as pd

from sklearn.utils.class_weight import compute_class_weight
from torch.nn import CrossEntropyLoss
from datasets import Dataset
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    Trainer,
    TrainingArguments,
    set_seed
)

print("Iniciando o treinamento com todas amostras...\n")

# 1. FUNÇÕES BASE E CONFIGURAÇÕES
set_seed(42)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

if "torchvision" in sys.modules:
    del sys.modules["torchvision"]

def preprocess_transformer(text):
    text = str(text)
    text = text.strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text

class WeightedTrainer(Trainer):
    def compute_loss(self, model, inputs, return_outputs=False, num_items_in_batch=None):
        labels = inputs.get("labels")
        outputs = model(**inputs)
        logits = outputs.get("logits")
        
        loss_fct = CrossEntropyLoss(weight=class_weights.to(logits.device))
        loss = loss_fct(logits, labels)
        
        return (loss, outputs) if return_outputs else loss

# 2. CARREGAR E PREPARAR OS DADOS
caminho_csv = "comentarios_classificados.csv"
df = pd.read_csv(caminho_csv)
df = df[df['classificacao'] != 'PENDENTE'].copy()

df['textClean'] = df['texto'].str.replace(r"http\S+|www\S+", " ", regex=True)
df['textClean'] = df['textClean'].apply(preprocess_transformer)

label_map = {"Negativo": 0, "Neutro": 1, "Positivo": 2}
df['feeling'] = df['classificacao'].map(label_map)

X_train_full = df['textClean']
y_train_full = df['feeling']

print(f"Total de comentários: {len(X_train_full)}")

# 3. PESOS E TOKENIZAÇÃO
# Calcula os pesos usando a amostra INTEIRA
classes = np.unique(y_train_full)
weights = compute_class_weight(class_weight='balanced', classes=classes, y=y_train_full)
class_weights = torch.tensor(weights, dtype=torch.float)

nome_modelo = "neuralmind/bert-base-portuguese-cased"
tokenizer = AutoTokenizer.from_pretrained(nome_modelo)

def tokenize_batch(batch):
    return tokenizer(batch["text"], truncation=True, padding=True, max_length=128)

train_dataset = Dataset.from_pandas(pd.DataFrame({'text': X_train_full, 'label': y_train_full}), preserve_index=False)
train_dataset = train_dataset.map(tokenize_batch, batched=True)
train_dataset.set_format(type="torch", columns=["input_ids", "attention_mask", "label"])

# 4. TREINAMENTO
model = AutoModelForSequenceClassification.from_pretrained(nome_modelo, num_labels=3).to(device)

# Melhores hiperparâmetros via Optuna
training_args = TrainingArguments(
    output_dir="./temp_results",
    learning_rate=1.827226177606625e-05,
    per_device_train_batch_size=8,
    num_train_epochs=4,
    weight_decay=0.02404167763981929,
    logging_steps=50,
    save_strategy="no", 
    evaluation_strategy="no",
    seed=42,
    disable_tqdm=False, # Ver o progresso
    fp16=True if torch.cuda.is_available() else False
)

trainer = WeightedTrainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset
)

print("\nTreinando o modelo...")
trainer.train()

# 5. SALVANDO O MODELO
pasta_destino = "./meu_bertimbau"

print(f"\nSalvando o modelo treinado e o tokenizador na pasta: {pasta_destino}")

# Salva o "cérebro" (pesos) do modelo
trainer.save_model(pasta_destino)

# Salva o tokenizador junto
tokenizer.save_pretrained(pasta_destino)

print("Modelo salvo!")
