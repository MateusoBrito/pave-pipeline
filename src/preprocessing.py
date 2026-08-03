# src/preprocessing.py

import re
import string
import os
import nltk
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag

class TextPreprocessor:
    def __init__(self, min_word_len=3, max_repetition=2, custom_stopwords_path=None):
        """
        Inicializa o pré-processador.
        Args:
            min_word_len (int): Tamanho mínimo da palavra a ser mantida.
            max_repetition (int): Número máximo de caracteres repetidos permitidos (ex: 2 transforma 'oiii' em 'oii').
            custom_stopwords_path (str): Caminho para arquivo txt extra de stopwords.
        """
        self.min_word_len = min_word_len
        self.max_repetition = max(1, max_repetition) # Garante que seja pelo menos 1
        self.lemmatizer = WordNetLemmatizer()
        
        # Garante recursos e carrega stopwords iniciais
        self._download_nltk_resources()
        self.stop_words = set(stopwords.words('english'))
        
        # Carrega stopwords extras se o caminho foi passado
        if custom_stopwords_path:
            self._load_custom_stopwords(custom_stopwords_path)

    def _download_nltk_resources(self):
        """Método interno para garantir downloads."""
        resources = [
            'stopwords', 
            'wordnet', 
            'averaged_perceptron_tagger',
            'averaged_perceptron_tagger_eng',  
            'omw-1.4', 
            'punkt'
        ]
        
        for resource in resources:
            try:
                nltk.data.find(f'corpora/{resource}')
            except LookupError:
                try:
                    nltk.data.find(f'tokenizers/{resource}')
                except LookupError:
                    try:
                        nltk.data.find(f'taggers/{resource}')
                    except LookupError:
                        nltk.download(resource, quiet=True)

    def _load_custom_stopwords(self, filepath):
        """Carrega stopwords adicionais e atualiza o set da instância."""
        if not os.path.exists(filepath):
            print(f"Aviso: Arquivo '{filepath}' não encontrado.")
            return

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                custom_words = {line.strip().lower() for line in f if line.strip()}
            self.stop_words.update(custom_words)
            print(f"Adicionadas {len(custom_words)} stopwords personalizadas.")
        except Exception as e:
            print(f"Erro ao ler stopwords: {e}")

    # --- Métodos que operam sobre STRING ---

    def _remove_urls(self, text):
        return re.sub(r'http\S+|www\.\S+', '', text)

    def _remove_tags(self, text):
        return re.sub(r'#\w+|@\w+', '', text)

    def _remove_punctuation(self, text):
        return ''.join(char for char in text if char not in string.punctuation)

    def _remove_numbers(self, text):
        return re.sub(r'\d+', '', text)

    def _remove_repetitions(self, text):
        """
        Remove caracteres repetidos baseado no self.max_repetition.
        Se max_repetition = 2: 'goood' vira 'good'.
        """
        # A regex procura por um caractere (.) e suas repetições (\1)
        # Se max=2, queremos encontrar onde ocorre 3 ou mais vezes ({2,}) para substituir
        if self.max_repetition < 1: return text
        
        # Constroi o padrão dinamicamente. Ex para 2: r'(.)\1{2,}'
        pattern = r'(.)\1{' + str(self.max_repetition) + r',}'
        
        # Constroi a substituição. Ex para 2: r'\1\1'
        replacement = r'\1' * self.max_repetition
        
        return re.sub(pattern, replacement, text)

    # --- Métodos que operam sobre LISTA DE TOKENS ---

    def _remove_stopwords(self, tokens):
        """Remove stopwords de uma lista de tokens."""
        return [word for word in tokens if word not in self.stop_words]

    def _remove_small_words(self, tokens):
        """Remove palavras com menos de min_word_len caracteres."""
        return [word for word in tokens if len(word) >= self.min_word_len]

    def _get_wordnet_pos(self, tag):
        if tag.startswith('J'): return wordnet.ADJ
        elif tag.startswith('V'): return wordnet.VERB
        elif tag.startswith('N'): return wordnet.NOUN
        elif tag.startswith('R'): return wordnet.ADV
        else: return wordnet.NOUN

    def _lemmatize_tokens(self, tokens):
        pos_tags = pos_tag(tokens)
        return [self.lemmatizer.lemmatize(w, self._get_wordnet_pos(t)) for w, t in pos_tags]

    # --- Orquestrador ---

    def preprocess(self, text):
        """
        Função principal que orquestra todo o pipeline de limpeza.
        """
        # 1. Limpezas de String
        text = str(text).lower()
        text = self._remove_urls(text)
        text = self._remove_tags(text)
        text = self._remove_punctuation(text)
        text = self._remove_numbers(text)
        text = self._remove_repetitions(text)
        
        # 2. Tokenização
        tokens = text.split()
        
        # 3. Filtragem e Lematização (Operações em lista)
        tokens = self._remove_stopwords(tokens)
        tokens = self._remove_small_words(tokens) 
        tokens = self._lemmatize_tokens(tokens)
        
        return ' '.join(tokens)