FROM apache/airflow:2.8.1-python3.11
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    pymongo \
    requests \
    pyyaml \
    google-api-python-client \
    pandas \
    nltk \
    sentence-transformers \
    torch

# Download NLTK data during build to speed up execution
RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('averaged_perceptron_tagger'); nltk.download('averaged_perceptron_tagger_eng'); nltk.download('omw-1.4'); nltk.download('punkt');"

# (Optional) Pre-download the embedding model to avoid downloading it on every run
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"
