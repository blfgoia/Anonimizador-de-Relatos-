import os
import re
import json
import base64
import hashlib
import logging
from datetime import datetime, timezone

# Aqui eu importo o cliente do Google Cloud Storage,
# que vou usar para acessar arquivos dentro do bucket
from google.cloud import storage


# =========================
# CONFIGURAÇÕES
# =========================

# Aqui eu defino o nome da aplicação
APP_NAME = "anonimizador-relatos"

# Aqui eu defino a pasta de entrada onde os arquivos chegam
INPUT_PREFIX = os.getenv("INPUT_PREFIX", "entrada/")

# Aqui eu defino a pasta onde salvarei os textos anonimizados
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "anonimizados/")

# Aqui eu defino a pasta onde salvarei os metadados do processamento
META_PREFIX = os.getenv("META_PREFIX", "metadados/")

# Aqui eu defino qual modelo do spaCy será usado para detectar entidades
SPACY_MODEL = os.getenv("SPACY_MODEL", "pt_core_news_sm")

# Aqui eu limito o tamanho máximo dos arquivos que o sistema aceita
MAX_FILE_SIZE_MB = float(os.getenv("MAX_FILE_SIZE_MB", "5"))

# Aqui eu defino quais extensões de arquivos são permitidas
ALLOWED_EXTENSIONS = {
    ext.strip().lower()
    for ext in os.getenv("ALLOWED_EXTENSIONS", ".txt,.md,.csv").split(",")
    if ext.strip()
}


# =========================
# LOG
# =========================

# Aqui eu configuro o sistema de logs para registrar o que está acontecendo
logging.basicConfig(level=logging.INFO)

# Aqui eu crio um logger usando o nome da aplicação
logger = logging.getLogger(APP_NAME)


# =========================
# CLIENTE GCS
# =========================

# Aqui eu inicializo o cliente que permite acessar o Google Cloud Storage
storage_client = storage.Client()


# =========================
# SPACY
# =========================

# Aqui eu inicializo variáveis que controlam o uso do spaCy
NLP = None
SPACY_AVAILABLE = False
SPACY_MODEL_LOADED = None


def carregar_spacy():

    # Aqui eu declaro que vou modificar essas variáveis globais
    global NLP, SPACY_AVAILABLE, SPACY_MODEL_LOADED

    # Aqui eu tento importar o spaCy
    try:
        import spacy
    except Exception as e:

        # Se falhar, eu desativo o uso do spaCy
        NLP = None
        SPACY_AVAILABLE = False
        SPACY_MODEL_LOADED = None

        # Aqui eu aviso no log que spaCy não está disponível
        logger.warning(f"spaCy não disponível. Regex seguirá ativa. Motivo: {e}")
        return


    # Aqui eu tento carregar o modelo definido nas configurações
    try:

        NLP = spacy.load(SPACY_MODEL)
        SPACY_AVAILABLE = True
        SPACY_MODEL_LOADED = SPACY_MODEL

        logger.info(f"spaCy carregado com sucesso: {SPACY_MODEL}")
        return

    except Exception as e:

        # Se o modelo não carregar eu aviso no log
        logger.warning(f"Modelo {SPACY_MODEL} não carregado: {e}")


    # Aqui eu tento baixar automaticamente o modelo spaCy
    try:

        from spacy.cli import download

        logger.info(f"Tentando baixar modelo spaCy: {SPACY_MODEL}")

        download(SPACY_MODEL)

        NLP = spacy.load(SPACY_MODEL)
        SPACY_AVAILABLE = True
        SPACY_MODEL_LOADED = SPACY_MODEL

        logger.info(f"spaCy baixado e carregado com sucesso: {SPACY_MODEL}")
        return

    except Exception as e:

        # Se não conseguir baixar o modelo eu registro o erro
        logger.warning(f"Falha ao baixar/carregar {SPACY_MODEL}: {e}")


    # Aqui eu tento um modelo alternativo em inglês como fallback
    try:

        fallback_model = "en_core_web_sm"

        logger.info(f"Tentando fallback spaCy: {fallback_model}")

        NLP = spacy.load(fallback_model)
        SPACY_AVAILABLE = True
        SPACY_MODEL_LOADED = fallback_model

        logger.info(f"spaCy carregado com fallback: {fallback_model}")
        return

    except Exception as e:

        logger.warning(f"Fallback spaCy também falhou: {e}")


    # Se nada funcionar, eu desativo completamente o spaCy
    NLP = None
    SPACY_AVAILABLE = False
    SPACY_MODEL_LOADED = None

    logger.warning("Nenhum modelo spaCy foi carregado. Regex seguirá ativa.")


# Aqui eu executo a função que carrega o spaCy quando o sistema inicia
carregar_spacy()