import os
import re
import json
import base64
import hashlib
import logging
from datetime import datetime, timezone

from google.cloud import storage

# =========================
# CONFIGURAÇÕES
# =========================

# Nome da aplicação
APP_NAME = "anonimizador-relatos"

# Pasta de entrada dos arquivos no bucket
INPUT_PREFIX = os.getenv("INPUT_PREFIX", "entrada/")

# Pasta onde os arquivos anonimizados serão salvos
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "anonimizados/")

# Pasta usada para salvar os metadados do processamento
META_PREFIX = os.getenv("META_PREFIX", "metadados/")

# Modelo spaCy definido por variável de ambiente
SPACY_MODEL = os.getenv("SPACY_MODEL", "pt_core_news_sm")

# Tamanho máximo permitido para cada arquivo
MAX_FILE_SIZE_MB = float(os.getenv("MAX_FILE_SIZE_MB", "5"))

# Extensões aceitas pelo sistema
ALLOWED_EXTENSIONS = {
    ext.strip().lower()
    for ext in os.getenv("ALLOWED_EXTENSIONS", ".txt,.md,.csv").split(",")
    if ext.strip()
}

# =========================
# LOG
# =========================

# Configuração básica dos logs
logging.basicConfig(level=logging.INFO)

# Logger principal da aplicação
logger = logging.getLogger(APP_NAME)

# =========================
# CLIENTE GCS
# =========================

# Cliente usado para acessar o Google Cloud Storage
storage_client = storage.Client()

# =========================
# SPACY
# =========================

# Variáveis de controle do spaCy
NLP = None
SPACY_AVAILABLE = False
SPACY_MODEL_LOADED = None

def carregar_spacy():
    global NLP, SPACY_AVAILABLE, SPACY_MODEL_LOADED

    # Tenta importar a biblioteca spaCy
    try:
        import spacy
    except Exception as e:
        # Se não conseguir importar, o sistema segue apenas com regex
        NLP = None
        SPACY_AVAILABLE = False
        SPACY_MODEL_LOADED = None
        logger.warning(f"spaCy não disponível. Regex seguirá ativa. Motivo: {e}")
        return

    # Primeira tentativa: carregar o modelo definido nas configurações
    try:
        NLP = spacy.load(SPACY_MODEL)
        SPACY_AVAILABLE = True
        SPACY_MODEL_LOADED = SPACY_MODEL
        logger.info(f"spaCy carregado com sucesso: {SPACY_MODEL}")
        return
    except Exception as e:
        logger.warning(f"Modelo {SPACY_MODEL} não carregado: {e}")

    # Segunda tentativa: baixar o modelo automaticamente e carregar em seguida
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
        logger.warning(f"Falha ao baixar/carregar {SPACY_MODEL}: {e}")

    # Terceira tentativa: usar um modelo alternativo em inglês
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

    # Se nada funcionar, o processamento continua apenas com expressões regulares
    NLP = None
    SPACY_AVAILABLE = False
    SPACY_MODEL_LOADED = None
    logger.warning("Nenhum modelo spaCy foi carregado. Regex seguirá ativa.")

# Carrega o spaCy na inicialização do serviço
carregar_spacy()

# =========================
# REGEX
# =========================

# Padrões usados para localizar dados sensíveis no texto
REGEX_PATTERNS = {
    "EMAIL": re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"),
    "URL": re.compile(r"\b(?:https?://|www\.)\S+\b", re.IGNORECASE),
    "CPF": re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b"),
    "CNPJ": re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b"),
    "RG": re.compile(r"\b\d{1,2}\.?\d{3}\.?\d{3}-?[\dXx]\b"),
    "CEP": re.compile(r"\b\d{5}-?\d{3}\b"),
    "TELEFONE": re.compile(r"\b(?:\+?55\s?)?(?:\(?\d{2}\)?\s?)?(?:9?\d{4})-?\d{4}\b"),
    "DATA": re.compile(
        r"\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s+de\s+[A-Za-zçÇãõáéíóúâêô]+\s+de\s+\d{2,4})\b",
        re.IGNORECASE
    ),
    "HORA": re.compile(r"\b(?:[01]?\d|2[0-3]):[0-5]\d\b"),
    "CARTAO_SUS": re.compile(r"\b\d{15}\b"),
    "ENDERECO": re.compile(
        r"\b(?:Rua|R\.|Avenida|Av\.|Travessa|Trav\.|Alameda|Praça|Praca|Rodovia|Setor|Quadra|QD|Lote|Lt)\s+[A-Za-z0-9À-ÿº°\-\., ]{2,100}",
        re.IGNORECASE
    ),
    "BAIRRO": re.compile(
        r"\b(?:bairro|setor|jardim|residencial|vila)\s+[A-Za-zÀ-ÿ0-9\-\s]{2,60}",
        re.IGNORECASE
    ),
    "HOSPITAL_CLINICA": re.compile(
        r"\b(?:Hospital|Clínica|Clinica|UBS|UPA|CAPS|Posto de Saúde|Posto de Saude)\s+[A-Za-zÀ-ÿ0-9\-\s]{2,80}",
        re.IGNORECASE
    ),
}

# Tradução das entidades do spaCy para rótulos mais claros
SPACY_ENTITY_MAP = {
    "PER": "PESSOA",
    "PERSON": "PESSOA",
    "LOC": "LOCAL",
    "GPE": "LOCAL",
    "ORG": "ORGANIZACAO",
}

# =========================
# UTILITÁRIOS
# =========================

# Retorna data e hora atual em UTC no padrão ISO
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

# Gera hash SHA-256 de um texto
def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# Gera hash SHA-256 de um conteúdo em bytes
def sha256_bytes(content):
    return hashlib.sha256(content).hexdigest()

# Cria um identificador único para rastrear o arquivo processado
def make_trace_id(bucket, blob_name, generation=""):
    base = f"{bucket}|{blob_name}|{generation}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

# Verifica se a extensão do arquivo é permitida
def extensao_permitida(blob_name):
    _, ext = os.path.splitext(blob_name.lower())
    return ext in ALLOWED_EXTENSIONS

# Verifica se o objeto representa apenas uma pasta
def is_folder_placeholder(blob_name):
    return blob_name.endswith("/")

# Confere se o arquivo está dentro da pasta de entrada
def is_input_file(blob_name):
    return blob_name.startswith(INPUT_PREFIX)

# Impede reprocessamento de arquivos que já estão na saída ou em metadados
def is_output_or_meta(blob_name):
    return blob_name.startswith(OUTPUT_PREFIX) or blob_name.startswith(META_PREFIX)

# Padroniza os nomes do arquivo anonimizado e do arquivo de metadados
def normalizar_nome_saida(blob_name, trace_id):
    nome_base = os.path.basename(blob_name)
    raiz, ext = os.path.splitext(nome_base)
    raiz_segura = re.sub(r"[^a-zA-Z0-9_-]+", "_", raiz).strip("_") or "arquivo"
    ext = ext if ext else ".txt"

    output_name = f"{OUTPUT_PREFIX}{raiz_segura}_anon_{trace_id[:8]}{ext}"
    meta_name = f"{META_PREFIX}{raiz_segura}_meta_{trace_id[:8]}.json"
    return output_name, meta_name

# Remove trechos sobrepostos para evitar substituições repetidas
def remover_sobreposicoes(spans):
    if not spans:
        return []

    spans = sorted(spans, key=lambda x: (x["start"], -(x["end"] - x["start"])))
    resultado = []
    ultimo_fim = -1

    for span in spans:
        if span["start"] >= ultimo_fim:
            resultado.append(span)
            ultimo_fim = span["end"]

    return resultado

# Gera placeholders numerados como [CPF_1], [EMAIL_1] e assim por diante
def gerar_placeholder(label, counters):
    counters[label] = counters.get(label, 0) + 1
    return f"[{label}_{counters[label]}]"

# =========================
# EXTRAÇÃO DE EVENTO
# =========================

# Tenta decodificar uma string base64 contendo JSON
def try_decode_base64_json(value):
    try:
        raw = base64.b64decode(value).decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None

# Extrai bucket e nome do arquivo a partir de diferentes formatos de evento
def extract_storage_event(payload):
    logger.info(f"Payload recebido: {json.dumps(payload, ensure_ascii=False)[:3000]}")

    # Caso o payload já traga bucket e nome diretamente
    if payload.get("bucket") and payload.get("name"):
        return payload["bucket"], payload["name"], payload

    # Caso as informações estejam dentro do campo data
    if isinstance(payload.get("data"), dict):
        data = payload["data"]
        if data.get("bucket") and data.get("name"):
            return data["bucket"], data["name"], data

    # Caso o evento venha via Pub/Sub com conteúdo codificado
    message = payload.get("message")
    if isinstance(message, dict) and message.get("data"):
        decoded = try_decode_base64_json(message["data"])
        if decoded:
            if decoded.get("bucket") and decoded.get("name"):
                return decoded["bucket"], decoded["name"], decoded
            if isinstance(decoded.get("data"), dict):
                d = decoded["data"]
                if d.get("bucket") and d.get("name"):
                    return d["bucket"], d["name"], d

    # Caso o evento venha em formato de auditoria
    proto = payload.get("protoPayload", {})
    resource_name = proto.get("resourceName", "")
    m = re.search(r"buckets/([^/]+)/objects/(.+)$", resource_name)
    if m:
        bucket = m.group(1)
        object_name = m.group(2).replace("%2F", "/").replace("+", " ")
        return bucket, object_name, proto

    # Se o evento não for reconhecido, retorna vazio
    return None, None, payload

# =========================
# ANONIMIZAÇÃO
# =========================

# Procura dados sensíveis no texto usando regex
def coletar_spans_regex(texto):
    spans = []
    for label, pattern in REGEX_PATTERNS.items():
        for match in pattern.finditer(texto):
            spans.append({
                "start": match.start(),
                "end": match.end(),
                "label": label,
                "text": match.group(0)
            })
    return spans

# Procura entidades usando spaCy quando disponível
def coletar_spans_spacy(texto):
    spans = []
    if not SPACY_AVAILABLE or NLP is None:
        return spans

    try:
        doc = NLP(texto)
        for ent in doc.ents:
            mapped = SPACY_ENTITY_MAP.get(ent.label_)
            if mapped:
                spans.append({
                    "start": ent.start_char,
                    "end": ent.end_char,
                    "label": mapped,
                    "text": ent.text
                })
    except Exception as e:
        logger.warning(f"Erro no spaCy: {e}")

    return spans

# Substitui dados sensíveis por placeholders e guarda o histórico das trocas
def anonimizar_texto(texto):
    spans = coletar_spans_regex(texto) + coletar_spans_spacy(texto)
    spans = remover_sobreposicoes(spans)

    counters = {}
    replacements = []
    partes = []
    cursor = 0

    for span in spans:
        inicio, fim = span["start"], span["end"]
        if inicio < cursor:
            continue

        label = span["label"]
        placeholder = gerar_placeholder(label, counters)

        partes.append(texto[cursor:inicio])
        partes.append(placeholder)

        replacements.append({
            "label": label,
            "original": span["text"],
            "placeholder": placeholder,
            "start": inicio,
            "end": fim
        })

        cursor = fim

    partes.append(texto[cursor:])
    texto_anon = "".join(partes)
    return texto_anon, replacements

# =========================
# PROCESSAMENTO
# =========================

# Faz a validação, leitura, anonimização e gravação do arquivo processado
def processar_arquivo(bucket_name, blob_name, event_data):
    # Ignora objetos que representam apenas pasta
    if is_folder_placeholder(blob_name):
        return {"status": "ignored", "reason": "placeholder_de_pasta"}

    # Ignora arquivos que já pertencem à saída ou aos metadados
    if is_output_or_meta(blob_name):
        return {"status": "ignored", "reason": "arquivo_em_saida_ou_metadado"}

    # Garante que somente arquivos da pasta de entrada sejam processados
    if not is_input_file(blob_name):
        return {"status": "ignored", "reason": "fora_da_pasta_entrada"}

    # Ignora formatos não permitidos
    if not extensao_permitida(blob_name):
        return {"status": "ignored", "reason": "extensao_nao_permitida"}

    # Acessa o bucket e o arquivo recebido
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.reload()

    # Verifica se o arquivo ultrapassa o limite de tamanho
    size_mb = (blob.size or 0) / (1024 * 1024)
    if size_mb > MAX_FILE_SIZE_MB:
        return {"status": "ignored", "reason": "arquivo_muito_grande"}

    # Baixa o conteúdo do arquivo
    conteudo_bytes = blob.download_as_bytes()

    # Tenta decodificar primeiro em UTF-8 e depois em latin-1
    try:
        conteudo = conteudo_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            conteudo = conteudo_bytes.decode("latin-1")
        except Exception:
            return {"status": "error", "reason": "falha_ao_decodificar_arquivo"}

    # Gera identificador único e processa a anonimização
    trace_id = make_trace_id(bucket_name, blob_name, str(blob.generation))
    texto_anon, replacements = anonimizar_texto(conteudo)
    output_blob_name, meta_blob_name = normalizar_nome_saida(blob_name, trace_id)

    # Salva o texto anonimizado
    bucket.blob(output_blob_name).upload_from_string(
        texto_anon,
        content_type="text/plain; charset=utf-8"
    )

    # Monta os metadados do processamento
    metadata = {
        "trace_id": trace_id,
        "status": "processed",
        "processed_at": utc_now_iso(),
        "bucket": bucket_name,
        "input_blob": blob_name,
        "output_blob": output_blob_name,
        "meta_blob": meta_blob_name,
        "generation": str(blob.generation),
        "size_bytes": blob.size,
        "original_sha256": sha256_bytes(conteudo_bytes),
        "anonymized_sha256": sha256_text(texto_anon),
        "spacy_enabled": SPACY_AVAILABLE,
        "spacy_model": SPACY_MODEL_LOADED,
        "counts": {
            "replacements_total": len(replacements)
        },
        "replacements": replacements
    }

    # Salva os metadados em JSON
    bucket.blob(meta_blob_name).upload_from_string(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        content_type="application/json; charset=utf-8"
    )

    return metadata

# =========================
# FUNÇÃO PRINCIPAL
# =========================

# Função principal acionada pela requisição
def hello_auditlog(request):
    try:
        # Requisição GET usada para teste rápido de funcionamento
        if request.method == "GET":
            return (
                json.dumps({
                    "service": APP_NAME,
                    "status": "ok",
                    "spacy_enabled": SPACY_AVAILABLE,
                    "spacy_model": SPACY_MODEL_LOADED,
                    "input_prefix": INPUT_PREFIX,
                    "output_prefix": OUTPUT_PREFIX,
                    "meta_prefix": META_PREFIX
                }, ensure_ascii=False),
                200,
                {"Content-Type": "application/json; charset=utf-8"}
            )

        # Lê o corpo da requisição e tenta identificar o evento recebido
        payload = request.get_json(silent=True) or {}
        bucket_name, blob_name, event_data = extract_storage_event(payload)

        # Caso o evento não seja reconhecido, encerra sem erro
        if not bucket_name or not blob_name:
            logger.warning("Evento não reconhecido.")
            return (
                json.dumps({
                    "status": "ignored",
                    "reason": "evento_nao_reconhecido",
                    "payload_keys": list(payload.keys())
                }, ensure_ascii=False),
                200,
                {"Content-Type": "application/json; charset=utf-8"}
            )

        logger.info(f"Evento reconhecido | bucket={bucket_name} | arquivo={blob_name}")

        # Processa o arquivo encontrado no evento
        resultado = processar_arquivo(bucket_name, blob_name, event_data)

        logger.info(f"Resultado: {json.dumps(resultado, ensure_ascii=False)[:3000]}")

        # Retorna o resultado final do processamento
        return (
            json.dumps(resultado, ensure_ascii=False),
            200,
            {"Content-Type": "application/json; charset=utf-8"}
        )

    except Exception as e:
        # Tratamento de erro interno inesperado
        logger.exception("Erro interno no processamento")
        return (
            json.dumps({
                "status": "error",
                "message": str(e)
            }, ensure_ascii=False),
            500,
            {"Content-Type": "application/json; charset=utf-8"}
        )
