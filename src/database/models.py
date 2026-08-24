from sqlalchemy import (
    Column,
    Integer,
    SmallInteger,
    BigInteger,
    String,
    Boolean,
    Date,
    DateTime,
    Text,
    Numeric,
    ForeignKey,
    Enum as SQLEnum,
    Index,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import enum

Base = declarative_base()

# ---------------------------------------------------------------------------
# Enums em Python (mapeando os Enums do DBML/Postgres)
# ---------------------------------------------------------------------------

class TipoDocumentoEnum(str, enum.Enum):
    video = "video"
    post = "post"
    comentario = "comentario"
    resposta = "resposta"
    anuncio = "anuncio"

class TipoTermoEnum(str, enum.Enum):
    alias = "alias"
    handle = "handle"
    hashtag = "hashtag"
    consulta = "consulta"
    canal = "canal"

class TipoModeloEnum(str, enum.Enum):
    topico = "topico"
    sentimento = "sentimento"

class StatusModeloEnum(str, enum.Enum):
    treinando = "treinando"
    vigente = "vigente"
    arquivado = "arquivado"

class StatusExecucaoEnum(str, enum.Enum):
    em_execucao = "em_execucao"
    sucesso = "sucesso"
    parcial = "parcial"
    falha = "falha"

class PolaridadeEnum(str, enum.Enum):
    negativo = "negativo"
    neutro = "neutro"
    positivo = "positivo"


# ---------------------------------------------------------------------------
# Fase 0 — Configuração
# ---------------------------------------------------------------------------

class Fonte(Base):
    __tablename__ = 'fonte'

    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    codigo = Column(String(20), unique=True, nullable=False)
    nome = Column(String(60), nullable=False)
    ativa = Column(Boolean, nullable=False, default=True)


class Entidade(Base):
    __tablename__ = 'entidade'

    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    codigo = Column(String(40), unique=True, nullable=False)
    nome_exibicao = Column(String(80), nullable=False)
    partido = Column(String(40))
    ativa = Column(Boolean, nullable=False, default=True)
    criado_em = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class EntidadeTermo(Base):
    __tablename__ = 'entidade_termo'

    id = Column(Integer, primary_key=True, autoincrement=True)
    entidade_id = Column(SmallInteger, ForeignKey('entidade.id', ondelete='CASCADE'), nullable=False)
    fonte_id = Column(SmallInteger, ForeignKey('fonte.id'))
    termo = Column(String(160), nullable=False)
    tipo = Column(SQLEnum(TipoTermoEnum, name='tipo_termo'), nullable=False)
    usar_na_busca = Column(Boolean, nullable=False, default=True)
    ativo = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        Index('idx_entidade_termo_unico', entidade_id, fonte_id, termo, unique=True),
        Index('idx_entidade_termo_termo', termo),
    )


# ---------------------------------------------------------------------------
# Fase 1 — Coleta
# ---------------------------------------------------------------------------

class ColetaExecucao(Base):
    __tablename__ = 'coleta_execucao'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fonte_id = Column(SmallInteger, ForeignKey('fonte.id'), nullable=False)
    data_referencia = Column(Date, nullable=False)
    dag_run_id = Column(String(120), nullable=False)
    iniciado_em = Column(DateTime(timezone=True), nullable=False)
    finalizado_em = Column(DateTime(timezone=True))
    status = Column(SQLEnum(StatusExecucaoEnum, name='status_execucao'), nullable=False, default=StatusExecucaoEnum.em_execucao)
    docs_novos = Column(Integer, default=0)
    docs_duplicados = Column(Integer, default=0)
    quota_consumida = Column(Integer, default=0)
    erro = Column(Text)

    __table_args__ = (
        Index('idx_coleta_execucao_fonte_data', fonte_id, data_referencia),
    )


class ColetaCheckpoint(Base):
    __tablename__ = 'coleta_checkpoint'

    fonte_id = Column(SmallInteger, ForeignKey('fonte.id'), primary_key=True, nullable=False)
    chave = Column(String(160), primary_key=True, nullable=False)
    ultimo_id_nativo = Column(String(160))
    ultimo_publicado_em = Column(DateTime(timezone=True))
    atualizado_em = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Documento(Base):
    __tablename__ = 'documento'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fonte_id = Column(SmallInteger, ForeignKey('fonte.id'), nullable=False)
    id_nativo = Column(String(200), nullable=False)
    id_mongo = Column(String(48), nullable=False)
    tipo = Column(SQLEnum(TipoDocumentoEnum, name='tipo_documento'), nullable=False)
    autor_hash = Column(String(64))  # Char(64) em postgres pode ser mapeado como String(64)
    url = Column(Text)
    texto = Column(Text)
    publicado_em = Column(DateTime(timezone=True), nullable=False)
    coletado_em = Column(DateTime(timezone=True), nullable=False)
    engajamento = Column(Integer, default=0)
    suspeito = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index('idx_documento_fonte_nativo', fonte_id, id_nativo, unique=True),
        Index('idx_documento_fonte_publicado', fonte_id, publicado_em),
        Index('idx_documento_publicado', publicado_em),
    )


class DocumentoEntidade(Base):
    __tablename__ = 'documento_entidade'

    documento_id = Column(BigInteger, ForeignKey('documento.id', ondelete='CASCADE'), primary_key=True, nullable=False)
    entidade_id = Column(SmallInteger, ForeignKey('entidade.id'), primary_key=True, nullable=False)
    termo_id = Column(Integer, ForeignKey('entidade_termo.id'))

    __table_args__ = (
        Index('idx_doc_entidade_entidade', entidade_id),
    )


# ---------------------------------------------------------------------------
# Fases 2 e 4 — Modelos, Tópicos e Análise
# ---------------------------------------------------------------------------

class Modelo(Base):
    __tablename__ = 'modelo'

    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    tipo = Column(SQLEnum(TipoModeloEnum, name='tipo_modelo'), nullable=False)
    fonte_id = Column(SmallInteger, ForeignKey('fonte.id'))
    nome = Column(String(80), nullable=False)
    versao = Column(String(20), nullable=False)
    janela_inicio = Column(Date)
    janela_fim = Column(Date)
    treinado_em = Column(DateTime(timezone=True))
    parametros = Column(JSONB)
    metricas = Column(JSONB)
    artefato_uri = Column(Text)
    status = Column(SQLEnum(StatusModeloEnum, name='status_modelo'), nullable=False, default=StatusModeloEnum.treinando)

    __table_args__ = (
        Index('idx_modelo_tipo_nome_versao', tipo, nome, versao, unique=True),
    )


class Tema(Base):
    __tablename__ = 'tema'

    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    codigo = Column(String(60), unique=True, nullable=False)
    nome = Column(String(120), nullable=False)
    ativo = Column(Boolean, nullable=False, default=True)


class Topico(Base):
    __tablename__ = 'topico'

    id = Column(Integer, primary_key=True, autoincrement=True)
    modelo_id = Column(SmallInteger, ForeignKey('modelo.id', ondelete='CASCADE'), nullable=False)
    numero = Column(Integer, nullable=False)
    tema_id = Column(SmallInteger, ForeignKey('tema.id'))
    rotulo = Column(String(140))
    revisado = Column(Boolean, nullable=False, default=False)
    palavras_chave = Column(ARRAY(Text))
    tamanho = Column(Integer)

    __table_args__ = (
        Index('idx_topico_modelo_numero', modelo_id, numero, unique=True),
        Index('idx_topico_tema', tema_id),
    )


class DocumentoAnalise(Base):
    __tablename__ = 'documento_analise'

    documento_id = Column(BigInteger, ForeignKey('documento.id', ondelete='CASCADE'), primary_key=True, nullable=False)
    topico_id = Column(Integer, ForeignKey('topico.id'))
    probabilidade = Column(Numeric(4, 3))
    polaridade = Column(SQLEnum(PolaridadeEnum, name='polaridade'))
    confianca = Column(Numeric(4, 3))
    modelo_sentimento_id = Column(SmallInteger, ForeignKey('modelo.id'))
    atualizado_em = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        Index('idx_doc_analise_topico', topico_id),
        Index('idx_doc_analise_polaridade', polaridade),
    )


# ---------------------------------------------------------------------------
# Fase 3 — Agregação (Dashboard)
# ---------------------------------------------------------------------------

class AggDiario(Base):
    __tablename__ = 'agg_diario'

    dia = Column(Date, primary_key=True, nullable=False)
    entidade_id = Column(SmallInteger, ForeignKey('entidade.id'), primary_key=True, nullable=False)
    fonte_id = Column(SmallInteger, ForeignKey('fonte.id'), primary_key=True, nullable=False)
    tema_id = Column(SmallInteger, ForeignKey('tema.id'), primary_key=True, nullable=False)
    documentos = Column(Integer, nullable=False, default=0)
    engajamento = Column(BigInteger, default=0)
    doc_negativos = Column(Integer, default=0)
    doc_neutros = Column(Integer, default=0)
    doc_positivos = Column(Integer, default=0)

    __table_args__ = (
        Index('idx_agg_diario_tema_dia', tema_id, dia),
    )