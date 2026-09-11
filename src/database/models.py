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

    codigo = Column(String(20), primary_key=True)
    nome = Column(String(60), nullable=False)
    ativa = Column(Boolean, nullable=False, default=True)


class Entidade(Base):
    __tablename__ = 'entidade'

    codigo = Column(String(40), primary_key=True)
    nome_exibicao = Column(String(80), nullable=False)
    partido = Column(String(40))
    foto = Column(String(255))
    ativa = Column(Boolean, nullable=False, default=True)
    criado_em = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class AlvoColeta(Base):
    """
    Define os alvos de coleta de cada entidade em cada fonte.
    Um alvo é composto por: canal (onde buscar) e termo_busca (o que buscar).
    Agora também armazena o estado (checkpoint) da última extração.
    """
    __tablename__ = 'alvo_coleta'

    id = Column(Integer, primary_key=True, autoincrement=True)
    entidade_codigo = Column(String(40), ForeignKey('entidade.codigo', ondelete='CASCADE'), nullable=False)
    fonte_codigo = Column(String(20), ForeignKey('fonte.codigo'), nullable=False)
    canal = Column(String(160), nullable=False)
    # Nome legível do canal quando `canal` em si não é (ex: channel_id do YouTube,
    # page_id do Meta) - só o YouTube popula isso hoje (seed_alvo_coleta, a partir de
    # entities.yaml/youtube.canais_noticia); NULL para os outros (o rótulo deles já sai
    # legível de outro jeito - ver canal_label() no webapp).
    rotulo = Column(String(160), nullable=True)
    termo_busca = Column(String(160), nullable=False, default="")

    tipo = Column(SQLEnum(TipoTermoEnum, name='tipo_termo'), nullable=False)
    usar_na_busca = Column(Boolean, nullable=False, default=True)
    ativo = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        Index('idx_alvo_coleta_unico', entidade_codigo, fonte_codigo, canal, termo_busca, unique=True),
    )

class AlvoColetaEstado(Base):
    __tablename__ = 'alvo_coleta_estado'

    alvo_coleta_id = Column(Integer, ForeignKey('alvo_coleta.id', ondelete='CASCADE'), primary_key=True)
    tipo_documento = Column(SQLEnum(TipoDocumentoEnum, name='tipo_documento'), primary_key=True)
    
    total_documentos = Column(Integer, nullable=False, default=0)
    
    primeiro_publicado_em = Column(DateTime(timezone=True))
    ultimo_publicado_em = Column(DateTime(timezone=True))
    atualizado_em = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


# ---------------------------------------------------------------------------
# Fase 1 — Coleta
# ---------------------------------------------------------------------------
# A tabela ColetaCheckpoint foi removida. O estado agora vive em AlvoColeta.

class Documento(Base):
    __tablename__ = 'documento'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    alvo_coleta_id = Column(Integer, ForeignKey('alvo_coleta.id'), nullable=False)
    id_nativo = Column(String(200), nullable=False)
    id_mongo = Column(String(48), nullable=False)
    tipo = Column(SQLEnum(TipoDocumentoEnum, name='tipo_documento'), nullable=False)
    url = Column(Text)
    texto = Column(Text)
    publicado_em = Column(DateTime(timezone=True), nullable=False)
    coletado_em = Column(DateTime(timezone=True), nullable=False)
    metadados = Column(JSONB)

    __table_args__ = (
        Index('idx_documento_alvo_nativo', alvo_coleta_id, id_nativo, unique=True),
        Index('idx_documento_alvo_publicado', alvo_coleta_id, publicado_em),
        Index('idx_documento_publicado', publicado_em),
    )


# ---------------------------------------------------------------------------
# Fases 2 e 4 — Modelos, Tópicos e Análise
# ---------------------------------------------------------------------------

class Modelo(Base):
    __tablename__ = 'modelo'

    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    tipo = Column(SQLEnum(TipoModeloEnum, name='tipo_modelo'), nullable=False)
    fonte_codigo = Column(String(20), ForeignKey('fonte.codigo'))
    nome = Column(String(80), nullable=False)
    versao = Column(String(20), nullable=False)
    janela_inicio = Column(Date)
    janela_fim = Column(Date)
    treinado_em = Column(DateTime(timezone=True))
    parametros = Column(JSONB)
    metricas = Column(JSONB)
    status = Column(SQLEnum(StatusModeloEnum, name='status_modelo'), nullable=False, default=StatusModeloEnum.treinando)

    __table_args__ = (
        Index('idx_modelo_tipo_nome_versao', tipo, nome, versao, unique=True),
    )


class Topico(Base):
    __tablename__ = 'topico'

    id = Column(Integer, primary_key=True, autoincrement=True)
    modelo_id = Column(SmallInteger, ForeignKey('modelo.id', ondelete='CASCADE'), nullable=False)
    numero = Column(Integer, nullable=False)
    rotulo = Column(String(140))
    revisado = Column(Boolean, nullable=False, default=False)
    palavras_chave = Column(ARRAY(Text))
    tamanho = Column(Integer)

    __table_args__ = (
        Index('idx_topico_modelo_numero', modelo_id, numero, unique=True),
    )


class Sentimento(Base):
    __tablename__ = 'sentimento'

    documento_id = Column(BigInteger, ForeignKey('documento.id', ondelete='CASCADE'), primary_key=True, nullable=False)
    modelo_id = Column(SmallInteger, ForeignKey('modelo.id', ondelete='CASCADE'), primary_key=True, nullable=False)
    polaridade = Column(SQLEnum(PolaridadeEnum, name='polaridade_enum'), nullable=False)


class DocumentoTopico(Base):
    __tablename__ = 'documento_topico'

    documento_id = Column(BigInteger, ForeignKey('documento.id', ondelete='CASCADE'), primary_key=True, nullable=False)
    topico_id = Column(Integer, ForeignKey('topico.id', ondelete='CASCADE'), primary_key=True, nullable=False)

    __table_args__ = (
        Index('idx_doc_topico_topico', topico_id),
    )