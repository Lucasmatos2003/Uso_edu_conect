from dotenv import load_dotenv
import os

# Carrega o .env do diretório raiz
load_dotenv()


# ========== MERAKI ==========
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise Exception("❌ API_KEY não encontrada no .env")


# ========== BANCO DE DADOS NEON ==========
PGHOST = os.getenv("PGHOST")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGPORT = os.getenv("PGPORT", "5432")
PGSSLMODE = os.getenv("PGSSLMODE", "require")

# Validação opcional (não quebra o código)
if not PGHOST:
    print("⚠️ Aviso: PGHOST não encontrado no .env")
