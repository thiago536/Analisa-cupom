<h1 align="center">
  <img src="Icon/icon.ico" width="120px" alt="Logo" />
  <br>
  Analisa Cupom
</h1>

<p align="center">
  <img src="https://img.shields.io/badge/vers%C3%A3o-1.0.0-blue" alt="Versão">
  <img src="https://img.shields.io/badge/python-3.x-blue" alt="Python">
  <img src="https://img.shields.io/badge/licen%C3%A7a-MIT-green" alt="Licença">
</p>

<p align="center">
  <strong>Sistema desktop para reconciliação inteligente de cupons fiscais (SEFAZ vs Sistema Interno), focado na identificação automática de falhas críticas.</strong>
</p>

---

## ✨ Features
- **Comparação de Cupons** — Identifica e classifica discrepâncias entre SEFAZ e os relatórios internos.
- **Suporte Multibanco** — Testa comunicação e cruza status consultando nativamente bancos locais Firebird ou instâncias remotas PostgreSQL.
- **Suporte a Extração Automática** — Analisa documentações da Secretaria da Fazenda recebidos em múltiplos formatos (`.pdf`, `.csv` e `.xls`).
- **Detecção Avançada** — Destaca automaticamente os alertas de quebra de sequência mais críticos: *Notas Inutilizadas na Sefaz, mas autorizadas isoladamente pelo ERP da empresa*.

## 🚀 Demo
### 🖥️ Demonstração

| Antes | Depois |
|-------|--------|
| [📸 Insira print da reconciliação manual ou planilha perdida] | [📸 Insira print do Analisa Cupom identificando falhas de Firebird e PostgreSQL] |

### ▶️ Fluxo completo
*[🎥 Insira um GIF ou vídeo curto demonstrando a importação do PDF local e validação com o banco de dados]*

## 🛠️ Stack
| Camada | Tecnologia |
|--------|------------|
| **Frontend UI** | Python (CustomTkinter) |
| **Motor de Tratamento** | Pandas, Regex |
| **Integração de Documentos**| PDFPlumber |
| **Banco de Dados** | fdb (Firebird ISQL), Psycopg2 (PostgreSQL) |

## ⚡ Instalação rápida

```bash
# Baixe o repositório
git clone https://github.com/thiago536/Analisa-cupom.git
cd Analisa-cupom

# Variáveis (Crie o arquivo .env)
DB_HOST=localhost
DB_PORT=5432
DB_USER=seu_usuario
DB_PASS=sua_senha

# Instale os requerimentos do sistema
pip install -r requirements.txt

# Inicie a interface Client
python app.py
```

## 📁 Estrutura do projeto
```
Analisa-cupom/
├── app.py               # Interface Gráfica, entrypoint do projeto e grids visuais
├── logic.py             # Lógica central: DataFrames, Parsing flexível e extração
├── firebird_isql.py     # Utilitário de resiliência e adaptação p/ drives Firebird 32x/64x
├── logging_utils.py     # Monitoramento e output de logs locais
├── requirements.txt     # Dependências restritas em produção
└── Icon/                # Assets gráficos e binários
```
