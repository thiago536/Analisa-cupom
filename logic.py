"""
Lógica de negócios para reconciliação de cupons fiscais SEFAZ vs Sistema.
Refatorado para usar Pandas e suportar Excel/CSV com detecção automática de cabeçalhos.
"""

import pandas as pd
import os
import configparser
import psycopg2
import re

# Configurar caminho da DLL do Firebird antes de importar fdb
import sys
if sys.platform == 'win32':
    # Tentar encontrar fbclient.dll em locais comuns
    possible_paths = [
        r'C:\Program Files\Firebird\Firebird_3_0\bin',
        r'C:\Program Files\Firebird\Firebird_4_0\bin',
        r'C:\Program Files (x86)\Firebird\Firebird_3_0\bin',
        r'C:\Program Files (x86)\Firebird\Firebird_4_0\bin',
        r'C:\Program Files (x86)\Firebird\bin',  # Caminho genérico
        r'C:\Program Files\Firebird\bin',
        r'C:\Firebird\bin',
    ]
    
    for path in possible_paths:
        dll_path = os.path.join(path, 'fbclient.dll')
        if os.path.exists(dll_path):
            os.environ['FIREBIRD_HOME'] = os.path.dirname(path)  # Diretório pai
            if path not in os.environ.get('PATH', ''):
                os.environ['PATH'] = path + os.pathsep + os.environ.get('PATH', '')
            break

# Patch para Python 3.13 - resetlocale foi removido
import locale
if not hasattr(locale, 'resetlocale'):
    def resetlocale(category=locale.LC_ALL):
        """Dummy resetlocale for Python 3.13+ compatibility"""
        pass
    locale.resetlocale = resetlocale

import fdb

import logging_utils

# Inicializar logger
logger = logging_utils.get_logger()

# Importar pdfplumber hardcoded (obrigatório agora)
try:
    import pdfplumber
except ImportError:
    pdfplumber = None
    logger.error("Biblioteca pdfplumber não encontrada!")


def _carregar_dados_brutos(filepath):
    """
    Carrega CSV ou Excel como DataFrame sem cabeçalho definido.
    Tenta primeiro ler como Excel (pela extensão), depois como CSV se falhar.
    Isso resolve o problema de arquivos CSV com extensão .xls/.xlsx incorreta.
    
    Args:
        filepath (str): Caminho para o arquivo
        
    Returns:
        pd.DataFrame: DataFrame bruto sem cabeçalho processado
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")
    
    ext = os.path.splitext(filepath)[1].lower()
    df = None
    erros = []
    
    # Estratégia 1: Tentar ler como Excel se a extensão indicar isso
    if ext in ['.xlsx', '.xls']:
        try:
            df = pd.read_excel(filepath, header=None)
            return df
        except Exception as e:
            erros.append(f"Excel: {str(e)}")
            # Se falhar, continua para tentar como CSV (extensão pode estar errada)
    
    # Estratégia 2: Tentar ler como CSV com diferentes encodings e separadores
    # Tentativa 1: UTF-8 com BOM (padrão Windows)
    try:
        df = pd.read_csv(filepath, header=None, encoding='utf-8-sig')
        return df
    except Exception as e:
        erros.append(f"CSV utf-8-sig: {str(e)}")
    
    # Tentativa 2: Latin1 (ISO-8859-1) - comum em sistemas antigos
    try:
        df = pd.read_csv(filepath, header=None, encoding='latin1')
        return df
    except Exception as e:
        erros.append(f"CSV latin1: {str(e)}")
    
    # Tentativa 3: Latin1 com separador ponto-e-vírgula
    try:
        df = pd.read_csv(filepath, header=None, encoding='latin1', sep=';')
        return df
    except Exception as e:
        erros.append(f"CSV latin1 sep=';': {str(e)}")
    
    # Tentativa 4: UTF-8 sem BOM
    try:
        df = pd.read_csv(filepath, header=None, encoding='utf-8')
        return df
    except Exception as e:
        erros.append(f"CSV utf-8: {str(e)}")
    
    # Tentativa 5: Windows-1252 (CP1252)
    try:
        df = pd.read_csv(filepath, header=None, encoding='cp1252')
        return df
    except Exception as e:
        erros.append(f"CSV cp1252: {str(e)}")
    
    # Se todas as tentativas falharam, lançar erro detalhado
    raise Exception(
        f"Não foi possível carregar o arquivo {filepath}.\n"
        f"Extensão: {ext}\n"
        f"Tentativas realizadas:\n" + "\n".join(f"  - {erro}" for erro in erros)
    )


def _encontrar_cabecalho(df, colunas_obrigatorias):
    """
    Procura em qual linha do DataFrame estão as colunas obrigatórias.
    Busca dinâmica nas primeiras 20 linhas com MATCHING FLEXÍVEL.
    
    Args:
        df (pd.DataFrame): DataFrame bruto
        colunas_obrigatorias (list): Lista de strings que devem estar no cabeçalho (aceita parcial)
        
    Returns:
        pd.DataFrame: DataFrame com cabeçalho definido e dados abaixo dele
    """
    max_linhas_busca = min(20, len(df))
    
    for idx in range(max_linhas_busca):
        # Pegar a linha como potencial cabeçalho
        linha_teste = df.iloc[idx].astype(str).str.strip().str.lower()
        
        # Verificar se TODAS as colunas obrigatórias aparecem (match parcial)
        colunas_encontradas = 0
        for col_obrig in colunas_obrigatorias:
            col_obrig_lower = col_obrig.lower()
            # Procurar se ALGUMA célula da linha contém a palavra-chave
            for celula in linha_teste:
                if col_obrig_lower in celula:
                    colunas_encontradas += 1
                    break  # Encontrou esta coluna, passe para a próxima
        
        # Se encontrou todas as colunas obrigatórias
        if colunas_encontradas >= len(colunas_obrigatorias):
            # Esta é a linha do cabeçalho
            novo_header = df.iloc[idx].astype(str).str.strip()
            novo_df = df.iloc[idx+1:].copy()  # Dados abaixo do cabeçalho
            novo_df.columns = novo_header
            novo_df.reset_index(drop=True, inplace=True)
            return novo_df
    
    # Se chegou aqui, não encontrou
    raise ValueError(
        f"Não foi possível encontrar as colunas obrigatórias no arquivo SEFAZ.\n"
        f"Procurando por: {colunas_obrigatorias}\n"
        f"Primeiras 5 linhas do arquivo:\n{df.head()}"
    )


def _ler_sefaz(filepath, serie_alvo):
    """
    Lê o arquivo SEFAZ Inutilizados e retorna os números de documentos da série especificada.
    Suporta dois formatos:
    1. Arquivo com apenas coluna "Inicial a" (cupons individuais)
    2. Arquivo com colunas "Inicial a" e "Final" (intervalos de cupons)
    
    Args:
        filepath (str): Caminho para o arquivo SEFAZ
        serie_alvo (str): Série a ser filtrada
        
    Returns:
        set: Conjunto com os números de documentos fiscais (intervalos expandidos)
    """
    try:
        # Carregar dados brutos
        logger.info(f"_ler_sefaz: Carregando arquivo {os.path.basename(filepath)}")
        df = _carregar_dados_brutos(filepath)
        logger.debug(f"_ler_sefaz: Arquivo carregado. Shape: {df.shape}")
        
        # Tentar encontrar cabeçalho com intervalos primeiro (Inicial a + Final)
        tem_intervalos = False
        try:
            logger.debug(f"_ler_sefaz: Tentando encontrar colunas com intervalos...")
            df = _encontrar_cabecalho(df, ['Inicial', 'Final', 'Série'])
            tem_intervalos = True
            logger.info(f"_ler_sefaz: ✅ Cabeçalho encontrado COM intervalos")
        except ValueError as e:
            # Se não encontrar "Final", tentar apenas com "Inicial"
            logger.debug(f"_ler_sefaz: Intervalos não encontrados, tentando sem 'Final'...")
            try:
                df = _encontrar_cabecalho(df, ['Inicial', 'Série'])
                tem_intervalos = False
                logger.info(f"_ler_sefaz: ✅ Cabeçalho encontrado SEM intervalos")
            except ValueError as e2:
                logger.error(f"_ler_sefaz: ❌ ERRO - Não encontrou nem com intervalos nem sem")
                logger.error(f"Erro original: {str(e2)}")
                raise
        
        # Localizar colunas (busca flexível)
        col_inicial = None
        col_final = None
        col_serie = None
        
        
        logger.debug(f"_ler_sefaz: Colunas disponíveis após encontrar cabeçalho: {list(df.columns)}")
        
        for col in df.columns:
            col_lower = str(col).lower()
            if 'inicial' in col_lower and not col_inicial:
                col_inicial = col
                logger.debug(f"_ler_sefaz: Coluna 'Inicial' = '{col}'")
            if 'final' in col_lower and not col_final:
                col_final = col
                logger.debug(f"_ler_sefaz: Coluna 'Final' = '{col}'")
            if 'série' in col_lower or 'serie' in col_lower:
                col_serie = col
                logger.debug(f"_ler_sefaz: Coluna 'Série' = '{col}'")
        
        if not col_inicial or not col_serie:
            raise ValueError(f"Colunas obrigatórias não encontradas. Disponíveis: {list(df.columns)}")
        
        # Filtrar e coletar documentos
        documentos = set()
        for _, row in df.iterrows():
            try:
                serie_val = str(row[col_serie]).strip()
                
                # Comparar série (normalizar para string)
                if serie_val != str(serie_alvo):
                    continue
                
                inicial_val = str(row[col_inicial]).strip()
                if not inicial_val or inicial_val.lower() == 'nan':
                    continue
                
                # Converter inicial para inteiro
                inicial_num = int(float(inicial_val))
                
                # Se tem coluna "Final", processar como intervalo
                if tem_intervalos and col_final:
                    final_val = str(row[col_final]).strip()
                    if final_val and final_val.lower() != 'nan':
                        try:
                            final_num = int(float(final_val))
                            # Expandir intervalo: de inicial até final (inclusive)
                            for num in range(inicial_num, final_num + 1):
                                documentos.add(str(num))
                        except (ValueError, TypeError):
                            # Se falhar ao ler "Final", adicionar apenas o inicial
                            documentos.add(str(inicial_num))
                    else:
                        # Se "Final" está vazio, adicionar apenas o inicial
                        documentos.add(str(inicial_num))
                else:
                    # Sem coluna "Final", adicionar apenas o inicial
                    documentos.add(str(inicial_num))
                    
            except (ValueError, TypeError):
                continue
        
        return documentos
        
    except Exception as e:
        raise Exception(f"Erro ao ler arquivo SEFAZ: {str(e)}")


def _ler_relatorio(filepath, serie_alvo):
    """
    Lê o arquivo Relatório do Sistema e retorna um dicionário de Doc. Fiscal -> Status.
    Usa Pandas para suportar Excel e CSV com detecção automática de cabeçalho.
    
    Args:
        filepath (str): Caminho para o arquivo Relatório
        serie_alvo (str): Série a ser filtrada
        
    Returns:
        dict: Dicionário no formato {'Doc. Fiscal': 'Status'}
    """
    try:
        # Carregar dados brutos
        df = _carregar_dados_brutos(filepath)
        
        # Encontrar cabeçalho dinamicamente
        df = _encontrar_cabecalho(df, ['Doc', 'Série', 'Status'])
        
        # Localizar colunas (busca flexível)
        col_doc = None
        col_serie = None
        col_status = None
        
        for col in df.columns:
            col_lower = str(col).lower()
            if 'doc' in col_lower and 'fiscal' in col_lower:
                col_doc = col
            elif 'doc' in col_lower and not col_doc:
                col_doc = col
            if 'série' in col_lower or 'serie' in col_lower:
                col_serie = col
            if 'status' in col_lower:
                col_status = col
        
        if not col_doc or not col_serie or not col_status:
            raise ValueError(f"Colunas não encontradas. Disponíveis: {list(df.columns)}")
        
        # Filtrar e coletar documentos
        relatorio = {}
        for _, row in df.iterrows():
            try:
                serie_val = str(row[col_serie]).strip()
                doc_val = str(row[col_doc]).strip()
                status_val = str(row[col_status]).strip()
                
                # Comparar série (normalizar para string)
                if serie_val == str(serie_alvo) and doc_val and doc_val.lower() != 'nan':
                    # Converter para inteiro para remover zeros à esquerda e .0
                    doc_limpo = str(int(float(doc_val)))
                    relatorio[doc_limpo] = status_val
            except (ValueError, TypeError):
                continue
        
        return relatorio
        
    except Exception as e:
        raise Exception(f"Erro ao ler arquivo Relatório: {str(e)}")


def executar_analise_discrepancia(path_sefaz, path_relatorio, serie_alvo):
    """
    Reconcilia cupons inutilizados no SEFAZ com o Relatório do Sistema.
    Identifica discrepâncias de status com base na série especificada.
    
    Args:
        path_sefaz (str): Caminho para o arquivo SEFAZ Inutilizados
        path_relatorio (str): Caminho para o arquivo Relatório do Sistema
        serie_alvo (str): Série a ser analisada
        
    Returns:
        dict: Dicionário com as chaves:
            - 'tipo': 'discrepancia'
            - 'discrepancia_grave': lista de docs Inutilizados no SEFAZ mas Autorizados no Sistema
            - 'conciliado_ok': lista de docs Inutilizados no SEFAZ e Cancelados/Outro no Sistema
            - 'nao_encontrado_no_relatorio': lista de docs Inutilizados no SEFAZ mas não existem no Relatório
            - 'count_sefaz': total de documentos inutilizados lidos do SEFAZ para a série
            - 'count_relatorio': total de documentos lidos do Relatório para a série
            - 'erro': None se sucesso, string com erro se houver problema
    """
    try:
        # Ler dados de ambos os arquivos
        docs_sefaz = _ler_sefaz(path_sefaz, serie_alvo)
        relatorio_sistema = _ler_relatorio(path_relatorio, serie_alvo)
        
        # Inicializar listas de resultados
        discrepancia_grave = []
        conciliado_ok = []
        nao_encontrado_no_relatorio = []
        
        # Iterar sobre os documentos inutilizados no SEFAZ
        for doc in docs_sefaz:
            if doc in relatorio_sistema:
                status = relatorio_sistema[doc]
                
                # Verificar se está autorizado (discrepância grave)
                # Tratando variações: Autorizada, Autorizado, etc.
                if 'autoriza' in status.lower():
                    discrepancia_grave.append(doc)
                else:
                    # Cancelado ou outro status (conciliado OK)
                    conciliado_ok.append(doc)
            else:
                # Documento não encontrado no relatório
                nao_encontrado_no_relatorio.append(doc)
        
        # Retornar resultados ordenados com contagens
        resultado = {
            'tipo': 'discrepancia',
            'discrepancia_grave': sorted(discrepancia_grave, key=lambda x: int(x)),
            'conciliado_ok': sorted(conciliado_ok, key=lambda x: int(x)),
            'nao_encontrado_no_relatorio': sorted(nao_encontrado_no_relatorio, key=lambda x: int(x)),
            'count_sefaz': len(docs_sefaz),
            'count_relatorio': len(relatorio_sistema),
            'erro': None
        }
        
        return resultado
        
    except (FileNotFoundError, ValueError, Exception) as e:
        return {
            'tipo': 'discrepancia',
            'discrepancia_grave': [],
            'conciliado_ok': [],
            'nao_encontrado_no_relatorio': [],
            'count_sefaz': 0,
            'count_relatorio': 0,
            'erro': str(e)
        }


def executar_comparacao_simples(path_a, path_b, serie_alvo):
    """
    Compara dois arquivos de cupons (A vs B) e retorna a diferença entre eles.
    Usa matemática de conjuntos para identificar cupons em comum e exclusivos.
    
    Args:
        path_a (str): Caminho para o primeiro arquivo
        path_b (str): Caminho para o segundo arquivo
        serie_alvo (str): Série a ser analisada
        
    Returns:
        dict: Dicionário com as chaves:
            - 'tipo': 'comparacao'
            - 'em_ambos': lista de cupons que estão em ambos os arquivos
            - 'so_no_arquivo_a': lista de cupons que estão apenas no arquivo A
            - 'so_no_arquivo_b': lista de cupons que estão apenas no arquivo B
            - 'count_a': total de cupons lidos do arquivo A
            - 'count_b': total de cupons lidos do arquivo B
            - 'nome_a': nome do arquivo A
            - 'nome_b': nome do arquivo B
            - 'erro': None se sucesso, string com erro se houver problema
    """
    try:
        import os
        
        # Extrair nomes dos arquivos para exibição
        nome_a = os.path.basename(path_a)
        nome_b = os.path.basename(path_b)
        
        # Tentar ler os arquivos de forma inteligente
        set_a = None
        set_b = None
        
        # Estratégia 1: Tentar A como SEFAZ (simples) e B como Relatório (complexo)
        try:
            set_a = _ler_sefaz(path_a, serie_alvo)
            set_b_dict = _ler_relatorio(path_b, serie_alvo)
            set_b = set(set_b_dict.keys())
        except:
            # Estratégia 2: Tentar A como Relatório (complexo) e B como SEFAZ (simples)
            try:
                set_a_dict = _ler_relatorio(path_a, serie_alvo)
                set_a = set(set_a_dict.keys())
                set_b = _ler_sefaz(path_b, serie_alvo)
            except:
                # Estratégia 3: Tentar ambos como SEFAZ (simples)
                try:
                    set_a = _ler_sefaz(path_a, serie_alvo)
                    set_b = _ler_sefaz(path_b, serie_alvo)
                except:
                    # Estratégia 4: Tentar ambos como Relatório (complexo)
                    set_a_dict = _ler_relatorio(path_a, serie_alvo)
                    set_a = set(set_a_dict.keys())
                    set_b_dict = _ler_relatorio(path_b, serie_alvo)
                    set_b = set(set_b_dict.keys())
        
        # Realizar operações de conjunto
        em_ambos = set_a.intersection(set_b)
        so_no_a = set_a.difference(set_b)
        so_no_b = set_b.difference(set_a)
        
        # Retornar resultados ordenados
        resultado = {
            'tipo': 'comparacao',
            'em_ambos': sorted(list(em_ambos), key=lambda x: int(x)),
            'so_no_arquivo_a': sorted(list(so_no_a), key=lambda x: int(x)),
            'so_no_arquivo_b': sorted(list(so_no_b), key=lambda x: int(x)),
            'count_a': len(set_a),
            'count_b': len(set_b),
            'nome_a': nome_a,
            'nome_b': nome_b,
            'erro': None
        }
        
        return resultado
        
    except (FileNotFoundError, ValueError, Exception) as e:
        return {
            'tipo': 'comparacao',
            'em_ambos': [],
            'so_no_arquivo_a': [],
            'so_no_arquivo_b': [],
            'count_a': 0,
            'count_b': 0,
            'nome_a': os.path.basename(path_a) if path_a else 'Arquivo A',
            'nome_b': os.path.basename(path_b) if path_b else 'Arquivo B',
            'erro': str(e)
        }


# Função de compatibilidade (mantém nome antigo)
def analisar_cupons(path_sefaz, path_relatorio, serie_alvo):
    """
    Função de compatibilidade. Chama executar_analise_discrepancia.
    Mantida para não quebrar código existente.
    """
    return executar_analise_discrepancia(path_sefaz, path_relatorio, serie_alvo)


def testar_conexao_db_universal(config):
    """
    Testa conexão com banco de dados (PostgreSQL ou Firebird).
    
    Args:
        config (dict): Configuração com as chaves:
            - 'tipo': 'nuvem' (PostgreSQL) ou 'local' (Firebird)
            - Para 'nuvem': 'dbname' (nome do banco)
            - Para 'local': 'path' (caminho do arquivo .FDB)
            
    Returns:
        dict: Dicionário com as chaves:
            - 'sucesso': True se conexão bem-sucedida, False caso contrário
            - 'erro': String com mensagem de erro (apenas se sucesso=False)
            - 'mensagem': String com mensagem de sucesso (apenas se sucesso=True)
    """
    try:
        tipo = config.get('tipo')
        
        if tipo == 'nuvem':
            # PostgreSQL (Nuvem)
            dbname = config.get('dbname', '')
            
            if not dbname:
                return {
                    'sucesso': False,
                    'erro': 'Nome do banco não pode estar vazio.'
                }
            
            # Credenciais para PostgreSQL (configuráveis via variáveis de ambiente)
            conn_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 5432)),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASS', ''),
                'dbname': dbname
            }
            
            # Conectar ao PostgreSQL
            conn = psycopg2.connect(**conn_config)
            
            # Obter versão
            cursor = conn.cursor()
            cursor.execute('SELECT version();')
            versao = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            return {
                'sucesso': True,
                'mensagem': f'Conexão estabelecida com sucesso!\nTipo: PostgreSQL (Nuvem)\nBanco: {dbname}\nHost: localhost:5432\nVersão: {versao.split(",")[0]}'
            }
            
        elif tipo == 'local':
            # Firebird (Local)
            path = config.get('path', '')
            user = config.get('user', '')
            password = config.get('password', '')
            
            if not path:
                return {
                    'sucesso': False,
                    'erro': 'Caminho do arquivo .FDB não pode estar vazio.'
                }
            
            if not user:
                return {
                    'sucesso': False,
                    'erro': 'Usuário não pode estar vazio.'
                }
            
            if not password:
                return {
                    'sucesso': False,
                    'erro': 'Senha não pode estar vazia.'
                }
            
            if not os.path.exists(path):
                return {
                    'sucesso': False,
                    'erro': f'Arquivo não encontrado: {path}'
                }
            
            # SOLUÇÃO: Usar isql-fb (ferramenta nativa) para evitar problemas de DLL 32/64 bits
            try:
                from firebird_isql import test_firebird_connection_isql
                return test_firebird_connection_isql(path, user, password)
            except ImportError:
                pass
            
            # Fallback: Tentar fdb com localhost DSN
            dsn = f'localhost:{path}'
            
            try:
                conn = fdb.connect(
                    dsn=dsn,
                    user=user,
                    password=password,
                    charset='UTF8'
                )
            
                cursor = conn.cursor()
                cursor.execute('SELECT RDB$GET_CONTEXT(\'SYSTEM\', \'ENGINE_VERSION\') FROM RDB$DATABASE')
                versao = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                return {
                    'sucesso': True,
                    'mensagem': f'Conexão estabelecida com sucesso!\\nTipo: Firebird (Local Server)\\nServidor: localhost\\nArquivo: {os.path.basename(path)}\\nUsuário: {user}\\nVersão: {versao}'
                }
            except Exception as e:
                return {
                    'sucesso': False,
                    'erro': f'Erro Firebird:\\n\\n{str(e)}\\n\\nDica: Verifique se o Python e o Firebird são ambos 64-bit ou ambos 32-bit.'
                }

        else:
            return {
                'sucesso': False,
                'erro': f'Tipo de banco inválido: {tipo}'
            }
            
    except psycopg2.Error as e:
        return {
            'sucesso': False,
            'erro': f'Erro de conexão PostgreSQL:\n{str(e)}'
        }
    except fdb.fbcore.DatabaseError as e:
        return {
            'sucesso': False,
            'erro': f'Erro de conexão Firebird:\n{str(e)}'
        }
    except Exception as e:
        return {
            'sucesso': False,
            'erro': f'Erro inesperado:\n{str(e)}'
        }


def _extrair_cupons_do_texto(texto_bruto):
    """
    Extrai números de cupom de um texto colado pelo usuário.
    Suporta formatos como:
    - "24. 158026" (pega 158026)
    - "154031" (pega 154031)
    - "1. 123456" (pega 123456)
    
    Args:
        texto_bruto (str): Texto com lista de cupons
        
    Returns:
        set: Conjunto de strings com os números de cupom
    """
    cupons = set()
    
    # Processar linha por linha
    for linha in texto_bruto.split('\n'):
        linha = linha.strip()
        if not linha:
            continue
        
        # Procurar o último número na linha (ignora numeração de lista)
        # Regex: procura sequência de dígitos no final da linha
        match = re.search(r'(\d+)\s*$', linha)
        if match:
            cupom = match.group(1)
            cupons.add(cupom)
    
    return cupons


def _extrair_cupons_com_serie(texto_bruto):
    """
    Extrai números de cupom com informação de série de um texto.
    Suporta formatos como:
    - "158026|SERIE_1" (cupom 158026 da série 1)
    - "154031" (cupom 154031 sem série específica)
    
    Args:
        texto_bruto (str): Texto com lista de cupons
        
    Returns:
        dict: Dicionário {cupom: serie} ou {cupom: None}
    """
    cupons_com_serie = {}
    
    # Processar linha por linha
    for linha in texto_bruto.split('\n'):
        linha = linha.strip()
        if not linha:
            continue
        
        # Verificar se tem formato com série: cupom|SERIE_X
        if '|SERIE_' in linha:
            partes = linha.split('|SERIE_')
            if len(partes) == 2:
                cupom = partes[0].strip()
                serie = partes[1].strip()
                cupons_com_serie[cupom] = serie
                continue
        
        # Formato normal (sem série)
        match = re.search(r'(\d+)\s*$', linha)
        if match:
            cupom = match.group(1)
            cupons_com_serie[cupom] = None
    
    return cupons_com_serie


def _ler_config_db_do_ini(path_ini):
    """
    Lê o arquivo .ini e retorna a configuração do banco de dados.
    Função auxiliar para reutilizar a lógica de leitura do .ini.
    
    Args:
        path_ini (str): Caminho para o arquivo .ini
        
    Returns:
        dict: Configuração do banco ou None se houver erro
    """
    try:
        if not os.path.exists(path_ini):
            return None
        
        config = configparser.ConfigParser()
        config.read(path_ini, encoding='utf-8')
        
        if 'Banco de Dados' not in config:
            return None
        
        secao_bd = config['Banco de Dados']
        
        config_db = {
            'host': secao_bd.get('NomeServidor', 'localhost'),
            'port': secao_bd.get('Porta', '5432'),
            'dbname': secao_bd.get('Caminho', ''),
            'user': secao_bd.get('Usuario', ''),
            'password': secao_bd.get('Senha', '')
        }
        
        if not config_db['dbname'] or not config_db['user']:
            return None
        
        return config_db
        
    except Exception:
        return None


def executar_analise_db(config, texto_bruto, serie_alvo):
    """
    Executa análise de cupons via banco de dados (PostgreSQL ou Firebird).
    
    Fluxo:
    1. Extrai cupons do texto
    2. Conecta ao banco (PostgreSQL ou Firebird)
    3. Para cada cupom:
       - Aplica padding de 9 dígitos (ex: 158026 -> 000158026)
       - Executa query SQL
       - Classifica resultado
    4. Retorna listas classificadas
    
    Args:
        config (dict): Configuração do banco com 'tipo' e credenciais
        texto_bruto (str): Texto com lista de cupons
        serie_alvo (str): Série a ser consultada
        
    Returns:
        dict: Dicionário com as chaves:
            - 'tipo': 'analise_db'
            - 'prontos_para_inutilizar': lista de cupons com E0001
            - 'autorizados': lista de cupons autorizados
            - 'cancelados': lista de cupons cancelados
            - 'nao_encontrados': lista de cupons não encontrados no BD
            - 'outros_erros': lista de cupons com outros erros
            - 'total_processados': total de cupons processados
            - 'erro': None se sucesso, string com erro se houver problema
    """
    try:
        # 1. Extrair cupons do texto
        cupons = _extrair_cupons_do_texto(texto_bruto)
        
        if not cupons:
            return {
                'tipo': 'analise_db',
                'prontos_para_inutilizar': [],
                'autorizados': [],
                'cancelados': [],
                'nao_encontrados': [],
                'outros_erros': [],
                'total_processados': 0,
                'erro': 'Nenhum cupom encontrado no texto fornecido.'
            }
        
        # 2. Conectar ao banco de dados
        tipo = config.get('tipo')
        
        if tipo == 'nuvem':
            # PostgreSQL
            conn_config = {
                'host': 'localhost',
                'port': 5432,
                'user': 'postgres',
                'password': '123',
                'dbname': config.get('dbname')
            }
            conn = psycopg2.connect(**conn_config)
        elif tipo == 'local':
            # Firebird - usar localhost DSN (como IBOConsole)
            path = config.get('path')
            dsn = f'localhost:{path}'
            try:
                conn = fdb.connect(dsn=dsn, user=config.get('user'), password=config.get('password'), charset='UTF8')
            except:
                # Fallback para embedded
                conn = fdb.connect(database=path, user=config.get('user'), password=config.get('password'), charset='UTF8')
        else:
            return {
                'tipo': 'analise_db',
                'prontos_para_inutilizar': [],
                'autorizados': [],
                'cancelados': [],
                'nao_encontrados': [],
                'outros_erros': [],
                'total_processados': 0,
                'erro': f'Tipo de banco inválido: {tipo}'
            }
        
        cursor = conn.cursor()
        
        # 4. Preparar listas de resultados
        prontos_para_inutilizar = []
        autorizados = []
        cancelados = []
        nao_encontrados = []
        outros_erros = []
        
        # 5. Loop principal: processar cada cupom
        for cupom in cupons:
            try:
                # Aplicar padding de 9 dígitos
                num_formatado = cupom.zfill(9)
                
                # Query SQL
                query = """
                    SELECT nfe_cod_resp, nfe_status, cancelada 
                    FROM vendas 
                    WHERE numero_nf = %s AND serie_nf = %s
                """
                
                cursor.execute(query, (num_formatado, serie_alvo))
                resultado = cursor.fetchone()
                
                # Lógica de decisão
                if not resultado:
                    # Cupom não encontrado no banco
                    nao_encontrados.append(cupom)
                else:
                    nfe_cod_resp = resultado[0]
                    nfe_status = resultado[1]
                    cancelada = resultado[2]
                    
                    # Verificar status
                    if nfe_cod_resp == 'E0001':
                        # Pronto para inutilizar
                        prontos_para_inutilizar.append(cupom)
                    elif nfe_status and 'autoriza' in nfe_status.lower():
                        # Autorizado (discrepância grave)
                        autorizados.append(cupom)
                    elif cancelada and cancelada.upper() == 'S':
                        # Já cancelado
                        cancelados.append(cupom)
                    else:
                        # Outros casos
                        outros_erros.append(f"{cupom} (Status: {nfe_status or 'N/A'})")
                        
            except Exception as e:
                # Erro ao processar cupom específico
                outros_erros.append(f"{cupom} (Erro: {str(e)})")
        
        # 6. Fechar conexão
        cursor.close()
        conn.close()
        
        # 7. Ordenar resultados
        prontos_para_inutilizar.sort(key=lambda x: int(x))
        autorizados.sort(key=lambda x: int(x))
        cancelados.sort(key=lambda x: int(x))
        nao_encontrados.sort(key=lambda x: int(x))
        
        # 8. Retornar resultados
        return {
            'tipo': 'analise_db',
            'prontos_para_inutilizar': prontos_para_inutilizar,
            'autorizados': autorizados,
            'cancelados': cancelados,
            'nao_encontrados': nao_encontrados,
            'outros_erros': outros_erros,
            'total_processados': len(cupons),
            'erro': None
        }
        
    except psycopg2.Error as e:
        return {
            'tipo': 'analise_db',
            'prontos_para_inutilizar': [],
            'autorizados': [],
            'cancelados': [],
            'nao_encontrados': [],
            'outros_erros': [],
            'total_processados': 0,
            'erro': f'Erro de conexão PostgreSQL:\n{str(e)}'
        }
    except Exception as e:
        return {
            'tipo': 'analise_db',
            'prontos_para_inutilizar': [],
            'autorizados': [],
            'cancelados': [],
            'nao_encontrados': [],
            'outros_erros': [],
            'total_processados': 0,
            'erro': f'Erro inesperado:\n{str(e)}'
        }


def converter_pdf_para_excel(path_pdf, temp_excel_path):
    """
    Converte um PDF tabular para Excel usando pdfplumber (versão robusta).
    
    Extrai tabelas de PDFs e as converte para o formato XLSX com estrutura padronizada.
    Colunas esperadas: 'Inicial a', 'Final', 'Série', 'Espécie'
    
    Args:
        path_pdf (str): Caminho para o arquivo PDF
        temp_excel_path (str): Caminho onde salvar o Excel temporário
        
    Returns:
        dict: {'sucesso': True, 'path': temp_excel_path, 'linhas': int} 
              ou {'sucesso': False, 'erro': str}
    """
    try:
        # Usar pdfplumber (não precisa de Java!)
        if pdfplumber is None:
            return {
                'sucesso': False,
                'erro': 'Biblioteca pdfplumber não está instalada.\nInstale com: pip install pdfplumber'
            }
        
        if not os.path.exists(path_pdf):
            return {
                'sucesso': False,
                'erro': f'Arquivo PDF não encontrado: {path_pdf}'
            }
        
        # 1. Extrair tabelas do PDF usando pdfplumber
        # 1. Extrair tabelas do PDF usando pdfplumber
        logger.info(f"Iniciando extração de tabelas do PDF com pdfplumber...")
        logger.info(f"Arquivo: {os.path.basename(path_pdf)}")
        
        all_tables = []
        with pdfplumber.open(path_pdf) as pdf:
            logger.info(f"PDF tem {len(pdf.pages)} páginas")
            for page_num, page in enumerate(pdf.pages, 1):
                logger.debug(f"Processando página {page_num}/{len(pdf.pages)}...")
                tables = page.extract_tables()
                if tables:
                    all_tables.extend(tables)
                    logger.debug(f"{len(tables)} tabela(s) encontrada(s) na pág {page_num}")
                else:
                    logger.debug(f"Nenhuma tabela na pág {page_num}")
        
        logger.info(f"Total de tabelas extraídas: {len(all_tables)}")
        
        # Se não encontrou tabelas, tentar extrair texto bruto
        if not all_tables:
            logger.warning("Nenhuma tabela detectada. Tentando extração de texto bruto...")
            all_text_lines = []
            with pdfplumber.open(path_pdf) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    logger.debug(f"Extraindo texto da página {page_num}...")
                    text = page.extract_text()
                    if text:
                        lines = text.split('\n')
                        all_text_lines.extend(lines)
            
            if not all_text_lines:
                return {
                    'sucesso': False,
                    'erro': 'Nenhuma tabela ou texto encontrado no PDF.\n\nO PDF pode ser uma imagem escaneada.\nTente usar OCR ou converter para Excel manualmente.'
                }
            
            # Tentar processar texto como tabela CSV-like
            logger.info(f"{len(all_text_lines)} linhas de texto extraídas. Tentando interpretar como tabela...")
            # Procurar por linhas que parecem conter dados tabulares
            table_data = []
            for line in all_text_lines:
                # Dividir por múltiplos espaços ou tabulações
                parts = [p.strip() for p in line.split() if p.strip()]
                if len(parts) >= 3:  # Precisa de pelo menos 3 colunas
                    table_data.append(parts)
            
            if table_data:
                all_tables = [table_data]
                logger.info(f"Interpretado como 1 tabela com {len(table_data)} linhas")
            else:
                return {
                    'sucesso': False,
                    'erro': 'Não foi possível extrair dados tabulares do PDF.\n\nO PDF não tem estrutura de tabela reconhecível.'
                }
        
        # 2. Converter tabelas (listas) para DataFrames e processar
        all_data = []
        # Colunas esperadas: usar nomes que serão aceitos pela função _ler_sefaz
        expected_columns = ['Inicial a', 'Final', 'Série', 'Espécie']
        
        print(f"[DEBUG] Processando {len(all_tables)} tabelas...")
        for idx, table in enumerate(all_tables, 1):
            logger.debug(f"Processando tabela {idx}/{len(all_tables)}...")
            
            if not table or len(table) < 2:
                logger.debug("Ignorada (vazia ou sem dados)")
                continue
            
            # Converter lista de listas para DataFrame
            try:
                # Criar DataFrame sem definir colunas (pandas gera 0, 1, 2...)
                # Isso evita erro se a primeira linha tiver menos colunas que as linhas de dados
                df = pd.DataFrame(table)
            except Exception as e:
                logger.debug(f"Erro ao criar DataFrame: {e}")
                continue
            
            # Filtrar DataFrames com pelo menos 4 colunas
            if df.shape[1] >= 4:
                # Selecionar as 4 primeiras colunas
                df = df.iloc[:, :4].copy()
                
                # Procurar pela linha de cabeçalho
                header_index = -1
                for idx, row in df.iterrows():
                    # Converter linha para string única para busca fácil
                    row_str = " ".join([str(val) for val in row.values]).lower()
                    if 'inicial' in row_str and 'final' in row_str:
                        header_index = idx
                        break
                
                # Se achou cabeçalho, usar ele e cortar o que vem antes
                if header_index != -1:
                    df = df.iloc[header_index+1:].copy()
                    df.columns = expected_columns
                else:
                    # Se não achou cabeçalho explícito, tentar validar dados numéricos
                    # Verificar se tem dados válidos (números na primeira coluna)
                    has_valid_data = False
                    for _, row in df.iterrows():
                        first_val = str(row.iloc[0]).strip()
                        if first_val and first_val.replace('.', '').replace(',', '').isdigit():
                            has_valid_data = True
                            break
                    
                    if has_valid_data:
                        # Assumir que são as colunas esperadas
                        df.columns = expected_columns
                    else:
                        logger.debug("Ignorada (sem cabeçalho e sem dados numéricos)")
                        continue

                # Limpeza: remover linhas onde a primeira coluna está vazia
                df = df.dropna(subset=['Inicial a'])
                
                # Remover linhas que contêm cabeçalhos repetidos (redundância de segurança)
                df = df[~df['Inicial a'].astype(str).str.contains('Inicial', case=False, na=False)]
                
                if len(df) > 0:
                    all_data.append(df)
                    logger.debug("OK - Tabela válida")
                else:
                    logger.debug("Ignorada (sem dados válidos após limpeza)")
            else:
                logger.debug("Ignorada (menos de 4 colunas)")
        
        print(f"[DEBUG] {len(all_data)} tabelas válidas encontradas.")
        
        if not all_data:
            return {
                'sucesso': False,
                'erro': 'Nenhum dado válido encontrado após a limpeza.'
            }
        
        # Concatenar todos os DataFrames
        logger.debug(f"Concatenando DataFrames...")
        final_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total: {len(final_df)} linhas")
        
        # 3. Limpeza e conversão de tipos
        logger.debug(f"Limpando e convertendo dados...")
        # Remover espaços em branco e converter para numérico
        for col in ['Inicial a', 'Final', 'Série']:
            final_df[col] = final_df[col].astype(str).str.strip()
            # Converte para numérico, forçando erros para NaN
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
        
        # Remover linhas onde as colunas chave ficaram vazias após a conversão
        final_df.dropna(subset=['Inicial a', 'Série'], inplace=True)
        
        # Converter para inteiro (tipo 'Int64' suporta valores nulos)
        for col in ['Inicial a', 'Final', 'Série']:
            final_df[col] = final_df[col].astype('Int64')
        
        # Limpar coluna Espécie
        final_df['Espécie'] = final_df['Espécie'].astype(str).str.strip()
        
        # 4. Salvar no formato XLSX
        logger.info(f"Salvando Excel: {os.path.basename(temp_excel_path)}...")
        final_df.to_excel(temp_excel_path, index=False, sheet_name='Notas')
        logger.info(f"✅ Conversão concluída! {len(final_df)} linhas exportadas.")
        
        return {
            'sucesso': True,
            'path': temp_excel_path,
            'linhas': len(final_df)
        }
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return {
            'sucesso': False,
            'erro': f'Erro ao converter PDF:\n\n{str(e)}\n\nDetalhes técnicos:\n{error_details}'
        }




    except Exception as e:
        return {
            'sucesso': False,
            'erro': f'Erro ao obter empresas: {str(e)}'
        }


def obter_empresas_disponiveis(config):
    """
    Obtém lista de empresas disponíveis no banco de dados.
    """
    try:
        tipo = config.get('tipo')
        
        if tipo == 'nuvem':
            # PostgreSQL (mantém lógica original mas tenta buscar detalhes)
            dbname = config.get('dbname', '')
            conn_config = {
                'host': 'localhost',
                'port': 5432,
                'user': 'postgres',
                'password': '123',
                'dbname': dbname
            }
            conn = psycopg2.connect(**conn_config)
            cursor = conn.cursor()
            
            # Tentar buscar detalhes na tabela EMPRESA primeiro
            try:
                cursor.execute('SELECT codigo, razao_social, cnpj FROM empresa ORDER BY codigo')
                rows = cursor.fetchall()
                # Retorna formato rico: [{'id': '1', 'nome': 'Razao', 'cnpj': '...'}]
                empresas = []
                for row in rows:
                    empresas.append({
                        'id': str(row[0]),
                        'nome': row[1] or f"Empresa {row[0]}",
                        'cnpj': row[2] or ""
                    })
            except:
                # Fallback se não existir tabela empresa
                conn.rollback() # Limpar erro
                cursor.execute('SELECT DISTINCT cod_empresa FROM vendas ORDER BY cod_empresa')
                # Retorna formato simples (apenas ID) mas encapsulado em dict para padronização
                empresas = [{'id': str(row[0]), 'nome': f"Empresa {row[0]} (Detalhes indisponíveis)", 'cnpj': ''} for row in cursor.fetchall()]

            cursor.close()
            conn.close()
            
        elif tipo == 'local':
            # Firebird via ISQL (NOVO)
            import firebird_isql
            
            # Tentar buscar detalhes tabela EMPRESA
            # Assumindo colunas CODIGO, RAZAO_SOCIAL, CNPJ conforme pedido
            query = 'SELECT CODIGO, RAZAO_SOCIAL, CNPJ FROM EMPRESA ORDER BY CODIGO'
            
            try:
                rows = firebird_isql.execute_query_isql(config, query)
                
                # Se retorno vazio ou erro (isql as vezes não lança exception se tabela nao existe mas retorna erro no stdout que o parser pega como rows vazias ou erro), 
                # vamos validar se tem conteúdo real.
                
                empresas = []
                # Verificar se rows contém chaves esperadas (parser retorna maiúsculo)
                if rows and 'CODIGO' in rows[0]:
                    for row in rows:
                        empresas.append({
                            'id': str(row.get('CODIGO')),
                            'nome': row.get('RAZAO_SOCIAL') or f"Empresa {row.get('CODIGO')}",
                            'cnpj': row.get('CNPJ') or ""
                        })
                else:
                    raise Exception("Tabela EMPRESA não retornou dados esperados")
                    
            except Exception as e:
                logger.warning(f"Falha ao buscar tabela EMPRESA: {e}. Usando fallback VENDAS.")
                # Fallback: SELECT DISTINCT cod_empresa FROM VENDAS
                query_fallback = 'SELECT DISTINCT cod_empresa FROM vendas ORDER BY cod_empresa'
                rows = firebird_isql.execute_query_isql(config, query_fallback)
                
                empresas = []
                for row in rows:
                    val = row.get('COD_EMPRESA')
                    if val:
                        empresas.append({
                            'id': str(val), 
                            'nome': f"Empresa {val} (Nome não encontrado)", 
                            'cnpj': ''
                        })

            # Deduplicar por ID
            seen_ids = set()
            unique_empresas = []
            for emp in empresas:
                if emp['id'] not in seen_ids:
                    seen_ids.add(emp['id'])
                    unique_empresas.append(emp)
                    
            empresas = unique_empresas
            
        else:
            return {
                'sucesso': False,
                'erro': f'Tipo de banco inválido: {tipo}'
            }
        
        return {
            'sucesso': True,
            'empresas': empresas
        }
        
    except Exception as e:
        return {
            'sucesso': False,
            'erro': f'Erro ao obter empresas: {str(e)}'
        }


def executar_analise_db_avancada(config, texto_bruto, lista_series, lista_empresas):
    """
    Executa análise avançada de cupons via banco de dados com:
    - Múltiplas séries
    - Múltiplas empresas
    - Deduplicação por chave NFe
    - Classificação refinada por status (A, C, I, E0001)
    - Agrupamento por série
    
    Regras de Status:
    - 'A' ou nfe_cod_resp=100: Autorizada
    - 'C': Cancelada
    - 'I': Já Inutilizada
    - 'E0001' ou não encontrado: Deve Inutilizar
    
    Args:
        config (dict): Configuração do banco
        texto_bruto (str): Texto com lista de cupons
        lista_series (list): Lista de séries (ex: ['1', '2'])
        lista_empresas (list): Lista de códigos de empresa (ex: ['1', '2'])
        
    Returns:
        dict: Dicionário com resultados agrupados por série
    """
    try:
        # Extrair cupons do texto (com informação de série se disponível)
        cupons_com_serie = _extrair_cupons_com_serie(texto_bruto)
        
        if not cupons_com_serie:
            return {
                'erro': 'Nenhum cupom válido encontrado no texto.',
                'total_processados': 0
            }
        

        # Conectar ao banco
        tipo = config.get('tipo')
        conn = None
        
        # Dicionário para agrupar por série (inicialização global)
        resultados_por_serie = {}
                
        # ROTEAMENTO POR TIPO DE BANCO
        if tipo == 'nuvem':
            # PostgreSQL (Código existente)
            dbname = config.get('dbname', '')
            conn_config = {
                'host': 'localhost',
                'port': 5432,
                'user': 'postgres',
                'password': '123',
                'dbname': dbname
            }
            conn = psycopg2.connect(**conn_config)
            cursor = conn.cursor()
            
            # Construir query SQL dinâmica
            placeholders_series = ','.join(['%s'] * len(lista_series))
            placeholders_empresas = ','.join(['%s'] * len(lista_empresas))
            
            query = f"""
                SELECT cod_empresa, numero_nf, nfe_chave, nfe_status, 
                       nfe_contingencia, cancelada, serie_nf, nfe_cod_resp
                FROM vendas
                WHERE numero_nf = %s
                AND serie_nf IN ({placeholders_series})
                AND cod_empresa IN ({placeholders_empresas})
            """
            
            # Loop e Execução (Postgres)
            for cupom, serie_origem in sorted(cupons_com_serie.items(), key=lambda x: int(x[0])):
                # Aplicar padding de 9 dígitos
                cupom_padded = cupom.zfill(9)
                
                params = [cupom_padded] + lista_series + lista_empresas
                cursor.execute(query, params)
                resultados = cursor.fetchall()
                
                # Processar resultados (função auxiliar abaixo)
                _processar_resultados_analise(resultados, cupom, serie_origem, lista_series, resultados_por_serie)
                
            cursor.close()
            conn.close()

        elif tipo == 'local':
            # Firebird via ISQL (NOVO)
            import firebird_isql
            
            # Construir query SQL para Firebird (ISQL requer IN com valores literais na query,
            # pois nossa função de execução simples não suporta lista no IN via parametro bind simulado)
            
            str_series = ", ".join([f"'{s}'" for s in lista_series])
            str_empresas = ", ".join([f"'{e}'" for e in lista_empresas])
            
            # Query base (sem o numero_nf ainda)
            query_base = f"""
                SELECT cod_empresa, numero_nf, nfe_chave, nfe_status, 
                       nfe_contingencia, cancelada, serie_nf, nfe_cod_resp
                FROM vendas
                WHERE serie_nf IN ({str_series})
                AND cod_empresa IN ({str_empresas})
                AND numero_nf = 
            """
            
            # Loop e Execução (ISQL)
            total_items = len(cupons_com_serie)
            for idx, (cupom, serie_origem) in enumerate(sorted(cupons_com_serie.items(), key=lambda x: int(x[0]))):
                if idx % 5 == 0:
                    logger.debug(f"Processando cupom {idx+1}/{total_items} no Firebird...")
                    
                cupom_padded = cupom.zfill(9)
                
                # Montar query final com o número
                query_final = f"{query_base} '{cupom_padded}'"
                
                # Executar
                try:
                    rows = firebird_isql.execute_query_isql(config, query_final)
                    
                    # Converter formato de dict para tupla (compatibilidade com lógica existente)
                    # Ordem esperada: cod_empresa, numero_nf, nfe_chave, nfe_status, 
                    #                 nfe_contingencia, cancelada, serie_nf, nfe_cod_resp
                    resultados = []
                    for r in rows:
                        tup = (
                            r.get('COD_EMPRESA'),
                            r.get('NUMERO_NF'),
                            r.get('NFE_CHAVE'),
                            r.get('NFE_STATUS'),
                            r.get('NFE_CONTINGENCIA'),
                            r.get('CANCELADA'),
                            r.get('SERIE_NF'),
                            r.get('NFE_COD_RESP')
                        )
                        resultados.append(tup)
                    
                    # Processar resultados
                    _processar_resultados_analise(resultados, cupom, serie_origem, lista_series, resultados_por_serie)
                    
                except Exception as e:
                    logger.error(f"Erro ao consultar cupom {cupom}: {str(e)}")
                    # Tratar como não encontrado/erro
                    # ... (lógica de erro pode ser adicionada aqui se necessário)

        return {
            'total_processados': len(cupons_com_serie),
            'resultados_por_serie': resultados_por_serie,
            'series': sorted(resultados_por_serie.keys()),
            'erro': None
        }
        
    except Exception as e:
        return {
            'erro': f'Erro na análise: {str(e)}',
            'total_processados': 0
        }


def _processar_resultados_analise(resultados, cupom, serie_origem, lista_series, resultados_por_serie):
    """
    Função auxiliar para processar resultados da query (seja Postgres ou Firebird)
    e classificar os cupons.
    """
    # Se não encontrado, adicionar à série de origem ou todas as séries
    if not resultados:
        series_para_adicionar = [serie_origem] if serie_origem else lista_series
        
        for serie in series_para_adicionar:
            if serie not in resultados_por_serie:
                resultados_por_serie[serie] = {
                    'para_inutilizar': [],
                    'autorizadas': [],
                    'canceladas': [],
                    'ja_inutilizadas': []
                }
            resultados_por_serie[serie]['para_inutilizar'].append({
                'cupom': cupom,
                'empresa': 'N/A',
                'motivo': 'Não encontrado no BD',
                'serie_origem': serie_origem
            })
        return

    # Deduplicação: Preferir registros com nfe_chave
    registro = None
    if len(resultados) > 1:
        com_chave = [r for r in resultados if r[2] and str(r[2]).strip()]
        if com_chave:
            registro = com_chave[0]
        else:
            registro = resultados[0]
    else:
        registro = resultados[0]
    
    # Extrair dados
    cod_empresa = registro[0]
    numero_nf = registro[1]
    nfe_chave = registro[2]
    nfe_status = registro[3]
    nfe_contingencia = registro[4]
    cancelada = registro[5]
    serie_nf = str(registro[6])
    nfe_cod_resp = registro[7]
    
    # Inicializar série se não existir
    if serie_nf not in resultados_por_serie:
        resultados_por_serie[serie_nf] = {
            'para_inutilizar': [],
            'autorizadas': [],
            'canceladas': [],
            'ja_inutilizadas': []
        }
    
    # Classificar baseado nas regras refinadas (COM DETALHES RICOS PARA UI)
    detalhes_ricos = {
        'nfe_status': nfe_status,
        'nfe_cod_resp': nfe_cod_resp
    }

    if nfe_status == 'A' or (nfe_cod_resp and str(nfe_cod_resp) == '100'):
        # Autorizada
        resultados_por_serie[serie_nf]['autorizadas'].append({
            'cupom': cupom,
            'empresa': cod_empresa,
            'motivo': f"Autorizada (Cód: {nfe_cod_resp or '100'})",
            'status_real': nfe_status,
            'cod_resp': nfe_cod_resp,
            'serie_origem': serie_origem,
            'serie_bd': serie_nf
        })
        
    elif nfe_status == 'C':
        # Cancelada
        resultados_por_serie[serie_nf]['canceladas'].append({
            'cupom': cupom,
            'empresa': cod_empresa,
            'motivo': f"Cancelada (Cód: {nfe_cod_resp or '101'})",
            'status_real': nfe_status,
            'cod_resp': nfe_cod_resp,
            'serie_origem': serie_origem,
            'serie_bd': serie_nf
        })
        
    elif nfe_status == 'I':
        # Já Inutilizada
        resultados_por_serie[serie_nf]['ja_inutilizadas'].append({
            'cupom': cupom,
            'empresa': cod_empresa,
            'motivo': f"Já Inutilizada (Status: I)",
            'status_real': nfe_status,
            'cod_resp': nfe_cod_resp,
            'serie_origem': serie_origem,
            'serie_bd': serie_nf
        })
        
    elif str(nfe_cod_resp) == 'E0001':
        # Deve Inutilizar (Erro E0001)
        resultados_por_serie[serie_nf]['para_inutilizar'].append({
            'cupom': cupom,
            'empresa': cod_empresa,
            'motivo': 'Erro de Envio (E0001)',
            'status_real': nfe_status,
            'cod_resp': nfe_cod_resp,
            'serie_origem': serie_origem,
            'serie_bd': serie_nf
        })
        
    else:
        # Status desconhecido ou erro - deve inutilizar
        motivo_str = f"Status: {nfe_status or 'N/A'}"
        if nfe_cod_resp:
            motivo_str += f", Resp: {nfe_cod_resp}"
            
        resultados_por_serie[serie_nf]['para_inutilizar'].append({
            'cupom': cupom,
            'empresa': cod_empresa,
            'motivo': motivo_str,
            'status_real': nfe_status,
            'cod_resp': nfe_cod_resp,
            'serie_origem': serie_origem,
            'serie_bd': serie_nf
        })
