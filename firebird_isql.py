"""
Módulo para conexão Firebird usando isql-fb (ferramenta de linha de comando do Firebird).
Solução alternativa ao fdb que tem problemas de compatibilidade 32/64 bits.
"""

import subprocess
import os
import tempfile
import re

def _find_isql():
    """Encontra o executável isql.exe no sistema."""
    possible_isql_paths = [
        r'C:\Program Files (x86)\Firebird\bin\isql.exe',
        r'C:\Program Files\Firebird\bin\isql.exe',
        r'C:\Firebird\bin\isql.exe',
    ]
    
    for path in possible_isql_paths:
        if os.path.exists(path):
            return path
    return None

def test_firebird_connection_isql(database_path, user, password):
    """
    Testa conexão Firebird usando isql-fb.
    """
    try:
        isql_path = _find_isql()
        if not isql_path:
            return {
                'sucesso': False,
                'erro': 'Ferramenta isql.exe não encontrada. Verifique se o Firebird está instalado.'
            }
        
        # Criar arquivo temporário com script SQL
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            sql_file = f.name
            f.write("SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') FROM RDB$DATABASE;\n")
            f.write("QUIT;\n")
        
        try:
            # DSN com aspas para suportar espaços no caminho
            dsn = f'localhost:"{database_path}"'
            cmd = [isql_path, '-user', user, '-password', password, dsn, '-i', sql_file]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            )
            
            if result.returncode == 0:
                output = result.stdout
                version = "Desconhecida"
                for line in output.split('\n'):
                    if line.strip() and not line.startswith('SQL>') and not line.startswith('CON>'):
                        version = line.strip()
                        break
                
                return {
                    'sucesso': True,
                    'mensagem': f'Conexão estabelecida com sucesso!\n\nTipo: Firebird (via isql-fb)\nServidor: localhost\nArquivo: {os.path.basename(database_path)}\nUsuário: {user}\nVersão: {version}'
                }
            else:
                # Retornar erro bruto para debug
                return {'sucesso': False, 'erro': _parse_error(result)}
                
        finally:
            try:
                os.unlink(sql_file)
            except:
                pass
                
    except Exception as e:
        return {'sucesso': False, 'erro': f'Erro inesperado: {str(e)}'}

def execute_query_isql(config, query_sql, params=None):
    """
    Executa uma query SQL genérica usando isql e retorna lista de dicionários.
    Usa 'SET LIST ON' para saída formatada chave-valor.
    
    Args:
        config (dict): {'path': ..., 'user': ..., 'password': ...}
        query_sql (str): Query SQL. Use placeholders %s ou ? se params fornecido (mas isql não suporta bind nativo aqui, faremos interpolação segura manual simples ou assumiremos query pronta).
                         NOTA: Para simplificar, assuma que a query já vem formatada ou faça replace básico.
        params (list/tuple): Opcional. 
    
    Returns:
        list[dict]: Lista de linhas retornadas.
    """
    try:
        isql_path = _find_isql()
        if not isql_path:
            raise Exception("isql.exe não encontrado")

        path = config.get('path')
        user = config.get('user')
        password = config.get('password')

        # Interpolação manual básica de parâmetros (CUIDADO com injeção em prod, mas ok para uso local controlado)
        if params:
            # Substituir %s ou ? pelos valores
            # Esta é uma implementação simplificada para o caso de uso específico (inteiros e strings simples)
            final_query = query_sql
            for p in params:
                val = str(p)
                if isinstance(p, str):
                    val = f"'{val}'"
                final_query = final_query.replace('%s', val, 1)
        else:
            final_query = query_sql

        # Preparar script
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            sql_file = f.name
            f.write("SET LIST ON;\n")  # Formato Chave: Valor
            f.write(f"{final_query};\n")
            f.write("QUIT;\n")
            
        try:
            # DSN com aspas para suportar espaços no caminho
            dsn = f'localhost:"{path}"'
            cmd = [isql_path, '-user', user, '-password', password, dsn, '-i', sql_file]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30, # Timeout maior para queries pesadas
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            )
            
            if result.returncode != 0:
                raise Exception(_parse_error(result))
            
            return _parse_list_output(result.stdout)
            
        finally:
            try:
                os.unlink(sql_file)
            except:
                pass

    except Exception as e:
        # Re-raise para tratar na logic.py
        raise Exception(f"Erro no ISQL: {str(e)}")

def _parse_error(result):
    """Extrai mensagem de erro amigável."""
    error_msg = result.stderr if result.stderr else result.stdout
    if 'unavailable' in error_msg.lower() or 'network' in error_msg.lower():
        return f'Servidor Firebird não está rodando ou inacessível.\nErro original: {error_msg}'
    elif 'password' in error_msg.lower() or 'user' in error_msg.lower():
        return f'Usuário ou senha inválidos (Verifique se o usuário SYSDBA existe).\nErro original: {error_msg}'
    return error_msg

def _parse_list_output(output):
    """
    Faz o parse da saída do isql com SET LIST ON.
    Formato esperado:
    
    COLUNA1                         Valor
    COLUNA2                         Valor
    
    COLUNA1                         Valor2
    ...
    """
    rows = []
    current_row = {}
    
    lines = output.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            if current_row:
                rows.append(current_row)
                current_row = {}
            continue
            
        if line.startswith('SQL>'):
            continue
            
        # Tentar separar chave e valor
        # ISQL com SET LIST ON usa um número fixo de espaços ou tab, 
        # mas geralmente a coluna tem 32 chars.
        # Vamos tentar split no primeiro espaço longo ou apenas primeiro espaço
        
        # Regex para pegar primeira palavra (coluna) e o resto (valor)
        match = re.match(r'^(\S+)\s+(.*)$', line)
        if match:
            col = match.group(1)
            val = match.group(2).strip()
            current_row[col] = val
            
    # Adicionar última linha se existir
    if current_row:
        rows.append(current_row)
        
    return rows
