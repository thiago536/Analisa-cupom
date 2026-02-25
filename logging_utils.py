import logging
import os
import datetime

def setup_logger():
    """
    Configura o sistema de logs para salvar em arquivo e mostrar no console.
    Cria um arquivo de log novo para cada dia/sessão na pasta 'logs'.
    """
    # Criar diretório de logs se não existir
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Nome do arquivo de log com timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"log_{timestamp}.txt")

    # Configuração do formatador
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    # Handler de Arquivo
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # Handler de Console (opcional, já que o app tem output visual, mas bom para debug)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Configurar Logger Raiz
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capturar tudo
    
    # Limpar handlers anteriores para evitar duplicação
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info(f"Iniciando sessão de log. Arquivo: {log_file}")
    return logger

def get_logger():
    """Retorna o logger configurado."""
    return logging.getLogger()
