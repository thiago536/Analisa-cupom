
import customtkinter as ctk
import tkinter as tk  # Para messagebox e filetypes
from tkinter import messagebox, filedialog
import os
import sys
import threading
import logging_utils
from PIL import Image
import ctypes

# Inicializar logger
logger = logging_utils.get_logger()

# Configuração do AppUserModelID para o ícone aparecer na Barra de Tarefas
try:
    myappid = 'eprosys.analisa_cupom.modern.v1' # Identificador único arbitrário
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
except Exception as e:
    logger.warning(f"Não foi possível definir AppUserModelID: {e}")

# Importar lógica
from logic import testar_conexao_db_universal, converter_pdf_para_excel

def resource_path(relative_path):
    """Retorna caminho absoluto para recursos (funciona em dev e PyInstaller)"""
    try:
        # PyInstaller cria pasta temporária em _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Configuração do Tema
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Configurar Icone da Janela (Title Bar)
        try:
            # Tentar carregar icon.icon.ico ou icon.ico
            icon_name = 'icon.icon.ico'
            icon_file = resource_path(os.path.join('Icon', icon_name))
            
            if not os.path.exists(icon_file):
                 icon_file = resource_path(os.path.join('Icon', 'icon.ico'))
            
            if os.path.exists(icon_file):
                self.iconbitmap(icon_file)
            else:
                logger.warning(f"Arquivo de ícone não encontrado em: {icon_file}")
        except Exception as e:
            logger.warning(f"Erro ao definir ícone da janela: {e}")

        # Configurações da Janela
        self.title("Analisa Quebra de Sequência - Modern")
        self.geometry("1100x850")
        
        # Variáveis de Dados
        self.empresas_disponiveis = [] # Agora será lista de dicts: [{'id':..., 'nome':..., 'cnpj':...}]
        self.empresas_selecionadas = [] # Lista de IDs (strings)
        
        self.db_type = ctk.StringVar(value='nuvem')
        self.db_nuvem_nome = ctk.StringVar()
        self.db_local_path = ctk.StringVar()
        self.db_local_user = ctk.StringVar(value='')
        self.db_local_pass = ctk.StringVar(value='')
        
        self.serie_alvo = ctk.StringVar()
        self.path_pdf_db = ctk.StringVar()
        
        # Layout Principal
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        
        # Frame Scrollável Principal
        self.main_scroll = ctk.CTkScrollableFrame(self)
        self.main_scroll.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        self.main_scroll.grid_columnconfigure(0, weight=1)
        
        # Configurar Imagem do Cabeçalho
        self.logo_image = None
        try:
            # Reutilizar lógica de path seguro
            icon_name = 'icon.icon.ico'
            icon_path = resource_path(os.path.join('Icon', icon_name))
            
            if os.path.exists(icon_path):
                pil_image = Image.open(icon_path)
                self.logo_image = ctk.CTkImage(light_image=pil_image, dark_image=pil_image, size=(40, 40))
        except Exception as e:
            logger.warning(f"Erro ao carregar imagem logo: {e}")

        # Título
        self.lbl_title = ctk.CTkLabel(
            self.main_scroll, 
            text="  Análise de Banco de Dados", 
            image=self.logo_image,
            compound="left",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        self.lbl_title.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="w")
        
        # === SEÇÃO 1: CONEXÃO ===
        self.frame_conexao = ctk.CTkFrame(self.main_scroll)
        self.frame_conexao.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.frame_conexao.grid_columnconfigure(1, weight=1)
        
        self.lbl_conexao = ctk.CTkLabel(
            self.frame_conexao, 
            text="Configurações de Conexão", 
            font=ctk.CTkFont(size=16, weight="bold")
        )
        self.lbl_conexao.grid(row=0, column=0, columnspan=2, padx=15, pady=10, sticky="w")
        
        # Radio Buttons
        self.radio_frame = ctk.CTkFrame(self.frame_conexao, fg_color="transparent")
        self.radio_frame.grid(row=1, column=0, columnspan=3, padx=15, pady=5, sticky="w")
        
        self.rb_nuvem = ctk.CTkRadioButton(
            self.radio_frame, 
            text="Nuvem (PostgreSQL)", 
            variable=self.db_type, 
            value="nuvem",
            command=self.atualizar_widgets_db
        )
        self.rb_nuvem.pack(side="left", padx=10)
        
        self.rb_local = ctk.CTkRadioButton(
            self.radio_frame, 
            text="Local (Firebird)", 
            variable=self.db_type, 
            value="local",
            command=self.atualizar_widgets_db
        )
        self.rb_local.pack(side="left", padx=10)
        
        # Campos PostgreSQL
        self.frame_nuvem = ctk.CTkFrame(self.frame_conexao, fg_color="transparent")
        self.frame_nuvem.grid(row=2, column=0, columnspan=3, sticky="ew", padx=15, pady=5)
        
        ctk.CTkLabel(self.frame_nuvem, text="Nome do Banco:").pack(side="left", padx=5)
        self.entry_nuvem = ctk.CTkEntry(self.frame_nuvem, textvariable=self.db_nuvem_nome, width=300)
        self.entry_nuvem.pack(side="left", padx=5, expand=True, fill="x")

        # Campos Firebird
        self.frame_local = ctk.CTkFrame(self.frame_conexao, fg_color="transparent")
        self.frame_local.grid(row=2, column=0, columnspan=3, sticky="ew", padx=15, pady=5)
        
        # Linha 1: Arquivo
        self.frame_local_linha1 = ctk.CTkFrame(self.frame_local, fg_color="transparent")
        self.frame_local_linha1.pack(fill="x", pady=5)
        
        ctk.CTkLabel(self.frame_local_linha1, text="Caminho (.FDB):").pack(side="left", padx=5)
        self.entry_local = ctk.CTkEntry(self.frame_local_linha1, textvariable=self.db_local_path, state="readonly", width=300)
        self.entry_local.pack(side="left", padx=5, expand=True, fill="x")
        
        self.btn_procurar_fdb = ctk.CTkButton(
            self.frame_local_linha1, 
            text="Procurar", 
            width=80, 
            command=self.selecionar_arquivo_fdb
        )
        self.btn_procurar_fdb.pack(side="left", padx=5)
        
        # Linha 2: Credenciais
        self.frame_local_linha2 = ctk.CTkFrame(self.frame_local, fg_color="transparent")
        self.frame_local_linha2.pack(fill="x", pady=5)
        
        ctk.CTkLabel(self.frame_local_linha2, text="Usuário:").pack(side="left", padx=5)
        self.entry_user = ctk.CTkEntry(self.frame_local_linha2, textvariable=self.db_local_user, width=150)
        self.entry_user.pack(side="left", padx=5)
        
        ctk.CTkLabel(self.frame_local_linha2, text="Senha:").pack(side="left", padx=5)
        self.entry_pass = ctk.CTkEntry(self.frame_local_linha2, textvariable=self.db_local_pass, show="*", width=150)
        self.entry_pass.pack(side="left", padx=5)
        
        # Botão Testar
        self.btn_testar = ctk.CTkButton(
            self.frame_conexao, 
            text="Testar Conexão", 
            command=self.testar_conexao,
            fg_color="green", hover_color="darkgreen"
        )
        self.btn_testar.grid(row=3, column=0, columnspan=3, pady=15)
        
        # === SEÇÃO 2: ANÁLISE ===
        self.frame_analise = ctk.CTkFrame(self.main_scroll)
        self.frame_analise.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        self.frame_analise.grid_columnconfigure(0, weight=1)
        
        self.lbl_analise = ctk.CTkLabel(
            self.frame_analise, 
            text="Análise de Cupons", 
            font=ctk.CTkFont(size=16, weight="bold")
        )
        self.lbl_analise.grid(row=0, column=0, padx=15, pady=10, sticky="w")
        
        # Opção A: Texto
        ctk.CTkLabel(self.frame_analise, text="OPÇÃO A: Cole a lista de cupons (um por linha):").grid(row=1, column=0, sticky="w", padx=15)
        self.text_input_cupons = ctk.CTkTextbox(self.frame_analise, height=80)
        self.text_input_cupons.grid(row=2, column=0, padx=15, pady=5, sticky="ew")
        
        ctk.CTkLabel(self.frame_analise, text="OU", font=ctk.CTkFont(weight="bold")).grid(row=3, column=0, pady=10)
        
        # Opção B: PDF
        ctk.CTkLabel(self.frame_analise, text="OPÇÃO B: Carregar PDF (Notas não lançadas):").grid(row=4, column=0, sticky="w", padx=15)
        
        self.frame_pdf = ctk.CTkFrame(self.frame_analise, fg_color="transparent")
        self.frame_pdf.grid(row=5, column=0, sticky="ew", padx=15, pady=5)
        
        self.entry_pdf = ctk.CTkEntry(self.frame_pdf, textvariable=self.path_pdf_db, state="readonly", width=400)
        self.entry_pdf.pack(side="left", expand=True, fill="x", padx=(0, 10))
        
        self.btn_pdf = ctk.CTkButton(self.frame_pdf, text="Procurar PDF", width=100, command=self.selecionar_pdf_db)
        self.btn_pdf.pack(side="left")
        
        # Série Alvo
        self.frame_serie = ctk.CTkFrame(self.frame_analise, fg_color="transparent")
        self.frame_serie.grid(row=6, column=0, sticky="ew", padx=15, pady=15)
        
        ctk.CTkLabel(self.frame_serie, text="Série(s) Alvo (separar por vírgula):").pack(side="left", padx=(0, 10))
        self.entry_serie = ctk.CTkEntry(self.frame_serie, textvariable=self.serie_alvo, width=200)
        self.entry_serie.pack(side="left")
        
        # Botão Analisar
        self.btn_analisar = ctk.CTkButton(
            self.frame_analise, 
            text="▶ INICIAR ANÁLISE", 
            height=50,
            font=ctk.CTkFont(size=15, weight="bold"),
            command=self.iniciar_analise_db
        )
        self.btn_analisar.grid(row=7, column=0, padx=20, pady=20, sticky="ew")
        
        # === SEÇÃO 3: RESULTADOS (GRID NOVO) ===
        self.frame_resultados = ctk.CTkFrame(self.main_scroll)
        self.frame_resultados.grid(row=3, column=0, padx=20, pady=10, sticky="ew")
        self.frame_resultados.grid_columnconfigure(0, weight=1)
        
        self.lbl_resultados = ctk.CTkLabel(
            self.frame_resultados, 
            text="Resultados Detalhados", 
            font=ctk.CTkFont(size=16, weight="bold")
        )
        self.lbl_resultados.grid(row=0, column=0, padx=15, pady=10, sticky="w")
        
        # Cabeçalho da Tabela
        self.header_frame = ctk.CTkFrame(self.frame_resultados, fg_color="#2b2b2b", height=30)
        self.header_frame.grid(row=1, column=0, sticky="ew", padx=15, pady=(0,5))
        self.header_frame.grid_columnconfigure(0, weight=1) # Cupom
        self.header_frame.grid_columnconfigure(1, weight=1) # Série
        self.header_frame.grid_columnconfigure(2, weight=2) # Status
        self.header_frame.grid_columnconfigure(3, weight=2) # Detalhe
        self.header_frame.grid_columnconfigure(4, weight=1) # Recomendaçao
        
        ctk.CTkLabel(self.header_frame, text="Cupom", font=("Arial", 12, "bold")).grid(row=0, column=0, pady=5)
        ctk.CTkLabel(self.header_frame, text="Série", font=("Arial", 12, "bold")).grid(row=0, column=1, pady=5)
        ctk.CTkLabel(self.header_frame, text="Status Sistema", font=("Arial", 12, "bold")).grid(row=0, column=2, pady=5)
        ctk.CTkLabel(self.header_frame, text="Detalhes/Motivo", font=("Arial", 12, "bold")).grid(row=0, column=3, pady=5)
        ctk.CTkLabel(self.header_frame, text="Ação", font=("Arial", 12, "bold")).grid(row=0, column=4, pady=5)
        
        # Scrollable Frame para as linhas da tabela
        self.results_scroll = ctk.CTkScrollableFrame(self.frame_resultados, height=400)
        self.results_scroll.grid(row=2, column=0, sticky="ew", padx=15, pady=(0, 15))
        self.results_scroll.grid_columnconfigure(0, weight=1) 
        self.results_scroll.grid_columnconfigure(1, weight=1)
        self.results_scroll.grid_columnconfigure(2, weight=2)
        self.results_scroll.grid_columnconfigure(3, weight=2)
        self.results_scroll.grid_columnconfigure(4, weight=1)

        # Estado inicial
        self.atualizar_widgets_db()

    def atualizar_widgets_db(self):
        tipo = self.db_type.get()
        if tipo == 'nuvem':
            self.frame_nuvem.grid()
            self.frame_local.grid_remove()
        else:
            self.frame_local.grid()
            self.frame_nuvem.grid_remove()
            
    def selecionar_arquivo_fdb(self):
        f = filedialog.askopenfilename(filetypes=[("Firebird DB", "*.fdb"), ("Todos", "*.*")])
        if f: self.db_local_path.set(f)
            
    def selecionar_pdf_db(self):
        f = filedialog.askopenfilename(filetypes=[("PDF", "*.pdf"), ("Todos", "*.*")])
        if f: self.path_pdf_db.set(f)
        
    def testar_conexao(self):
        messagebox.showinfo("Teste", "Testando conexão... aguarde.")
        tipo = self.db_type.get()
        config = {'tipo': tipo}
        
        if tipo == 'nuvem':
            config['dbname'] = self.db_nuvem_nome.get()
        else:
            config['path'] = self.db_local_path.get()
            config['user'] = self.db_local_user.get()
            config['password'] = self.db_local_pass.get()
            
        def _run():
            res = testar_conexao_db_universal(config)
            if res['sucesso']:
                self.after(0, lambda: messagebox.showinfo("Sucesso", f"Conexão OK!\n{res['mensagem']}"))
            else:
                self.after(0, lambda: messagebox.showerror("Erro", res['erro']))
                
        threading.Thread(target=_run).start()
    
    def limpar_tabela_resultados(self):
        for widget in self.results_scroll.winfo_children():
            widget.destroy()

    def adicionar_linha_tabela(self, row_idx, cupom, serie, status, detalhe, acao, cor_status):
        lbl_cupom = ctk.CTkLabel(self.results_scroll, text=cupom)
        lbl_cupom.grid(row=row_idx, column=0, pady=5)
        
        lbl_serie = ctk.CTkLabel(self.results_scroll, text=serie)
        lbl_serie.grid(row=row_idx, column=1, pady=5)
        
        lbl_status = ctk.CTkLabel(self.results_scroll, text=status, text_color=cor_status, font=("Arial", 12, "bold"))
        lbl_status.grid(row=row_idx, column=2, pady=5)
        
        lbl_detalhe = ctk.CTkLabel(self.results_scroll, text=detalhe)
        lbl_detalhe.grid(row=row_idx, column=3, pady=5)
        
        lbl_acao = ctk.CTkLabel(self.results_scroll, text=acao)
        lbl_acao.grid(row=row_idx, column=4, pady=5)
        
        # Separador visual
        sep = ctk.CTkFrame(self.results_scroll, height=1, fg_color="gray30")
        sep.grid(row=row_idx+1, column=0, columnspan=5, sticky="ew", pady=(0,5))

    def iniciar_analise_db(self):
        self.limpar_tabela_resultados()
        
        texto_bruto = self.text_input_cupons.get("1.0", "end").strip()
        path_pdf = self.path_pdf_db.get().strip()
        series_texto = self.serie_alvo.get().strip()
        tipo = self.db_type.get()
        
        # Validações
        usar_pdf = bool(path_pdf)
        usar_texto = bool(texto_bruto)
        
        if not usar_pdf and not usar_texto:
            messagebox.showerror("Erro", "Selecione um PDF ou cole a lista de cupons.")
            return
            
        if not series_texto:
            messagebox.showerror("Erro", "Digite a(s) série(s) alvo.")
            return
            
        lista_series = [s.strip() for s in series_texto.split(',') if s.strip()]
        
        # Config
        config = {'tipo': tipo}
        if tipo == 'nuvem':
            config['dbname'] = self.db_nuvem_nome.get().strip()
        else:
            config['path'] = self.db_local_path.get().strip()
            config['user'] = self.db_local_user.get().strip()
            config['password'] = self.db_local_pass.get().strip()
            
            if not config['path'] or not config['user'] or not config['password']:
               messagebox.showerror("Erro", "Preencha todos os campos do banco local.")
               return
        
        # Exibir loading na tabela
        lbl_loading = ctk.CTkLabel(self.results_scroll, text="Processando... Aguarde...", font=("Arial", 14))
        lbl_loading.grid(row=0, column=0, columnspan=5, pady=20)

        threading.Thread(target=self._executar_analise_thread, args=(config, texto_bruto, path_pdf, lista_series, usar_pdf)).start()

    def _executar_analise_thread(self, config, texto_bruto, path_pdf, lista_series, usar_pdf):
        try:
            texto_cupons = texto_bruto
            
            # 1. Converter PDF se necessário
            if usar_pdf:
                import tempfile
                temp_dir = tempfile.gettempdir()
                temp_excel = os.path.join(temp_dir, "_temp_db_analise.xlsx")
                
                res_conv = converter_pdf_para_excel(path_pdf, temp_excel)
                if not res_conv['sucesso']:
                    self.after(0, lambda: messagebox.showerror("Erro PDF", res_conv['erro']))
                    return
                
                # Ler Excel
                from logic import _ler_sefaz
                cupons_por_serie = {}
                
                for serie in lista_series:
                    c_set = _ler_sefaz(temp_excel, serie)
                    cupons_por_serie[serie] = sorted(list(c_set), key=lambda x: int(x))
                
                # Formatar
                linhas = []
                for serie, lista in cupons_por_serie.items():
                    for cp in lista:
                        linhas.append(f"{cp}|SERIE_{serie}")
                texto_cupons = '\n'.join(linhas)
                
                try: os.remove(temp_excel)
                except: pass
            
            # 2. Obter Empresas (Agora com detalhes)
            from logic import obter_empresas_disponiveis
            res_emp = obter_empresas_disponiveis(config)
            
            if not res_emp['sucesso']:
                self.after(0, lambda: messagebox.showerror("Erro Banco", res_emp['erro']))
                return
            
            self.empresas_disponiveis = res_emp['empresas']
            
            # 3. Seleção de Empresa
            self.after(0, lambda: self._decidir_empresas(config, texto_cupons, lista_series))
            
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Erro Fatal", str(e)))

    def _decidir_empresas(self, config, texto_cupons, lista_series):
        if len(self.empresas_disponiveis) > 1:
            self.limpar_tabela_resultados() # Limpar msg loading
            self._mostrar_selecao_empresas(config, texto_cupons, lista_series)
        else:
            # Selecionar único ID disponível
            if self.empresas_disponiveis:
                # É uma lista de dicts agora, pegar ID
                if isinstance(self.empresas_disponiveis[0], dict):
                     self.empresas_selecionadas = [self.empresas_disponiveis[0]['id']]
                else: 
                     self.empresas_selecionadas = [self.empresas_disponiveis[0]] # Fallback string
            else:
                self.empresas_selecionadas = []
                
            self._lançar_analise_final(config, texto_cupons, lista_series)

    def _mostrar_selecao_empresas(self, config, texto_cupons, lista_series):
        # Janela Modal Rica
        top = ctk.CTkToplevel(self)
        top.title("Selecionar Empresas")
        top.geometry("600x600")
        top.transient(self)
        top.grab_set()
        
        ctk.CTkLabel(top, text="Selecione as Empresas Encontradas", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)
        
        ctk.CTkLabel(top, text="O sistema encontrou múltiplas empresas no banco de dados.", text_color="gray").pack()
        
        scroll = ctk.CTkScrollableFrame(top)
        scroll.pack(fill="both", expand=True, padx=20, pady=10)
        
        vars_emp = {}
        for emp_data in self.empresas_disponiveis:
            # emp_data é um dict: {'id', 'nome', 'cnpj'}
            emp_id = emp_data['id']
            emp_nome = emp_data['nome']
            emp_cnpj = emp_data.get('cnpj', '')
            
            display_text = f"Cód: {emp_id} - {emp_nome}"
            if emp_cnpj:
                display_text += f" (CNPJ: {emp_cnpj})"
            
            var = ctk.BooleanVar(value=True)
            vars_emp[emp_id] = var
            
            # Card style checkbox
            f = ctk.CTkFrame(scroll, fg_color="#333333")
            f.pack(fill="x", pady=2, padx=2)
            cb = ctk.CTkCheckBox(f, text=display_text, variable=var, font=("Arial", 12))
            cb.pack(side="left", padx=10, pady=10)
            
        def _confirmar():
            sel = [eid for eid, v in vars_emp.items() if v.get()]
            if not sel:
                messagebox.showwarning("Aviso", "Selecione pelo menos uma.")
                return
            self.empresas_selecionadas = sel
            top.destroy()
            self._lançar_analise_final(config, texto_cupons, lista_series)
            
        ctk.CTkButton(top, text="Confirmar Seleção", command=_confirmar, height=40).pack(pady=20)

    def _lançar_analise_final(self, config, texto_cupons, lista_series):
        threading.Thread(
            target=self._run_analise_final, 
            args=(config, texto_cupons, lista_series)
        ).start()

    def _run_analise_final(self, config, texto_cupons, lista_series):
        from logic import executar_analise_db_avancada
        res = executar_analise_db_avancada(
            config, 
            texto_cupons, 
            lista_series, 
            self.empresas_selecionadas
        )
        self.after(0, lambda: self._exibir_resultados_db(res))

    def _exibir_resultados_db(self, resultado):
        self.limpar_tabela_resultados()
        
        if resultado.get('erro'):
            messagebox.showerror("Erro", resultado['erro'])
            return
            
        res_series = resultado.get('resultados_por_serie', {})
        row_counter = 0
        total_inutilizar = 0
        
        for serie, dados in res_series.items():
            # Cabeçalho da série na grid
            header_serie = ctk.CTkLabel(self.results_scroll, text=f"--- SÉRIE {serie} ---", font=("Arial", 14, "bold"))
            header_serie.grid(row=row_counter, column=0, columnspan=5, pady=10)
            row_counter += 2
            
            # --- Inutilizar ---
            for item in dados.get('para_inutilizar', []):
                cupom = item['cupom']
                motivo_raw = item['motivo']
                
                status_u = "NÃO ENCONTRADO"
                cor = "#FF5555" # Vermelho
                acao = "⚠️ Deve Inutilizar"
                
                # Tentar extrair detalhes
                detalhe = motivo_raw
                if "Erro de Envio" in motivo_raw:
                    status_u = "ERRO E0001"
                elif "Status:" in motivo_raw:
                     status_u = "STATUS DESCONHECIDO"
                
                self.adicionar_linha_tabela(row_counter, cupom, serie, status_u, detalhe, acao, cor)
                row_counter += 2
                total_inutilizar += 1
                
            # --- Autorizadas ---
            for item in dados.get('autorizadas', []):
                self.adicionar_linha_tabela(row_counter, item['cupom'], serie, "AUTORIZADA", "Cód: 100", "✅ OK", "#55FF55") # Verde
                row_counter += 2
                
            # --- Canceladas ---
            for item in dados.get('canceladas', []):
                self.adicionar_linha_tabela(row_counter, item['cupom'], serie, "CANCELADA", "Cód: 101/135", "✅ OK (Já cancelada)", "#FFAA00") # Laranja
                row_counter += 2
                
             # --- Já Inutilizadas ---
            for item in dados.get('ja_inutilizadas', []):
                self.adicionar_linha_tabela(row_counter, item['cupom'], serie, "JÁ INUTILIZADA", "Status: I", "✅ OK", "#AAAAAA") # Cinza
                row_counter += 2
        
        msg_final = f"Análise Finalizada!\n\nTotal de cupons para inutilizar: {total_inutilizar}"
        if total_inutilizar > 0:
            messagebox.showwarning("Concluído", msg_final)
        else:
            messagebox.showinfo("Concluído", msg_final)

if __name__ == "__main__":
    app = App()
    app.mainloop()