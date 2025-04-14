import PyPDF2
import urllib.request
import io
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
import json
import os
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('extrator.log')
    ]
)
logger = logging.getLogger(__name__)

def carregar_ultima_edicao(sheet):
    """Carrega o número da última edição buscada da célula H1"""
    try:
        valor = sheet.acell('H1').value
        if valor and valor.isdigit():
            logger.info(f"Última edição carregada: {valor}")
            return int(valor)
        else:
            logger.warning(f"Valor inválido na H1: '{valor}'. Usando padrão 53.")
            return 53
    except Exception as e:
        logger.error(f"Erro ao carregar última edição: {e}")
        return 53

def salvar_ultima_edicao(sheet, n_edicao):
    """Salva o número da última edição na célula H1"""
    try:
        sheet.update(range_name='H1', values=[[str(n_edicao)]])
        logger.info(f"Salva última edição {n_edicao} na H1")
    except Exception as e:
        logger.error(f"Erro ao salvar última edição: {e}")

def salvar_no_google_sheets(sheet, edital):
    """
    Salva os dados do edital no Google Sheets no formato especificado:
    - Uma linha de cabeçalho
    - Cada linha do texto em uma linha separada na coluna D
    - Mantém data, edição e página apenas na primeira linha
    """
    try:
        # Encontra a próxima linha vazia
        todos_valores = sheet.get_all_values()
        proxima_linha = len(todos_valores) + 1
        
        # Adiciona cabeçalho
        cabecalhos = ["Data", "Edição", "Página", "Texto"]
        sheet.update(range_name=f'A{proxima_linha}:D{proxima_linha}', values=[cabecalhos])
        logger.info(f"Cabeçalho adicionado na linha {proxima_linha}")

        # Divide o texto em linhas
        linhas_texto = edital["texto"].split('\n')
        logger.info(f"Encontradas {len(linhas_texto)} linhas de texto")

        # Salva cada linha do texto
        for i, linha in enumerate(linhas_texto):
            if not linha.strip():  # Ignora linhas vazias
                continue
                
            valores = [
                edital["data"] if i == 0 else "",
                edital["edicao"] if i == 0 else "",
                edital["pagina"] if i == 0 else "",
                linha.strip()
            ]
            
            range_to_update = f'A{proxima_linha + 1 + i}:D{proxima_linha + 1 + i}'
            sheet.update(range_name=range_to_update, values=[valores])
            logger.debug(f"Linha {proxima_linha + 1 + i} atualizada")

        logger.info(f"Dados do edital {edital['edicao']} salvos com sucesso")
    except Exception as e:
        logger.error(f"Erro ao salvar edital: {e}")
        raise

def get_google_credentials():
    """Obtém credenciais do Google a partir de variáveis de ambiente ou arquivo"""
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        if 'GOOGLE_CREDENTIALS' in os.environ:
            logger.info("Usando credenciais de variáveis de ambiente")
            return Credentials.from_service_account_info(
                json.loads(os.environ['GOOGLE_CREDENTIALS']),
                scopes=scope
            )
        elif os.path.exists('projectodedados.json'):
            logger.info("Usando credenciais de arquivo local")
            return Credentials.from_service_account_file(
                'projectodedados.json',
                scopes=scope
            )
        else:
            raise Exception("Nenhuma credencial encontrada")
    except Exception as e:
        logger.error(f"Erro nas credenciais: {e}")
        raise

def processar_pdf(pdf_content, n_edicao, data_edicao):
    """Processa o conteúdo do PDF em busca de editais"""
    lista_editais = []
    leitor_pdf = PyPDF2.PdfReader(pdf_content)
    
    for numero_pagina, pagina in enumerate(leitor_pdf.pages, start=1):
        try:
            texto_pagina = pagina.extract_text()
            if not texto_pagina:
                continue
                
            # Normaliza o texto para busca
            texto_normalizado = texto_pagina.lower().replace('\n', ' ')
            
            if "edital de chamamento" in texto_normalizado:
                posicao = texto_normalizado.find("edital de chamamento")
                trecho_texto = texto_pagina[max(0, posicao-50):posicao+500]
                
                logger.info(f"Edital encontrado na página {numero_pagina}")
                logger.debug(f"Trecho:\n{trecho_texto[:200]}...")

                edital = {
                    'data': data_edicao,
                    'edicao': n_edicao,
                    'pagina': numero_pagina,
                    'texto': trecho_texto.strip()
                }
                lista_editais.append(edital)
        except Exception as e:
            logger.error(f"Erro ao processar página {numero_pagina}: {e}")
    
    return lista_editais

def diario_oficial_df(request, context):
    """Função principal para extração de dados"""
    try:
        logger.info("Iniciando extração de dados")
        
        # Configuração inicial
        creds = get_google_credentials()
        client = gspread.authorize(creds)
        spreadsheet = client.open("editais_chamamento_dodf_code")
        sheet = spreadsheet.sheet1
        
        # Data e edição
        hoje = datetime.today()
        if hoje.weekday() == 5:  # Sábado
            hoje -= timedelta(days=1)
        elif hoje.weekday() == 6:  # Domingo
            hoje -= timedelta(days=2)
            
        n_edicao = carregar_ultima_edicao(sheet) + 1
        data_edicao = f"{hoje.day:02d}-{hoje.month:02d}-{hoje.year}"
        edicao_formatada = f"{n_edicao:03d}"
        
        logger.info(f"Processando edição {n_edicao} para {data_edicao}")

        # Construção da URL
        meses = ["01_Janeiro", "02_Fevereiro", "03_Março", "04_Abril",
                "05_Maio", "06_Junho", "07_Julho", "08_Agosto",
                "09_Setembro", "10_Outubro", "11_Novembro", "12_Dezembro"]
        mes_pasta = meses[hoje.month - 1]
        
        pasta_codificada = quote(f"{hoje.year}|{mes_pasta}|DODF {edicao_formatada} {data_edicao}|")
        arquivo_codificado = quote(f"DODF {edicao_formatada} {data_edicao} INTEGRA.pdf")
        url = f"https://dodf.df.gov.br/dodf/jornal/visualizar-pdf?pasta={pasta_codificada}&arquivo={arquivo_codificada}"
        
        logger.info(f"URL gerada: {url}")

        # Download e processamento do PDF
        try:
            with urllib.request.urlopen(url) as response:
                pdf_content = io.BytesIO(response.read())
            
            lista_editais = processar_pdf(pdf_content, n_edicao, data_edicao)
            
            if lista_editais:
                logger.info(f"Encontrados {len(lista_editais)} editais")
                for edital in lista_editais:
                    salvar_no_google_sheets(sheet, edital)
            else:
                logger.info("Nenhum edital encontrado neste PDF")
                
            # Atualiza última edição mesmo se não encontrar editais
            salvar_ultima_edicao(sheet, n_edicao)
            
            return "Dados atualizados com sucesso!" if lista_editais else "Nenhum edital encontrado."
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.warning(f"Edição {n_edicao} não encontrada (404)")
            else:
                logger.error(f"Erro HTTP ao acessar edição {n_edicao}: {e}")
            salvar_ultima_edicao(sheet, n_edicao)
            return f"Erro ao acessar PDF: {e}"
            
    except Exception as e:
        logger.error(f"Erro crítico: {e}", exc_info=True)
        return f"Erro na execução: {str(e)}"

if __name__ == "__main__":
    print("Iniciando execução local...")
    diario_oficial_df(None, None)