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

# Lista de meses para construção da URL
meses = [
    "01_Janeiro", "02_Fevereiro", "03_Março", "04_Abril",
    "05_Maio", "06_Junho", "07_Julho", "08_Agosto",
    "09_Setembro", "10_Outubro", "11_Novembro", "12_Dezembro"
]

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
        sheet.update('H1', str(n_edicao))
        logger.info(f"Salva última edição {n_edicao} na H1")
        return True
    except Exception as e:
        logger.error(f"Falha ao salvar última edição: {str(e)}")
        return False

def salvar_no_google_sheets(sheet, edital):
    """
    Salva os dados do edital no Google Sheets.
    - Adiciona uma linha de cabeçalho antes dos dados de cada edital.
    - Quebra o texto em várias células, com cada linha do texto em uma célula diferente.
    """
    try:
        # Encontra a próxima linha vazia
        todos_valores = sheet.get_all_values()
        proxima_linha = len(todos_valores) + 1

        # Adiciona cabeçalho
        cabecalhos = ["Data", "Edição", "Página", "Texto"]
        sheet.append_row(cabecalhos)
        logger.info(f"Cabeçalho adicionado na linha {proxima_linha}")

        # Divide o texto em linhas
        linhas_texto = [linha for linha in edital["texto"].split('\n') if linha.strip()]
        logger.info(f"Encontradas {len(linhas_texto)} linhas de texto")

        # Salva cada linha do texto
        for i, linha in enumerate(linhas_texto):
            valores = [
                edital["data"] if i == 0 else "",
                edital["edicao"] if i == 0 else "",
                edital["pagina"] if i == 0 else "",
                linha.strip()
            ]
            sheet.append_row(valores)
            logger.debug(f"Linha {proxima_linha + 1 + i} adicionada")

        logger.info(f"Dados do edital {edital['edicao']} salvos com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar edital: {str(e)}")
        return False

def get_google_credentials():
    """Obtém as credenciais do Google com verificação rigorosa"""
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        if 'GOOGLE_CREDS_JSON' in os.environ:
            logger.info("Usando credenciais do GitHub Secrets")
            creds_json = os.environ['GOOGLE_CREDS_JSON']
            
            # Verificação extra do JSON
            try:
                json.loads(creds_json)  # Testa se é JSON válido
                return Credentials.from_service_account_info(
                    json.loads(creds_json),
                    scopes=scope
                )
            except json.JSONDecodeError as e:
                logger.error("JSON de credenciais inválido!")
                raise

        elif os.path.exists('projectodedados.json'):
            logger.info("Usando credenciais locais")
            return Credentials.from_service_account_file(
                'projectodedados.json',
                scopes=scope
            )
        else:
            raise Exception("Nenhuma credencial encontrada")
            
    except Exception as e:
        logger.error(f"Falha na autenticação: {str(e)}")
        raise

def processar_pdf(pdf_content, n_edicao, data_edicao):
    """Processa o PDF em busca de editais de chamamento"""
    lista_editais = []
    try:
        pdf = PyPDF2.PdfReader(pdf_content)
        logger.info(f"PDF processado - {len(pdf.pages)} páginas")

        for pagina_num, pagina in enumerate(pdf.pages, start=1):
            try:
                texto = pagina.extract_text()
                if not texto:
                    continue

                # Busca por variações da frase
                texto_normalizado = texto.lower().replace('\n', ' ')
                if ("edital de chamamento" in texto_normalizado or 
                    "edital chamamento" in texto_normalizado or
                    "edital n°" in texto_normalizado):
                    
                    pos = texto_normalizado.find("edital")
                    trecho = texto[max(0, pos-100):pos+500]  # Pega contexto

                    logger.info(f"Edital encontrado na página {pagina_num}")
                    logger.debug(f"Trecho extraído:\n{trecho[:200]}...")

                    lista_editais.append({
                        'data': data_edicao,
                        'edicao': n_edicao,
                        'pagina': pagina_num,
                        'texto': trecho.strip()
                    })
            except Exception as e:
                logger.warning(f"Erro ao processar página {pagina_num}: {str(e)}")

        return lista_editais
    except Exception as e:
        logger.error(f"Erro ao processar PDF: {str(e)}")
        return []

def diario_oficial_df(request, context):
    """Função principal para extração de dados"""
    try:
        logger.info("=== INÍCIO DA EXECUÇÃO ===")

        # 1. Conexão com Google Sheets
        try:
            creds = get_google_credentials()
            client = gspread.authorize(creds)
            planilha = client.open("editais_chamamento_dodf_code")
            sheet = planilha.sheet1
            
            # Teste de escrita imediato
            sheet.update('J1', [["Teste de conexão em " + str(datetime.now())]])
            logger.info("Teste de conexão bem-sucedido")
        except Exception as e:
            logger.error(f"FALHA NA CONEXÃO: {str(e)}")
            return "Erro: Falha na autenticação com Google Sheets"

        # 2. Controle de edições
        try:
            ultima_edicao = carregar_ultima_edicao(sheet)
            n_edicao = ultima_edicao + 1
            logger.info(f"Edição atual: {ultima_edicao} | Próxima: {n_edicao}")
        except Exception as e:
            logger.error(f"ERRO NO CONTADOR: {str(e)}")
            return "Erro: Falha ao acessar contador H1"

        # 3. Download do PDF
        try:
            hoje = datetime.today()
            if hoje.weekday() == 5: hoje -= timedelta(days=1)  # Ajuste para sábado
            elif hoje.weekday() == 6: hoje -= timedelta(days=2)  # Ajuste para domingo

            data_edicao = f"{hoje.day:02d}-{hoje.month:02d}-{hoje.year}"
            url = f"https://dodf.df.gov.br/dodf/jornal/visualizar-pdf?pasta={quote(f'{hoje.year}|{meses[hoje.month-1]}|DODF {n_edicao:03d} {data_edicao}|')}&arquivo={quote(f'DODF {n_edicao:03d} {data_edicao} INTEGRA.pdf')}"
            
            logger.info(f"URL: {url}")
            
            with urllib.request.urlopen(url) as response:
                pdf_content = io.BytesIO(response.read())
                if len(pdf_content.getvalue()) < 1024:
                    raise Exception("PDF vazio ou inválido")
        except urllib.error.HTTPError as e:
            logger.error(f"PDF não encontrado (HTTP {e.code})")
            salvar_ultima_edicao(sheet, n_edicao)  # Atualiza mesmo se falhar
            return f"Erro: Edição {n_edicao} não encontrada"
        except Exception as e:
            logger.error(f"ERRO NO PDF: {str(e)}")
            salvar_ultima_edicao(sheet, n_edicao)
            return "Erro: Falha no download do PDF"

        # 4. Processamento e salvamento
        try:
            editais = processar_pdf(pdf_content, n_edicao, data_edicao)
            if editais:
                for edital in editais:
                    salvar_no_google_sheets(sheet, edital)
            
            # Atualiza contador independentemente de encontrar editais
            salvar_ultima_edicao(sheet, n_edicao)
            
            return "Sucesso" + (f" ({len(editais)} editais)" if editais else " (0 editais)")
            
        except Exception as e:
            logger.error(f"ERRO NO PROCESSAMENTO: {str(e)}")
            return "Erro: Falha ao processar dados"

    except Exception as e:
        logger.error(f"ERRO GRAVE: {str(e)}", exc_info=True)
        return "Erro crítico na execução"

if __name__ == "__main__":
    print("Iniciando execução local...")
    resultado = diario_oficial_df(None, None)
    print(resultado)