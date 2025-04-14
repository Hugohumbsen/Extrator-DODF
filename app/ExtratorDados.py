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
    """Obtém as credenciais do Google a partir das variáveis de ambiente"""
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        if 'GOOGLE_CREDENTIALS' in os.environ:
            logger.info("Usando credenciais do GitHub Actions")
            creds_info = json.loads(os.environ['GOOGLE_CREDENTIALS'])
            return Credentials.from_service_account_info(creds_info, scopes=scope)
        elif os.path.exists('projectodedados.json'):
            logger.info("Usando credenciais locais do arquivo")
            return Credentials.from_service_account_file('projectodedados.json', scopes=scope)
        else:
            raise Exception("Nenhuma credencial do Google encontrada")
    except Exception as e:
        logger.error(f"Erro nas credenciais: {str(e)}")
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
                texto_normalizado = texto.lower()
                if "edital de chamamento" in texto_normalizado:
                    pos = texto_normalizado.find("edital de chamamento")
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
        logger.info("Iniciando extração do DODF")

        # 1. Autenticação
        try:
            creds = get_google_credentials()
            client = gspread.authorize(creds)
            planilha = client.open("editais_chamamento_dodf_code")
            sheet = planilha.sheet1
            logger.info("Conexão com Google Sheets estabelecida")
        except Exception as e:
            logger.error(f"Falha na conexão com Google Sheets: {str(e)}")
            return "Erro: Falha na conexão com a planilha"

        # 2. Determinar edição atual
        try:
            ultima_edicao = carregar_ultima_edicao(sheet)
            n_edicao = ultima_edicao + 1
            logger.info(f"Última edição: {ultima_edicao}, Próxima: {n_edicao}")
        except Exception as e:
            logger.error(f"Erro ao carregar última edição: {str(e)}")
            return "Erro: Não foi possível determinar a última edição"

        # 3. Preparar data e URL
        hoje = datetime.today()
        if hoje.weekday() == 5: hoje -= timedelta(days=1)  # Sábado
        elif hoje.weekday() == 6: hoje -= timedelta(days=2)  # Domingo

        data_edicao = f"{hoje.day:02d}-{hoje.month:02d}-{hoje.year}"
        edicao_formatada = f"{n_edicao:03d}"
        
        meses = ["01_Janeiro", "02_Fevereiro", "03_Março", "04_Abril",
                "05_Maio", "06_Junho", "07_Julho", "08_Agosto",
                "09_Setembro", "10_Outubro", "11_Novembro", "12_Dezembro"]
        mes_pasta = meses[hoje.month - 1]

        url = f"https://dodf.df.gov.br/dodf/jornal/visualizar-pdf?pasta={quote(f'{hoje.year}|{mes_pasta}|DODF {edicao_formatada} {data_edicao}|')}&arquivo={quote(f'DODF {edicao_formatada} {data_edicao} INTEGRA.pdf')}"
        logger.info(f"URL gerada: {url}")

        # 4. Baixar e processar PDF
        try:
            with urllib.request.urlopen(url) as response:
                pdf_content = io.BytesIO(response.read())
                logger.info(f"PDF baixado - {len(pdf_content.getvalue())} bytes")

            editais = processar_pdf(pdf_content, n_edicao, data_edicao)
            logger.info(f"Encontrados {len(editais)} editais")

            # 5. Salvar resultados
            if editais:
                for edital in editais:
                    if not salvar_no_google_sheets(sheet, edital):
                        logger.error("Falha ao salvar edital")
            
            # 6. Atualizar contador (sempre, mesmo sem editais)
            if not salvar_ultima_edicao(sheet, n_edicao):
                logger.error("Falha ao atualizar contador")

            return "Processamento concluído" + ("" if editais else " (nenhum edital encontrado)")

        except urllib.error.HTTPError as e:
            logger.error(f"Erro HTTP {e.code} ao acessar {url}")
            # Atualiza contador mesmo se o PDF não existir
            salvar_ultima_edicao(sheet, n_edicao)
            return f"Erro: Edição não encontrada (HTTP {e.code})"
            
        except Exception as e:
            logger.error(f"Erro ao processar PDF: {str(e)}")
            return "Erro: Falha no processamento do PDF"

    except Exception as e:
        logger.error(f"Erro crítico: {str(e)}")
        return f"Erro: {str(e)}"

if __name__ == "__main__":
    print("Iniciando execução local...")
    resultado = diario_oficial_df(None, None)
    print(resultado)