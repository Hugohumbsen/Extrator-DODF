import PyPDF2
import urllib.request
import io
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
import os
import json
import logging

# Configuração de logging (apenas para terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Remove FileHandler para não criar arquivo local
    ]
)
logger = logging.getLogger(__name__)

# Lista de meses
meses = [
    "01_Janeiro", "02_Fevereiro", "03_Março", "04_Abril",
    "05_Maio", "06_Junho", "07_Julho", "08_Agosto",
    "09_Setembro", "10_Outubro", "11_Novembro", "12_Dezembro"
]

def carregar_ultima_edicao(sheet):
    try:
        valor = sheet.acell('H1').value
        return int(valor) if valor and valor.isdigit() else 53
    except Exception as e:
        logger.error(f"Erro ao carregar última edição: {e}")
        return 53

def salvar_ultima_edicao(sheet, n_edicao):
    try:
        sheet.update('H1', str(n_edicao))
        logger.info(f"Última edição atualizada para {n_edicao}")
        return True
    except Exception as e:
        logger.error(f"Falha ao salvar última edição: {e}")
        return False

def processar_pdf(pdf_content, n_edicao, data_edicao):
    editais = []
    try:
        pdf = PyPDF2.PdfReader(pdf_content)
        logger.info(f"PDF processado - {len(pdf.pages)} páginas")

        for pagina_num, pagina in enumerate(pdf.pages, start=1):
            texto = pagina.extract_text() or ""
            texto_normalizado = texto.lower().replace('\n', ' ')
            
            if "edital de chamamento" in texto_normalizado:
                pos = texto_normalizado.find("edital")
                trecho = texto[max(0, pos-100):pos+500]

                editais.append({
                    'data': data_edicao,
                    'edicao': n_edicao,
                    'pagina': pagina_num,
                    'texto': trecho.strip()
                })
                logger.info(f"Edital encontrado na página {pagina_num}")

        return editais
    except Exception as e:
        logger.error(f"Erro ao processar PDF: {e}")
        return []

def salvar_editais(sheet, editais):
    if not editais:
        logger.info("Nenhum edital para salvar")
        return False

    try:
        logger.info(f"Salvando {len(editais)} editais na planilha...")

        # Cabeçalho
        sheet.append_row(["Data", "Edição", "Página", "Texto"])
        logger.info(f"Dados inseridos: ['Data', 'Edição', 'Página', 'Texto']")

        # Montar todas as linhas
        linhas_para_salvar = []
        for edital in editais:
            linhas = edital['texto'].split('\n')
            for linha in linhas:
                logger.info(f"Salvando linha: {linha}")
                linhas_para_salvar.append([edital['data'], edital['edicao'], edital['pagina'], linha])

        # Adicionar em lote
        sheet.append_rows(linhas_para_salvar)
        logger.info(f"{len(linhas_para_salvar)} linhas adicionadas com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar editais: {e}", exc_info=True)
        return False

def get_google_credentials():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    if 'GOOGLE_CREDS_JSON' in os.environ:
        return Credentials.from_service_account_info(
            json.loads(os.environ['GOOGLE_CREDS_JSON']),
            scopes=scope
        )
    elif os.path.exists('../projetodedadosjson'):
        creds = Credentials.from_service_account_file(
            '../projetodedadosjson',
            scopes=scope
        )
        return creds
    else:
        raise Exception("Nenhuma credencial encontrada")

def main():
    try:
        hoje = datetime.today()
        if hoje.weekday() == 5: hoje -= timedelta(days=1)
        elif hoje.weekday() == 6: hoje -= timedelta(days=2)
        
        creds = get_google_credentials()
        client = gspread.authorize(creds)
        sheet = client.open("editais_chamamento_dodf_code").sheet1

        # Teste de escrita (debug de permissão)
        try:
            sheet.append_row(["Teste", "OK", "", ""])
            logger.info("Teste de escrita na planilha: OK")
        except Exception as e:
            logger.error(f"Erro ao escrever na planilha (permissão?): {e}")
            return "Erro: Sem permissão de escrita na planilha"

        sheet.update('J1', [[f"Última execução: {datetime.now()}"]])
        ultima_edicao = carregar_ultima_edicao(sheet)
        n_edicao = ultima_edicao + 1
        data_edicao = f"{hoje.day:02d}-{hoje.month:02d}-{hoje.year}"
        
        logger.info(f"Buscando edição {n_edicao} - {data_edicao}")

        url = f"https://dodf.df.gov.br/dodf/jornal/visualizar-pdf?pasta={quote(f'{hoje.year}|{meses[hoje.month-1]}|DODF {n_edicao:03d} {data_edicao}|')}&arquivo={quote(f'DODF {n_edicao:03d} {data_edicao} INTEGRA.pdf')}"
        
        try:
            logger.info(f"Baixando PDF da URL: {url}")
            with urllib.request.urlopen(url) as response:
                pdf_content = io.BytesIO(response.read())
                editais = processar_pdf(pdf_content, n_edicao, data_edicao)

                if editais:
                    if salvar_editais(sheet, editais):
                        logger.info("Editais salvos corretamente")
                    else:
                        logger.error("Falha ao salvar editais")
                else:
                    logger.info("Nenhum edital encontrado")

                salvar_ultima_edicao(sheet, n_edicao)
                return "Sucesso" + (f" ({len(editais)} editais)" if editais else "")
                
        except urllib.error.HTTPError as e:
            logger.error(f"Edição não encontrada (HTTP {e.code})")
            salvar_ultima_edicao(sheet, n_edicao)
            return "Edição não encontrada"
            
    except Exception as e:
        logger.error(f"Erro crítico: {e}", exc_info=True)
        return f"Erro: {str(e)}"

if __name__ == "__main__":
    print(main())