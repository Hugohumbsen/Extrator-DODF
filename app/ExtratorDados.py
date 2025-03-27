import PyPDF2
import urllib.request
import io
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import quote
import json
import os

# Função para carregar a última edição buscada do Google Sheets
def carregar_ultima_edicao(sheet):
    try:
        # Lê o valor da célula H1
        valor = sheet.acell('H1').value
        if valor and valor.isdigit():  # Verifica se o valor é um número válido
            return int(valor)
        else:
            print(f"Valor inválido na célula H1: '{valor}'. Usando valor padrão 53.")
            return 53  # Valor padrão se a célula estiver vazia ou contiver um valor inválido
    except Exception as e:
        print(f"Erro ao carregar a última edição: {e}")
        return 53  # Valor padrão em caso de erro

# Função para salvar a última edição buscada no Google Sheets
def salvar_ultima_edicao(sheet, n_edicao):
    try:
        # Salva o valor na célula H1 usando argumentos nomeados
        sheet.update(range_name='H1', values=[[str(n_edicao)]])
    except Exception as e:
        print(f"Erro ao salvar a última edição: {e}")

# Função para salvar os dados do edital no Google Sheets
def salvar_no_google_sheets(sheet, edital):
    """
    Salva os dados do edital no Google Sheets.
    - Adiciona uma linha de cabeçalho antes dos dados de cada edital.
    - Quebra o texto em várias células, com cada linha do texto em uma célula diferente.

    :param sheet: Objeto da planilha do Google.
    :param edital: Dicionário contendo 'data', 'edicao', 'pagina' e 'texto'.
    """
    try:
        # Encontra a próxima linha vazia
        proxima_linha = len(sheet.get_all_values()) + 1

        # Adiciona a linha de cabeçalho
        cabecalhos = ["Data", "Edição", "Página", "Texto"]
        sheet.update(range_name=f'A{proxima_linha}:D{proxima_linha}', values=[cabecalhos])

        # Divide o texto em linhas
        linhas_texto = edital["texto"].split("\n")

        # Salva cada linha do texto em uma célula separada na coluna D
        for i, linha in enumerate(linhas_texto):
            # Define os valores a serem salvos
            valores = [
                edital["data"] if i == 0 else "",  # Coluna A (só na primeira linha)
                edital["edicao"] if i == 0 else "",  # Coluna B (só na primeira linha)
                edital["pagina"] if i == 0 else "",  # Coluna C (só na primeira linha)
                linha  # Coluna D (cada linha do texto)
            ]

            # Atualiza a linha correspondente
            sheet.update(range_name=f'A{proxima_linha + 1 + i}:D{proxima_linha + 1 + i}', values=[valores])
    except Exception as e:
        print(f"Erro ao salvar no Google Sheets: {e}")

def get_google_credentials():
    """Obtém as credenciais do Google a partir das variáveis de ambiente"""
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]
    
    # Verifica se está rodando no GitHub Actions (com segredo)
    if 'GOOGLE_CREDENTIALS' in os.environ:
        credentials_json = os.environ['GOOGLE_CREDENTIALS']
        return Credentials.from_service_account_info(json.loads(credentials_json), scopes=scope)
    # Se estiver rodando localmente com arquivo JSON
    elif os.path.exists('projectodedados.json'):
        return Credentials.from_service_account_file('projectodedados.json', scopes=scope)
    else:
        raise Exception("Nenhuma credencial do Google encontrada. Verifique as configurações.")

# Função principal que será chamada pelo Cloud Functions
def diario_oficial_df(request, context):
    try:
        # Obtém as credenciais
        creds = get_google_credentials()
        
        # Autoriza e abre a planilha
        client = gspread.authorize(creds)

        # Abre a planilha pelo nome ou pelo URL
        spreadsheet = client.open("editais_chamamento_dodf_code").sheet1

        # Carrega a última edição buscada
        n_edicao = carregar_ultima_edicao(spreadsheet)

        # Lista de meses
        meses = [
            "01_Janeiro", "02_Fevereiro", "03_Março", "04_Abril",
            "05_Maio", "06_Junho", "07_Julho", "08_Agosto",
            "09_Setembro", "10_Outubro", "11_Novembro", "12_Dezembro"
        ]

        # Pegando a data atual separada
        hoje = datetime.today()

        # Se for sábado (5), retrocede 1 dia. Se for domingo (6), retrocede 2 dias.
        if hoje.weekday() == 5:  # Sábado
            hoje -= timedelta(days=1)
        elif hoje.weekday() == 6:  # Domingo
            hoje -= timedelta(days=2)
            
        dia = hoje.day
        mes = hoje.month
        ano = hoje.year

        print(f"Rodando para a data: {dia}/{mes}/{ano}")

        # Define a edição atual como a última edição buscada + 1
        n_edicao += 1

        # Lógica para definir o nome da pasta do mês
        mes_pasta = meses[mes - 1]

        lista_editais = []

        print(f"Buscando edição {n_edicao}")

        # Formatar o número da edição com 3 dígitos (ex: 027)
        edicao_formatada = f"{n_edicao:03d}"    

        # Formatar a data corretamente (ex: 07-02-2025)
        data_edicao = f"{dia:02d}-{mes:02d}-{ano}"

        # Codificar o URL corretamente para lidar com caracteres especiais
        pasta_codificada = quote(f"{ano}|{mes_pasta}|DODF {edicao_formatada} {data_edicao}|")
        arquivo_codificado = quote(f"DODF {edicao_formatada} {data_edicao} INTEGRA.pdf")
        link = f"https://dodf.df.gov.br/dodf/jornal/visualizar-pdf?pasta={pasta_codificada}&arquivo={arquivo_codificado}"

        print(f"Acessando: {link}\n")

        try:
            with urllib.request.urlopen(link) as response:
                conteudo_pdf = response.read()
            
            pdf_content = io.BytesIO(conteudo_pdf)
            leitor_pdf = PyPDF2.PdfReader(pdf_content)

            # Variável para verificar se a frase foi encontrada
            frase_encontrada = False

            for numero_pagina, pagina in enumerate(leitor_pdf.pages, start=1):
                texto_pagina = pagina.extract_text()
                if texto_pagina and "edital de chamamento" in texto_pagina.lower():
                    posicao_edital = texto_pagina.lower().find("edital")
                    trecho_texto = texto_pagina[posicao_edital:posicao_edital + 1000]

                    print(f"Trecho encontrado na página {numero_pagina}:\n{trecho_texto}\n")

                    edital = {
                        'data': data_edicao,
                        'edicao': n_edicao,
                        'pagina': numero_pagina,
                        'texto': trecho_texto
                    }

                    lista_editais.append(edital)
                    frase_encontrada = True
                    break  # Sai do loop de páginas

            if not frase_encontrada:
                print(f"Nenhuma menção a 'edital de chamamento' encontrada na edição {n_edicao}.")

        except urllib.error.HTTPError as e:
            if e.code == 404:  # Se o link não existir (erro 404)
                print(f"Edição {n_edicao} não encontrada.")
            else:
                print(f"Erro ao acessar a edição {n_edicao}: {e}.")
        except Exception as e:
            print(f"Erro ao acessar a edição {n_edicao}: {e}.")

        # Salva a última edição buscada no Google Sheets
        salvar_ultima_edicao(spreadsheet, n_edicao)

        # Adiciona os dados ao Google Sheets
        for edital in lista_editais:
            salvar_no_google_sheets(spreadsheet, edital)

        # Mostra a última edição encontrada
        if lista_editais:
            ultima_edicao = lista_editais[-1]['edicao']
            print(f"A última edição encontrada foi: {ultima_edicao}")
        else:
            print("Nenhuma edição válida foi encontrada.")

        return "Dados atualizados com sucesso!" if lista_editais else "Nenhuma edição encontrada."

    except Exception as e:
        print(f"Erro crítico na execução do script: {e}")
        return f"Erro na execução: {str(e)}"

# Teste local
if __name__ == "__main__":
    print("Iniciando o script...")
    diario_oficial_df(None, None)