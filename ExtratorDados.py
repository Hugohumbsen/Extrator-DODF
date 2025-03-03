import PyPDF2
import urllib.request
import io
import csv
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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

n_edicao = 32
barrinha = "|"

# Lógica para definir o nome da pasta do mês
mes_pasta = meses[mes - 1]

lista_editais = []
edicao_encontrada = False  # Flag para parar após encontrar um edital

while not edicao_encontrada:
    print(f"Buscando edição {n_edicao}")

    # Formatar o número da edição com 3 dígitos (ex: 027)
    edicao_formatada = f"{n_edicao:03d}"    

    # Formatar a data corretamente (ex: 07-02-2025)
    data_edicao = f"{dia:02d}-{mes:02d}-{ano}"

    # Corrigir o link
    link = f"https://dodf.df.gov.br/dodf/jornal/visualizar-pdf?pasta={ano}|{mes_pasta}|DODF%20{edicao_formatada}%20{data_edicao}|&arquivo=DODF%20{edicao_formatada}%20{data_edicao}%20INTEGRA.pdf"

    print(f"Acessando: {link}\n")

    try:
        with urllib.request.urlopen(link) as response:
            conteudo_pdf = response.read()
        
        pdf_content = io.BytesIO(conteudo_pdf)
        leitor_pdf = PyPDF2.PdfReader(pdf_content)

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
                edicao_encontrada = True  # Para o loop assim que encontrar um edital
                break  # Sai do loop de páginas

    except Exception as e:
        print(f"Erro ao acessar a edição {n_edicao}. Tentando próxima...\n")

    n_edicao += 1  # Incrementa a edição para a próxima tentativa

# Salvando os dados em CSV
csv_filename = f'editais_{mes_pasta}.csv'
with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    head = ['data', 'edicao', 'pagina', 'texto']
    writer = csv.DictWriter(csvfile, fieldnames=head)
    writer.writeheader()

    for edital in lista_editais:
        writer.writerow(edital)


# Função para salvar no Google Sheets
def salvar_no_google_sheets(sheet, edital):
    """
    Salva os dados do edital no Google Sheets.
    
    :param sheet: Objeto da planilha do Google.
    :param edital: Dicionário contendo 'data', 'edicao', 'pagina' e 'texto'.
    """
    # Divide o texto do edital em linhas menores
    linhas_texto = edital["texto"].split("\n")

    # Adiciona os dados principais na primeira linha de forma explícita
    sheet.append_row(["Data", "Edição", "Página", "Texto"])  # Coloca os cabeçalhos primeiro
    sheet.append_row([edital["data"], edital["edicao"], edital["pagina"], linhas_texto[0]])

    # Insere o restante do texto abaixo da linha principal, com as 3 primeiras colunas vazias
    for linha in linhas_texto[1:]:
        sheet.append_row(["", "", "", linha])  # Deixa as 3 primeiras colunas vazias para alinhar o texto


# Lendo o CSV com Pandas
editais = pd.read_csv(csv_filename)
print(editais)

# Define o escopo de permissões
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive.file",
         "https://www.googleapis.com/auth/drive"]

# Carrega as credenciais do arquivo JSON
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/Hugo/Documents/DadoJson/projetodedados.json', scope)

# Autoriza e abre a planilha
client = gspread.authorize(creds)

# Abre a planilha pelo nome ou pelo URL
spreadsheet = client.open("editais_chamamento_dodf_code").sheet1

# Adiciona os dados ao Google Sheets
for edital in lista_editais:
    salvar_no_google_sheets(spreadsheet, edital)
