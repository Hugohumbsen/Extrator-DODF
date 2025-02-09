import PyPDF2
import urllib.request
import io
import csv
import pandas as pd
from datetime import datetime, timedelta

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

n_edicao = 27
barrinha = "|"

# Lógica para definir o nome da pasta do mês

mes_pasta = meses[mes - 1]


lista_editais = []

while True:
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

    except Exception as e:
        print(f"Erro ao acessar a edição {n_edicao}. Pode ser que ela não exista. Parando...\n")
        break  

    n_edicao += 1  # Próxima edição

# Salvando os dados em CSV
csv_filename = f'editais_{mes_pasta}.csv'
with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    head = ['data', 'edicao', 'pagina', 'texto']
    writer = csv.DictWriter(csvfile, fieldnames=head)
    writer.writeheader()

    for edital in lista_editais:
        writer.writerow(edital)

# Lendo o CSV com Pandas
editais = pd.read_csv(csv_filename)
print(editais)
