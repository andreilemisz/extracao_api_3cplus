"""
Este programa acessa a API da discadora 3C+, retira informações de uma campanha específica a respeito de todas as ligações que foram feitas, limpa e filtra os dados, retornando 4 arquivos em .csv com as seguintes informações:
    - Total de chamadas discadas com informações individuais
    - Total de chamadas mas sem repetir os números
    - Apenas os números que retornaram positivo
    - Apenas os números que retornaram negativos
As configurações no cabecalho resolvem quase todas as variáveis, mas não todas. A API itera sobre todas as páginas que existem no retorno dessa API. 
"""

import os
import pandas as pd
import requests
import json
import csv

###########################
# Atenção: 
# Sempre verificar qual o nome dos cabeçalhos para substituir nas configurações abaixo
# Também pode desativar funções específicas no final dependendo se já tiver JSON ou CSV pronto
###########################

""" Variáveis globais. Alterar para funcionar com o relatório emitido pela API """

# Primeiro as variáveis da API
API_Token = "{inserir token aqui}"
API_ID_Campanha = 0
API_Dados = {
    "campaigns": list({API_ID_Campanha}),
    "per_page": 300,
    "simple_paginate": "0"
    }
API_Headers = {
    "Authorization": f'Bearer {API_Token}'
}

# É a ID da primeira linha que vai ser consultada, depois ela aumenta para cada página que for lida
page = 1
# Para evitar repetir o cabeçalho várias vezes durante a passagem pro CSV, assim fica 1 vez só
ja_tem_cabecalho = False

# Caminho onde está o JSON pronto, se for o caso. Não é necessário preencher se for fazer a request da API automaticamente
caminho_json = '-.json' 
# O próximo começa vazio. Vai ser preenchido se fizer a consulta na API ou se fizer o load padrão da execução do programa. Se fizer o load automatico do programa, ele vai puxar o JSON que estiver no caminho da variavel "caminho_json" que está estabelecida acima
dados_em_json = None
# Local onde será criado o arquivo .CSV, que será usado para carregar as funções de filtragem. Se não for fazer a conversão de JSON para CSV e já tiver a tabela pronta, basta alterar o caminho dela aqui
caminho_csv = "-.csv"

# Cria automaticamente um arquivo .csv que vai ser preenchido pela função. Se ele já existir, o programa apaga ele pra não dar problema no futuro
if not os.path.exists(caminho_csv):
    with open(caminho_csv, 'w', newline="", encoding="utf-8"):
        pass
else:
    os.remove(caminho_csv)

# Segundo passo, variáveis da transformação do JSON em CSV
# Para novos valores, precisa adicionar nessa variável e dentro da função arquivo_json_para_csv nos dois momentos perto do fim
valores_a_buscar = ['-', '-']

# Terceiro, variáveis sobre a filtragem dos dados
cabecalho_data = "call_date"
cabecalho_qualificacao = "qualification"
cabecalho_telefone = "number"
nomes_para_substituir_positivos = ['Retornar depois (contato correto)', 'Não aceitou proposta e sabe da antecipação', 'Não aceitou proposta e NÃO sabe da antecipação', 'Aceitou proposta e sabe da antecipação', 'Aceitou proposta e NÃO sabe da antecipação ', 'Já vendeu', 'Falecido']
nomes_para_substituir_negativos = ['Número errado', 'Não qualificada', 'Mudo', 'Discar novamente', 'Contato de parente e NÃO conseguiu o contato certo', 'Contato de parente e conseguiu o contato certo', 'Caixa Postal', 'Sem qualificação', '-']
nome_positivo = "número_bom"
nome_negativo = "número_ruim"
arquivo_relatorio_ultima_ligacao = "-.csv"
arquivo_relatorio_numberos_bons = "-.csv"
arquivo_relatorio_numberos_ruins = "-.csv"

def extracao_dados_api(page, ja_tem_cabecalho): 
    """ Apenas ativar essa função se precisar extrair o JSON direto da API da discadora """

    # A URL tem que ficar aqui dentro para pegar o valor mais atualizado do start
    API_URL = f"https://3c.fluxoti.com/api/v1/calls?page={page}"

    # Impedir o programa de continuar se bater o limite das linhas consultadas da API
    if page is False:
        print("Criação da tabela finalizada.")
    
    else:

        # Comando GET para pegar os dados da API
        resultado_extracao = requests.get(API_URL, headers=API_Headers, json=API_Dados)
        dados_em_json = resultado_extracao.json()

        # Criando o arquivo JSON com o que foi extraído da request
        with open(caminho_json, 'w', encoding='utf-8') as f:
            json.dump(dados_em_json, f, ensure_ascii=False, indent=4)

        print(f"Extrai da API a página {page}")
        verificar_paginacao(page, dados_em_json, ja_tem_cabecalho)

        return dados_em_json

    # O fim dessa função é a extração em JSON da consulta na API da discadora
    # O JSON está salvo na variável "dados_em_json" para ser usado nas próximas etapas

def verificar_paginacao(page, dados_em_json, ja_tem_cabecalho):
    """ Função que busca na variável específica do JSON se ele aponta para a existência de mais uma página além daquela que está sendo exibida """

    busca_paginacao = dados_em_json.get('meta', {}).get("pagination", {})
    if busca_paginacao.get("count", False) == 0:
        print("Acabou a paginação.")
        paginacao_finalizada = True
        arquivo_json_para_csv(page, dados_em_json, paginacao_finalizada, ja_tem_cabecalho)
    else:
        print("Ainda tem paginação. Vou aumentar a consulta para a próxima página.")
        page += 1
        arquivo_json_para_csv(page, dados_em_json, ja_tem_cabecalho)


def arquivo_json_para_csv(page, dados_em_json, ja_tem_cabecalho, paginacao_finalizada=None):
    """ Essa função pega o arquivo JSON no argumento 1 (dados_em_json) e transforma ele em um arquivo .CSV com cabeçalho e os valores específicos das linhas que a gente selecionar nas variáveis gerais e no final da função nos campos específicos """
    
    # A categoria 'data' é uma lista dentro do retorno em JSON e é onde estão os valores que a gente precisa, por isso precisa fazer essa busca primeiro
    dados_na_categoria_correta = dados_em_json.get('data', [])
    with open(caminho_csv, 'a', newline="", encoding="utf-8") as arquivocsv:
        cabecalhos = valores_a_buscar
        funcao_escritor = csv.DictWriter(arquivocsv, fieldnames=cabecalhos)
        if ja_tem_cabecalho == False:
            funcao_escritor.writeheader()
            ja_tem_cabecalho = True

        """ Aqui é preciso escrever cada valor que for puxado para coluna """
        for linha in dados_na_categoria_correta:
            any_value = linha.get('-', None)

            """ Mesma coisa aqui, para cada coluna a ser adicionada tem que adicionar a sintaxe abaixo nos dois lugares """
            if any_value is not None:

                funcao_escritor.writerow({'-': any_value})

    if paginacao_finalizada is True:
        print("Arquivo .csv terminado!")
    else:
        print(f"Vou extrair a API novamente só que agora com a page em {page}")
        extracao_dados_api(page, ja_tem_cabecalho)

    # O fim dessa função é a criação de uma planilha CSV criada com cabeçalhos e os valores que a gente decidiu filtrar

def execucao_filtragem_relatorio(caminho_csv):
    """ Passo a Passo das Funções que Serão Executadas para as Filtragens """

    # Leitura do csv e alteração da coluna data para entender como datetime (já vem certo)
    df = pd.read_csv(caminho_csv)
    df[cabecalho_data] = pd.to_datetime(df[cabecalho_data])

    # Esse relatório não retorna nada para a função, apenas emite a .csv
    relatorio_ultima_ligacao(df)
    # A "substituição_nomes" retorna uma nova df já com os valores novos
    substituicao_nomes(df)
    # Relatórios separados, usando a df criada na função acima
    relatorio_numeros_bons(df)
    relatorio_numeros_ruins(df)
    # Fim dos programa. Os 3 .csv devem estar na pasta geral

def relatorio_ultima_ligacao(df):
    """ Usado para calcular quantos telefones únicos foram alimentados na campanha """

    df_ultima_ligacao = df.loc[df.groupby(cabecalho_telefone)[cabecalho_data].idxmax()]
    df_ultima_ligacao.to_csv(arquivo_relatorio_ultima_ligacao, index=False)

def substituicao_nomes(df):
    """ Troca dos nomes das qualificações ruins e boas por 'numero_bom' e 'numero_ruim'. Precisa ser atualizado sempre que uma campanha muda as regras de preenchimento, ou outro termo é adicionado nas qualificações """

    df[cabecalho_qualificacao] = df[cabecalho_qualificacao].replace(nomes_para_substituir_positivos, nome_positivo)
    df[cabecalho_qualificacao] = df[cabecalho_qualificacao].replace(nomes_para_substituir_negativos, nome_negativo)
    return df

def relatorio_numeros_bons(df):
    """ Emissão do relatório em .csv apenas com os números BONS, sem repetir o mesmo telefone """

    df_positivo = df[df[cabecalho_qualificacao] == nome_positivo]
    df_positivo.to_csv(arquivo_relatorio_numberos_bons, index=False)

def relatorio_numeros_ruins(df):
    """ Emissão do relatório em .csv apenas com os números RUINS, sem repetir o mesmo telefone """

    df_negativos = df[df[cabecalho_qualificacao] == nome_negativo]
    df_negativos = df_negativos.loc[df_negativos.groupby(cabecalho_telefone)[cabecalho_data].idxmax()]
    df_negativos.to_csv(arquivo_relatorio_numberos_ruins, index=False)

###########################
""" Execução do Programa """
""" Programar quais operações são necessárias, na ordem """

if __name__ == "__main__":

    extracao_dados_api(page, ja_tem_cabecalho)
    execucao_filtragem_relatorio(caminho_csv)

# Fim do programa