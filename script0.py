import pandas as pd



try:
    agencias_df = pd.read_csv('csv/agencias.csv')
    clientes_df = pd.read_csv('csv/clientes.csv')
    colaboradores_df = pd.read_csv('csv/colaboradores.csv')
    contas_df = pd.read_csv('csv/contas.csv')
    propostas_credito_df = pd.read_csv('csv/propostas_credito.csv')
    transacoes_df = pd.read_csv('csv/transacoes.csv')
    print("Arquivos CSV carregados com sucesso!")
except FileNotFoundError as e:
    print(f"Erro: Arquivo não encontrado. Verifique se todos os CSVs estão no lugar correto. Detalhe: {e}")
    exit()



print("\n--- Convertendo colunas de data de Object para Datetime ---")


dataframes_to_convert = {
    'agencias_df': ['data_abertura'],
    'clientes_df': ['data_inclusao', 'data_nascimento'],
    'colaboradores_df': ['data_nascimento'],
    'contas_df': ['data_abertura', 'data_ultimo_lancamento'],
    'propostas_credito_df': ['data_entrada_proposta'],
    'transacoes_df': ['data_transacao']
}


dfs_map = {
    'agencias_df': agencias_df,
    'clientes_df': clientes_df,
    'colaboradores_df': colaboradores_df,
    'contas_df': contas_df,
    'propostas_credito_df': propostas_credito_df,
    'transacoes_df': transacoes_df
}


for df_name, columns in dataframes_to_convert.items():
    print(f"\nProcessando DataFrame: {df_name}")
    current_df = dfs_map[df_name]
    for col in columns:
        if col in current_df.columns:
            current_df[col] = pd.to_datetime(current_df[col], errors='coerce')
            print(f"  - Coluna '{col}' convertida.")
        else:
            print(f"  - ATENÇÃO: Coluna '{col}' não encontrada.")


print("\n--- Verificando os tipos de dados após a conversão ---")

print("\n--- Info para agencias_df ---")
agencias_df.info()

print("\n--- Info para clientes_df ---")
clientes_df.info()

print("\n--- Info para colaboradores_df ---")
colaboradores_df.info()

print("\n--- Info para contas_df ---")
contas_df.info()

print("\n--- Info para propostas_credito_df ---")
propostas_credito_df.info()

print("\n--- Info para transacoes_df ---")
transacoes_df.info()

print("\nConversão de datas concluída com sucesso!")