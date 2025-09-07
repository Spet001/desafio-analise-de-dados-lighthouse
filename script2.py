import pandas as pd


try:
    transacoes_df = pd.read_csv('csv/transacoes.csv')
    contas_df = pd.read_csv('csv/contas.csv')
    agencias_df = pd.read_csv('csv/agencias.csv')
    print("Arquivos carregados com sucesso.")
except FileNotFoundError as e:
    print(f"Erro ao carregar arquivos: {e}. Verifique se todos os CSVs estão presentes.")
    exit()


transacoes_df['data_transacao'] = pd.to_datetime(transacoes_df['data_transacao'], errors='coerce')
transacoes_df.dropna(subset=['data_transacao'], inplace=True)

trans_contas_df = pd.merge(transacoes_df, contas_df, on='num_conta', how='left')


df_completo = pd.merge(trans_contas_df, agencias_df, on='cod_agencia', how='left')

df_completo.dropna(subset=['nome'], inplace=True)

print("Junção de dados concluída. Cada transação está associada a uma agência.")


data_mais_recente = df_completo['data_transacao'].max()
data_inicio_periodo = data_mais_recente - pd.DateOffset(months=6)

print(f"\nAnalisando o período de 6 meses de {data_inicio_periodo.strftime('%Y-%m-%d')} a {data_mais_recente.strftime('%Y-%m-%d')}")


df_ult_6_meses = df_completo[df_completo['data_transacao'] >= data_inicio_periodo]


ranking_agencias = df_ult_6_meses.groupby('nome')['cod_transacao'].count().reset_index()
ranking_agencias = ranking_agencias.rename(columns={'nome': 'agencia', 'cod_transacao': 'numero_de_transacoes'})


ranking_agencias = ranking_agencias.sort_values(by='numero_de_transacoes', ascending=False)

print("\n--- Ranking Completo de Agências (Últimos 6 Meses) ---")
print(ranking_agencias.to_string(index=False))



top_3_agencias = ranking_agencias.head(3)
bottom_3_agencias = ranking_agencias.tail(3)

print("\n--- Desempenho das Agências ---")
print("\n--- 3 Agências com MAIOR número de transações ---")
print(top_3_agencias.to_string(index=False))

print("\n--- 3 Agências com MENOR número de transações ---")
print(bottom_3_agencias.sort_values(by='numero_de_transacoes', ascending=True).to_string(index=False))