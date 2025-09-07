import pandas as pd
import os


output_folder = 'dashboard_data'

# Criar output
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    print(f"Pasta '{output_folder}' criada com sucesso.")


print("\n[SEÇÃO 1] Carregando e preparando os dados...")

try:
    agencias_df = pd.read_csv('csv/agencias.csv')
    contas_df = pd.read_csv('csv/contas.csv')
    transacoes_df = pd.read_csv('csv/transacoes.csv')
    clientes_df = pd.read_csv('csv/clientes.csv')

    transacoes_df['data_transacao'] = pd.to_datetime(transacoes_df['data_transacao'], errors='coerce')
    transacoes_df.dropna(subset=['data_transacao'], inplace=True)

    print("Dados carregados e colunas de data processadas.")

except FileNotFoundError as e:
    print(f"ERRO: Arquivo não encontrado -> {e}.")
    exit()


print("\n[SEÇÃO 2] Gerando CSVs para o Item 3: Desempenho por Período...")

dias_map = {
    0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira',
    3: 'Quinta-feira', 4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'
}
transacoes_df['dia_da_semana'] = transacoes_df['data_transacao'].dt.weekday.map(dias_map)
day_of_week_analysis = transacoes_df.groupby('dia_da_semana').agg(
    numero_transacoes=('cod_transacao', 'count'),
    volume_movimentado=('valor_transacao', lambda x: x.abs().sum())
).reset_index()
dias_ordenados = list(dias_map.values())
day_of_week_analysis['dia_da_semana'] = pd.Categorical(day_of_week_analysis['dia_da_semana'], categories=dias_ordenados, ordered=True)
day_of_week_analysis = day_of_week_analysis.sort_values('dia_da_semana')

# Exportar 
output_path = os.path.join(output_folder, 'analise_dia_da_semana.csv')
day_of_week_analysis.to_csv(output_path, index=False, decimal=',', sep=';')
print(f"  - Salvo em: {output_path}")


transacoes_df['tipo_mes'] = transacoes_df['data_transacao'].dt.month.apply(lambda m: 'Par' if m % 2 == 0 else 'Ímpar')
month_type_analysis = transacoes_df.groupby('tipo_mes')['valor_transacao'].mean().reset_index()

# Exportar
output_path = os.path.join(output_folder, 'analise_tipo_mes.csv')
month_type_analysis.to_csv(output_path, index=False, decimal=',', sep=';')
print(f"  - Salvo em: {output_path}")

print("\n[SEÇÃO 3] Gerando CSV para o Item 5: Desempenho das Agências...")

trans_contas_df = pd.merge(transacoes_df, contas_df, on='num_conta', how='left')
df_completo_ag = pd.merge(trans_contas_df, agencias_df, on='cod_agencia', how='left')
df_completo_ag.dropna(subset=['nome'], inplace=True)

data_mais_recente = df_completo_ag['data_transacao'].max()
data_inicio_periodo = data_mais_recente - pd.DateOffset(months=6)
df_ult_6_meses = df_completo_ag[df_completo_ag['data_transacao'] >= data_inicio_periodo]

ranking_agencias = df_ult_6_meses.groupby('nome')['cod_transacao'].count().reset_index()
ranking_agencias = ranking_agencias.rename(columns={'nome': 'agencia', 'cod_transacao': 'numero_de_transacoes'})
ranking_agencias = ranking_agencias.sort_values(by='numero_de_transacoes', ascending=False)

# Exportar
output_path = os.path.join(output_folder, 'ranking_agencias.csv')
ranking_agencias.to_csv(output_path, index=False, decimal=',', sep=';')
print(f"  - Salvo em: {output_path}")


# Exportar CSV principal
print("\n[SEÇÃO 4] Gerando CSV principal enriquecido para o Dashboard...")

df_dashboard = pd.merge(trans_contas_df, agencias_df[['cod_agencia', 'nome', 'cidade', 'uf']], on='cod_agencia', how='left')
df_dashboard = pd.merge(df_dashboard, clientes_df[['cod_cliente', 'tipo_cliente']], on='cod_cliente', how='left')


df_dashboard = df_dashboard[[
    'cod_transacao', 'data_transacao', 'nome_transacao', 'valor_transacao',
    'num_conta', 'cod_cliente', 'tipo_cliente',
    'cod_agencia', 'nome', 'cidade', 'uf'
]]
df_dashboard.rename(columns={'nome': 'nome_agencia'}, inplace=True)

# Exportar
output_path = os.path.join(output_folder, 'transacoes_enriquecidas.csv')
df_dashboard.to_csv(output_path, index=False, decimal=',', sep=';', date_format='%Y-%m-%d %H:%M:%S')
print(f"  - Salvo em: {output_path}")

print("\n--- GERAÇÃO DE ARQUIVOS CONCLUÍDA ---")
print(f"Todos os arquivos foram salvos na pasta '{output_folder}'.")
