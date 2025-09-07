import pandas as pd
import locale

try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
except locale.Error:
    print("Locale 'pt_BR.utf8' não encontrado. Usando o locale padrão.")



try:
    transacoes_df = pd.read_csv('csv/transacoes.csv')


    transacoes_df['data_transacao'] = pd.to_datetime(transacoes_df['data_transacao'], errors='coerce')

    transacoes_df.dropna(subset=['data_transacao'], inplace=True)

    print("Arquivo de transações carregado e preparado.")

except FileNotFoundError:
    print("Erro: 'transacoes.csv' não encontrado. Por favor, verifique o arquivo.")
    exit()


print("\n--- Análise: Média de Transações e Volume por Dia da Semana ---")

dias_map = {
    0: 'Segunda-feira',
    1: 'Terça-feira',
    2: 'Quarta-feira',
    3: 'Quinta-feira',
    4: 'Sexta-feira',
    5: 'Sábado',
    6: 'Domingo'
}


transacoes_df['dia_da_semana'] = transacoes_df['data_transacao'].dt.strftime('%A').str.capitalize()

day_of_week_analysis = transacoes_df.groupby('dia_da_semana').agg(
    numero_transacoes=('cod_transacao', 'count'),
    volume_movimentado=('valor_transacao', lambda x: x.abs().sum())
).reset_index()


dias_ordenados = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
day_of_week_analysis['dia_da_semana'] = pd.Categorical(day_of_week_analysis['dia_da_semana'], categories=dias_ordenados, ordered=True)
day_of_week_analysis = day_of_week_analysis.sort_values('dia_da_semana')

print("Resultados da análise por dia da semana:")
print(day_of_week_analysis.to_string(index=False))


maior_n_transacoes = day_of_week_analysis.loc[day_of_week_analysis['numero_transacoes'].idxmax()]
maior_volume = day_of_week_analysis.loc[day_of_week_analysis['volume_movimentado'].idxmax()]

print(f"\n> Dia com maior número de transações: {maior_n_transacoes['dia_da_semana']} ({maior_n_transacoes['numero_transacoes']} transações)")
print(f"> Dia com maior volume movimentado: {maior_volume['dia_da_semana']} (R$ {maior_volume['volume_movimentado']:,.2f})")

print("\n--- Análise: Volume Médio de Transações em Meses Pares vs. Ímpares ---")


transacoes_df['tipo_mes'] = transacoes_df['data_transacao'].dt.month.apply(lambda m: 'Par' if m % 2 == 0 else 'Ímpar')


month_type_analysis = transacoes_df.groupby('tipo_mes')['valor_transacao'].mean().reset_index()
month_type_analysis = month_type_analysis.rename(columns={'valor_transacao': 'volume_medio_transacao'})


print("\nResultados da análise por tipo de mês:")
print(month_type_analysis.to_string(index=False))


volume_par = month_type_analysis[month_type_analysis['tipo_mes'] == 'Par']['volume_medio_transacao'].iloc[0]
volume_impar = month_type_analysis[month_type_analysis['tipo_mes'] == 'Ímpar']['volume_medio_transacao'].iloc[0]

print("\n> Conclusão sobre a hipótese do analista:")
if volume_par > volume_impar:
    print("A afirmação do analista é valida. O volume médio de transações nos meses pares é maior do que nos meses ímpares.")
else:
    print("A afirmação do analista não é valida. O volume médio de transações nos meses ímpares é maior ou igual ao dos meses pares.")