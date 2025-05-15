import pandas as pd
import sys
from datetime import datetime

# ======================================
# 1. CARREGAMENTO DE DADOS
# ======================================
def carregar_dados():
    """Carrega todos os dados das planilhas Excel"""
    file_path = 'Case_Infomaz_Base_de_Dados.xlsx'
    
    try:
        dfs = {
            'produtos': pd.read_excel(
                file_path, 
                sheet_name='Cadastro Produtos', 
                skiprows=1,
                usecols=['ID PRODUTO', 'ID ESTOQUE', 'NOME PRODUTO', 'CATEGORIA']
            ),
            'vendas': pd.read_excel(
                file_path,
                sheet_name='Transações Vendas',
                header=1,  # Cabeçalho na linha 3
                usecols=['ID NOTA', 'DATA NOTA', 'VALOR NOTA', 'VALOR ITEM', 'QTD ITEM', 'ID PRODUTO', 'ID CLIENTE']
            ),
            'estoque': pd.read_excel(
                file_path,
                sheet_name='Cadastro de Estoque',
                skiprows=1,
                usecols=['ID ESTOQUE', 'VALOR ESTOQUE', 'QTD ESTOQUE', 'DATA ESTOQUE', 'ID FORNECEDOR']
            ),
            'clientes': pd.read_excel(
                file_path,
                sheet_name='Cadastro Clientes',
                skiprows=1,
                usecols=['ID CLIENTE', 'NOME CLIENTE', 'DATA CADASTRO']
            ),
            'fornecedores': pd.read_excel(
                file_path,
                sheet_name='Cadastro Fornecedores',
                skiprows=1,
                usecols=['ID FORNECEDOR', 'NOME FORNECEDOR', 'DATA CADASTRO']
            )
        }
        
        # Debug: verificar colunas carregadas
        print("\nColunas carregadas com sucesso em cada planilha:")
        for nome, df in dfs.items():
            print(f"{nome.upper()}: {list(df.columns)}")
            
        return dfs
        
    except Exception as e:
        print(f"ERRO AO LER ARQUIVO EXCEL: {str(e)}")
        sys.exit(1)

# ======================================
# 2. PRÉ-PROCESSAMENTO
# ======================================
def preprocessar_dados(dfs):
    """Prepara os dados para análise"""
    try:
        # Converter datas
        dfs['vendas']['DATA NOTA'] = pd.to_datetime(dfs['vendas']['DATA NOTA'])
        dfs['estoque']['DATA ESTOQUE'] = pd.to_datetime(dfs['estoque']['DATA ESTOQUE'])
        
        # Criar coluna Mês/Ano
        dfs['vendas']['Mês/Ano'] = dfs['vendas']['DATA NOTA'].dt.to_period('M')
        dfs['estoque']['Mês/Ano'] = dfs['estoque']['DATA ESTOQUE'].dt.to_period('M')
        
        return dfs
        
    except Exception as e:
        print(f"ERRO NO PRÉ-PROCESSAMENTO: {str(e)}")
        sys.exit(1)

# ======================================
# 3. CÁLCULO DAS MÉTRICAS
# ======================================
def calcular_metricas(dfs):
    """Calcula todas as métricas requeridas"""
    try:
        metricas = {}
        
        # Métrica 1: Valor total por categoria
        merged_vendas = pd.merge(dfs['produtos'], dfs['vendas'], on='ID PRODUTO', how='left')
        metricas['m1_total_categoria'] = merged_vendas.groupby('CATEGORIA')['VALOR ITEM'].sum().reset_index()
        
        # Métrica 2: Margem por produto
        # Primeiro obtemos o valor médio por produto das vendas
        valor_por_produto = dfs['vendas'].groupby('ID PRODUTO')['VALOR ITEM'].mean().reset_index()
        
        # Merge produtos com estoque (usando ID_ESTOQUE)
        merged_prod_estoque = pd.merge(dfs['produtos'], dfs['estoque'], on='ID ESTOQUE', how='left')
        
        # Agora merge com os valores médios
        merged_estoque = pd.merge(merged_prod_estoque, valor_por_produto, on='ID PRODUTO', how='left')
        
        # Calculamos a margem
        merged_estoque['Custo Unitário'] = merged_estoque['VALOR ESTOQUE'] / merged_estoque['QTD ESTOQUE']
        merged_estoque['Margem'] = merged_estoque['VALOR ITEM'] - merged_estoque['Custo Unitário']
        metricas['m2_margem'] = merged_estoque[['NOME PRODUTO', 'Margem']].dropna()
        
        # Métrica 3: Ranking clientes (mantido igual)
        metricas['m3_ranking_clientes'] = (dfs['vendas']
            .groupby(['ID CLIENTE', 'Mês/Ano'])['QTD ITEM']
            .sum()
            .reset_index()
            .sort_values(['Mês/Ano', 'QTD ITEM'], ascending=[True, False]))
        
        # Métrica 4: Ranking fornecedores (ajustado)
        # Primeiro merge estoque com produtos para obter ID_PRODUTO
        merged_fornecedores = pd.merge(dfs['estoque'], dfs['produtos'], on='ID ESTOQUE', how='left')
        metricas['m4_ranking_fornecedores'] = (merged_fornecedores
            .groupby(['ID FORNECEDOR', 'Mês/Ano'])['QTD ESTOQUE']
            .sum()
            .reset_index()
            .sort_values(['Mês/Ano', 'QTD ESTOQUE'], ascending=[True, False]))
        
        # Métrica 5: Produtos mais vendidos (mantido igual)
        metricas['m5_produtos_vendidos'] = (dfs['vendas']
            .groupby(['ID PRODUTO', 'Mês/Ano'])['QTD ITEM']
            .sum()
            .reset_index()
            .sort_values(['Mês/Ano', 'QTD ITEM'], ascending=[True, False]))
        
        # Métrica 6: Produtos por valor (mantido igual)
        dfs['vendas']['Valor Total'] = dfs['vendas']['QTD ITEM'] * dfs['vendas']['VALOR ITEM']
        metricas['m6_produtos_valor'] = (dfs['vendas']
            .groupby(['ID PRODUTO', 'Mês/Ano'])['Valor Total']
            .sum()
            .reset_index()
            .sort_values(['Mês/Ano', 'Valor Total'], ascending=[True, False]))
        
        # Métrica 7: Média por categoria (mantido igual)
        metricas['m7_media_categoria'] = (merged_vendas
            .groupby(['CATEGORIA', 'Mês/Ano'])['VALOR ITEM']
            .mean()
            .reset_index())
        
        # Métrica 8: Margem por categoria (usando merged_estoque corrigido)
        metricas['m8_margem_categoria'] = (merged_estoque
            .groupby('CATEGORIA')['Margem']
            .sum()
            .reset_index()
            .sort_values('Margem', ascending=False))
        
        # Métrica 9: Produtos por cliente (mantido igual)
        metricas['m9_produtos_cliente'] = (pd.merge(dfs['vendas'], dfs['produtos'], on='ID PRODUTO')
            [['ID CLIENTE', 'NOME PRODUTO']]
            .drop_duplicates()
            .sort_values('ID CLIENTE'))
        
        # Métrica 10: Ranking estoque (ajustado)
        # Merge estoque com produtos para obter ID_PRODUTO
        merged_estoque_prod = pd.merge(dfs['estoque'], dfs['produtos'], on='ID ESTOQUE', how='left')
        metricas['m10_ranking_estoque'] = (merged_estoque_prod
            .groupby('ID PRODUTO')['QTD ESTOQUE']
            .sum()
            .reset_index()
            .sort_values('QTD ESTOQUE', ascending=False))
        
        return metricas
        
    except Exception as e:
        print(f"ERRO NO CÁLCULO DAS MÉTRICAS: {str(e)}")
        print("Dados disponíveis:")
        print("Vendas:", dfs['vendas'].columns.tolist())
        print("Produtos:", dfs['produtos'].columns.tolist())
        print("Estoque:", dfs['estoque'].columns.tolist())
        sys.exit(1)

# ======================================
# 4. EXPORTAÇÃO DOS RESULTADOS
# ======================================
def exportar_resultados(metricas):
    """Exporta os resultados para Excel"""
    try:
        with pd.ExcelWriter('Resultados_Infomaz.xlsx') as writer:
            metricas['m1_total_categoria'].to_excel(writer, sheet_name='Vendas_Categoria', index=False)
            metricas['m2_margem'].to_excel(writer, sheet_name='Margem_Produtos', index=False)
            metricas['m3_ranking_clientes'].to_excel(writer, sheet_name='Ranking_Clientes', index=False)
            metricas['m4_ranking_fornecedores'].to_excel(writer, sheet_name='Ranking_Fornecedores', index=False)
            metricas['m5_produtos_vendidos'].to_excel(writer, sheet_name='Produtos_Vendidos', index=False)
            metricas['m6_produtos_valor'].to_excel(writer, sheet_name='Produtos_Valor', index=False)
            metricas['m7_media_categoria'].to_excel(writer, sheet_name='Media_Categoria', index=False)
            metricas['m8_margem_categoria'].to_excel(writer, sheet_name='Margem_Categoria', index=False)
            metricas['m9_produtos_cliente'].to_excel(writer, sheet_name='Produtos_Cliente', index=False)
            metricas['m10_ranking_estoque'].to_excel(writer, sheet_name='Estoque_Produtos', index=False)
            
        print("\nArquivo 'Resultados_Infomaz.xlsx' gerado com sucesso!")
        
    except Exception as e:
        print(f"ERRO NA EXPORTAÇÃO: {str(e)}")
        sys.exit(1)

# ======================================
# EXECUÇÃO PRINCIPAL
# ======================================
if __name__ == "__main__":
    print("Iniciando processamento...")

    # 1. Carregar dados
    print("\nCarregando dados...")
    dados = carregar_dados()
    
    # 2. Pré-processamento
    print("Preparando dados para análise...")
    dados = preprocessar_dados(dados)
    
    # 3. Cálculo das métricas
    print("Calculando métricas...")
    resultados = calcular_metricas(dados)
    
    # 4. Exportação
    print("Exportando resultados...")
    exportar_resultados(resultados)
    
    print("\nProcesso concluído com sucesso!")