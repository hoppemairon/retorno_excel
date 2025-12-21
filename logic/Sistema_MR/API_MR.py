import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

api_url = os.getenv("API_MR_URL")
chave_api = os.getenv("API_MR_KEY")

def buscar_categorias_api() -> pd.DataFrame:
    """
    Busca todas as categorias disponíveis na API MR.
    
    Returns:
        DataFrame com categorias únicas disponíveis no sistema
    """
    headers = {
        "Content-Type": "application/json",
        "mr-key": chave_api
    }
    
    try:
        # Buscar algumas empresas para extrair categorias
        ids_empresas_sample = [
            "772644ba-3a49-4736-8443-f057581d6b39",  # ARARANGUA
            "4d49850f-ebf1-433d-a32a-527b54e856aa",  # TERRA DE AREIA
            "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401"   # CAMINHO DO SOL
        ]
        
        todas_categorias = set()
        
        for id_empresa in ids_empresas_sample:
            url = f"{api_url}/api/export/lancamentos/{id_empresa}?anos=2024,2025"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                try:
                    json_response = response.json()
                    if isinstance(json_response, dict) and "result" in json_response:
                        dados = json_response["result"]
                        if isinstance(dados, list):
                            df_temp = pd.json_normalize(dados)
                            if 'categoria' in df_temp.columns:
                                categorias_empresa = df_temp['categoria'].dropna().unique()
                                todas_categorias.update(categorias_empresa)
                except Exception as e:
                    print(f"❌ Erro ao processar categorias da empresa {id_empresa}: {e}")
        
        # Converter para DataFrame
        categorias_list = sorted(list(todas_categorias))
        df_categorias = pd.DataFrame({
            'categoria_completa': categorias_list,
            'categoria_nome': [cat.split('-', 1)[-1].strip() if '-' in cat else cat for cat in categorias_list]
        })
        
        return df_categorias
        
    except Exception as e:
        print(f"❌ Erro ao buscar categorias: {e}")
        return pd.DataFrame()

def validar_transacoes_robusta(df_transacoes: pd.DataFrame, df_mr: pd.DataFrame, tolerancia_dias: int = 7, tolerancia_valor: float = 0.01) -> pd.DataFrame:
    """
    Validação robusta de transações contra o sistema MR.
    
    Args:
        df_transacoes: DataFrame com transações a validar
        df_mr: DataFrame com dados do MR
        tolerancia_dias: Tolerância em dias para comparação de datas
        tolerancia_valor: Tolerância percentual para comparação de valores
        
    Returns:
        DataFrame com resultados da validação
    """
    import unidecode
    from datetime import datetime, timedelta
    
    # Normalizar dados do MR
    df_mr_norm = df_mr.copy()
    df_mr_norm['data'] = pd.to_datetime(df_mr_norm['data'], errors='coerce').dt.date
    df_mr_norm['valor'] = pd.to_numeric(df_mr_norm['valor'], errors='coerce')
    df_mr_norm['contato_norm'] = df_mr_norm['contato'].apply(
        lambda x: unidecode.unidecode(str(x)).lower().strip() if pd.notna(x) else ""
    )
    
    # Normalizar transações
    df_trans_norm = df_transacoes.copy()
    df_trans_norm['data_norm'] = pd.to_datetime(df_trans_norm['Data Pagamento'], dayfirst=True, errors='coerce').dt.date
    df_trans_norm['valor_norm'] = df_trans_norm['Valor Pago (R$)'].str.replace(".", "", regex=False).str.replace(",", ".").astype(float)
    df_trans_norm['favorecido_norm'] = df_trans_norm['Favorecido'].apply(
        lambda x: unidecode.unidecode(str(x)).lower().strip() if pd.notna(x) else ""
    )
    
    resultados = []
    
    for idx, transacao in df_trans_norm.iterrows():
        melhor_match = None
        melhor_score = 0
        tipo_match = "NOVO"
        
        # 1. Busca por nome exato
        matches_nome = df_mr_norm[df_mr_norm['contato_norm'].str.contains(transacao['favorecido_norm'], na=False)]
        
        if not matches_nome.empty:
            for _, mr_row in matches_nome.iterrows():
                score = 0
                
                # Score por nome (base)
                score += 50
                
                # Score por valor (±tolerancia)
                if abs(mr_row['valor'] - transacao['valor_norm']) <= (transacao['valor_norm'] * tolerancia_valor):
                    score += 30
                
                # Score por data (±tolerancia_dias)
                if pd.notna(mr_row['data']) and pd.notna(transacao['data_norm']):
                    diff_dias = abs((mr_row['data'] - transacao['data_norm']).days)
                    if diff_dias <= tolerancia_dias:
                        score += 20 - (diff_dias * 2)  # Quanto mais próximo, maior o score
                
                if score > melhor_score:
                    melhor_score = score
                    melhor_match = mr_row
        
        # Determinar resultado baseado no score
        if melhor_score >= 70:  # Nome + (Valor OU Data muito próximos)
            tipo_match = "JÁ EXISTE"
        elif melhor_score >= 50:  # Apenas nome
            tipo_match = "POSSÍVEL DUPLICATA"
        else:
            tipo_match = "NOVO"
        
        resultado = {
            'Status': f"{'✅' if tipo_match == 'JÁ EXISTE' else '⚠️' if tipo_match == 'POSSÍVEL DUPLICATA' else '🆕'} {tipo_match}",
            'Score_Match': melhor_score,
            'Arquivo': transacao.get('Arquivo', ''),
            'Data': transacao['data_norm'].strftime('%d/%m/%Y') if pd.notna(transacao['data_norm']) else '',
            'Favorecido': transacao['Favorecido'],
            'Valor': transacao['valor_norm'],
            'Contato_MR': melhor_match['contato'] if melhor_match is not None else '',
            'Categoria_MR': melhor_match.get('categoria', '').split('-', 1)[-1].strip() if melhor_match is not None and melhor_match.get('categoria') else '',
            'Data_MR': melhor_match['data'].strftime('%d/%m/%Y') if melhor_match is not None and pd.notna(melhor_match['data']) else '',
            'Valor_MR': melhor_match['valor'] if melhor_match is not None else '',
            'Acao': 'Não importar' if tipo_match == 'JÁ EXISTE' else 'Revisar' if tipo_match == 'POSSÍVEL DUPLICATA' else 'Importar no sistema'
        }
        
        resultados.append(resultado)
    
    return pd.DataFrame(resultados)

def buscar_lancamentos_api(ids_empresa: str, anos: str = "2025") -> pd.DataFrame:
    headers = {
        "Content-Type": "application/json",
        "mr-key": chave_api
    }

    todos_dados = []

    for id_empresa in ids_empresa.split(","):
        url = f"{api_url}/api/export/lancamentos/{id_empresa}?anos={anos}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            try:
                json_response = response.json()
                if isinstance(json_response, dict) and "result" in json_response:
                    dados = json_response["result"]
                    if isinstance(dados, list):
                        df_parcial = pd.json_normalize(dados)
                        todos_dados.append(df_parcial)
                else:
                    print(f"⚠️ Estrutura inesperada: {json_response}")
            except Exception as e:
                print(f"❌ Erro ao processar JSON da empresa {id_empresa}: {e}")
        else:
            print(f"❌ Erro HTTP {response.status_code} para empresa {id_empresa}: {response.text}")

    return pd.concat(todos_dados, ignore_index=True) if todos_dados else pd.DataFrame()

def buscar_transferencias_api(ids_empresa: str, anos: str = "2025") -> pd.DataFrame:
    """
    Busca transferências da API MR para as empresas especificadas.
    
    Args:
        ids_empresa: String com IDs das empresas separados por vírgula
        anos: Anos para buscar (padrão: 2025)
        
    Returns:
        DataFrame com transferências encontradas
    """
    headers = {
        "Content-Type": "application/json",
        "mr-key": chave_api
    }

    todos_dados = []

    for id_empresa in ids_empresa.split(","):
        url = f"{api_url}/api/export/transferencias/{id_empresa}?anos={anos}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            try:
                json_response = response.json()
                if isinstance(json_response, dict) and "result" in json_response:
                    dados = json_response["result"]
                    if isinstance(dados, list) and len(dados) > 0:
                        df_parcial = pd.json_normalize(dados)
                        # Adicionar campo para identificar que são transferências
                        df_parcial['tipo_registro'] = 'transferencia'
                        df_parcial['empresa_id'] = id_empresa
                        todos_dados.append(df_parcial)
                    else:
                        print(f"ℹ️ Nenhuma transferência encontrada para empresa {id_empresa}")
                else:
                    print(f"⚠️ Estrutura inesperada em transferências: {json_response}")
            except Exception as e:
                print(f"❌ Erro ao processar JSON de transferências da empresa {id_empresa}: {e}")
        else:
            print(f"❌ Erro HTTP {response.status_code} para transferências da empresa {id_empresa}: {response.text}")

    return pd.concat(todos_dados, ignore_index=True) if todos_dados else pd.DataFrame()

def buscar_lancamentos_e_transferencias_api(ids_empresa: str, anos: str = "2025") -> dict:
    """
    Busca tanto lançamentos quanto transferências da API MR.
    
    Args:
        ids_empresa: String com IDs das empresas separados por vírgula
        anos: Anos para buscar (padrão: 2025)
        
    Returns:
        dict: {'lancamentos': DataFrame, 'transferencias': DataFrame, 'combinado': DataFrame}
    """
    print(f"🔍 Buscando lançamentos e transferências para {len(ids_empresa.split(','))} empresas...")
    
    # Buscar lançamentos
    df_lancamentos = buscar_lancamentos_api(ids_empresa, anos)
    if not df_lancamentos.empty:
        df_lancamentos['tipo_registro'] = 'lancamento'
        print(f"✅ {len(df_lancamentos)} lançamentos encontrados")
    
    # Buscar transferências
    df_transferencias = buscar_transferencias_api(ids_empresa, anos)
    if not df_transferencias.empty:
        print(f"✅ {len(df_transferencias)} transferências encontradas")
    
    # Combinar os dados para verificação de duplicatas
    dados_combinados = []
    
    # Processar lançamentos
    if not df_lancamentos.empty:
        dados_combinados.append(df_lancamentos)
    
    # Processar transferências
    if not df_transferencias.empty:
        dados_combinados.append(df_transferencias)
    
    # Criar DataFrame combinado
    df_combinado = pd.concat(dados_combinados, ignore_index=True) if dados_combinados else pd.DataFrame()
    
    return {
        'lancamentos': df_lancamentos,
        'transferencias': df_transferencias,
        'combinado': df_combinado,
        'total_registros': len(df_combinado)
    }

def validar_transacoes_contra_api(df_ofx: pd.DataFrame, ids_empresa: list, anos: str = "2025") -> dict:
    """
    Valida transações OFX contra os lançamentos existentes na API MR.
    
    Args:
        df_ofx: DataFrame com transações OFX processadas
        ids_empresa: Lista de IDs das empresas para buscar na API
        anos: Anos para buscar na API (padrão: 2025)
        
    Returns:
        dict: Resultado da validação com estatísticas e DataFrames
    """
    try:
        # Buscar lançamentos da API para as empresas
        print(f"🔍 Buscando lançamentos da API para {len(ids_empresa)} empresas...")
        ids_str = ",".join(ids_empresa)
        df_api = buscar_lancamentos_api(ids_str, anos)
        
        if df_api.empty:
            return {
                'sucesso': False,
                'erro': 'Nenhum lançamento encontrado na API',
                'df_api': pd.DataFrame(),
                'df_comparacao': pd.DataFrame(),
                'estatisticas': {}
            }
        
        print(f"✅ {len(df_api)} lançamentos encontrados na API")
        
        # Preparar dados para comparação
        df_ofx_prep = df_ofx.copy()
        df_api_prep = df_api.copy()
        
        # Padronizar colunas de data
        if 'data' in df_ofx_prep.columns:
            df_ofx_prep['data_comparacao'] = pd.to_datetime(df_ofx_prep['data']).dt.date
        
        if 'data' in df_api_prep.columns:
            df_api_prep['data_comparacao'] = pd.to_datetime(df_api_prep['data'], errors='coerce').dt.date
        elif 'dataLancamento' in df_api_prep.columns:
            df_api_prep['data_comparacao'] = pd.to_datetime(df_api_prep['dataLancamento'], errors='coerce').dt.date
        
        # Padronizar valores
        if 'valor_absoluto' in df_ofx_prep.columns:
            df_ofx_prep['valor_comparacao'] = pd.to_numeric(df_ofx_prep['valor_absoluto'], errors='coerce')
        
        if 'valor' in df_api_prep.columns:
            df_api_prep['valor_comparacao'] = pd.to_numeric(df_api_prep['valor'], errors='coerce').abs()
        elif 'valorLancamento' in df_api_prep.columns:
            df_api_prep['valor_comparacao'] = pd.to_numeric(df_api_prep['valorLancamento'], errors='coerce').abs()
        
        # Criar chaves de comparação
        df_ofx_prep['chave_comparacao'] = (
            df_ofx_prep['data_comparacao'].astype(str) + '_' + 
            df_ofx_prep['valor_comparacao'].round(2).astype(str)
        )
        
        df_api_prep['chave_comparacao'] = (
            df_api_prep['data_comparacao'].astype(str) + '_' + 
            df_api_prep['valor_comparacao'].round(2).astype(str)
        )
        
        # Identificar transações já existentes e novas
        chaves_api = set(df_api_prep['chave_comparacao'].dropna())
        chaves_ofx = set(df_ofx_prep['chave_comparacao'].dropna())
        
        # Classificar transações OFX
        df_ofx_prep['status_api'] = df_ofx_prep['chave_comparacao'].apply(
            lambda x: 'EXISTE_NA_API' if x in chaves_api else 'NOVA_TRANSACAO'
        )
        
        # Estatísticas
        total_ofx = len(df_ofx_prep)
        existentes = len(df_ofx_prep[df_ofx_prep['status_api'] == 'EXISTE_NA_API'])
        novas = len(df_ofx_prep[df_ofx_prep['status_api'] == 'NOVA_TRANSACAO'])
        
        estatisticas = {
            'total_transacoes_ofx': total_ofx,
            'total_lancamentos_api': len(df_api_prep),
            'transacoes_existentes': existentes,
            'transacoes_novas': novas,
            'percentual_existentes': (existentes / total_ofx * 100) if total_ofx > 0 else 0,
            'percentual_novas': (novas / total_ofx * 100) if total_ofx > 0 else 0
        }
        
        print(f"📊 Validação concluída: {existentes} existentes, {novas} novas")
        
        return {
            'sucesso': True,
            'df_ofx_validado': df_ofx_prep,
            'df_api': df_api_prep,
            'estatisticas': estatisticas,
            'chaves_comparacao': {
                'api': chaves_api,
                'ofx': chaves_ofx,
                'intersecao': chaves_api.intersection(chaves_ofx),
                'somente_ofx': chaves_ofx - chaves_api,
                'somente_api': chaves_api - chaves_ofx
            }
        }
        
    except Exception as e:
        return {
            'sucesso': False,
            'erro': f'Erro na validação: {str(e)}',
            'df_api': pd.DataFrame(),
            'df_comparacao': pd.DataFrame(),
            'estatisticas': {}
        }

def buscar_nomes_empresas(ids_empresa: list) -> dict:
    """
    Busca os nomes das empresas via API MR.
    Se não conseguir via API, usa mapeamento conhecido.
    
    Args:
        ids_empresa (list): Lista de IDs das empresas
        
    Returns:
        dict: Mapeamento ID -> Nome da empresa
    """
    # Mapeamento conhecido das empresas do Grupo Rota
    empresas_conhecidas = {
        "772644ba-3a49-4736-8443-f057581d6b39": "GRUPO ROTA - ARARANGUÁ",
        "4d49850f-ebf1-433d-a32a-527b54e856aa": "GRUPO ROTA - TERRA DE AREIA",
        "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401": "GRUPO ROTA - CAMINHO DO SOL",
        "149653c2-f107-4c60-aad0-b034789c8504": "GRUPO ROTA - JAGUARUNA",
        "735b6b4e-5513-4bb5-a9c4-50d92462921d": "GRUPO ROTA - PARADOURO",
        "1db3be97-a6d6-484a-b75b-fc1bdc6c487a": "GRUPO ROTA - SÃO PAULO",
        "93f44c44-bfd4-417f-bad2-20933e5c0228": "GRUPO ROTA - ELDORADO",
        "a13229ca-0f8a-442a-91ab-27e0adc1810b": "GRUPO ROTA - PINHEIRO MACHADO",
        "eb84222f-2e6b-4f68-8457-760d10e24043": "GRUPO ROTA - SEBERI",
        "85d3091d-af31-4cb5-86fc-1558aaefa19b": "GRUPO ROTA - POA IPIRANGA",
        "7a078786-1d9e-4433-9d63-8dfc58130b5f": "GRUPO ROTA - CRISTAL",
        "73a32cc3-d7ac-48d7-91d7-9046045d0bd7": "GRUPO ROTA - PORTO ALEGRE",
        "b2107e4f-59a7-44a0-9a84-d81abaab5ad2": "GRUPO ROTA - CANDIOTA",
        "cad79622-124a-4dc0-9408-7da5227576f0": "GRUPO ROTA - PARADOURO REST.",
        "3885ddf8-f0ac-4468-98ab-97a248e29150": "GRUPO ROTA - TRANSPORTADORA"
    }
    
    headers = {
        "Content-Type": "application/json",
        "mr-key": chave_api
    }
    
    nomes_empresas = {}
    
    for id_empresa in ids_empresa:
        # Primeiro, verificar se já conhecemos esta empresa
        if id_empresa in empresas_conhecidas:
            nomes_empresas[id_empresa] = empresas_conhecidas[id_empresa]
            continue
            
        # Se não conhecemos, tentar buscar via API
        try:
            url = f"{api_url}/api/export/lancamentos/{id_empresa}?anos=2025&limit=1"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                json_response = response.json()
                if isinstance(json_response, dict) and "result" in json_response:
                    dados = json_response["result"]
                    if isinstance(dados, list) and len(dados) > 0:
                        # Tentar extrair nome da empresa do primeiro lançamento
                        primeiro_lancamento = dados[0]
                        nome_empresa = (
                            primeiro_lancamento.get('nomeEmpresa') or
                            primeiro_lancamento.get('empresa') or
                            primeiro_lancamento.get('razaoSocial') or
                            f"Empresa {id_empresa[:8]}..."
                        )
                        nomes_empresas[id_empresa] = nome_empresa
                    else:
                        # Fallback para empresa não conhecida
                        nomes_empresas[id_empresa] = f"Empresa {id_empresa[:8]}..."
                else:
                    nomes_empresas[id_empresa] = f"Empresa {id_empresa[:8]}..."
            else:
                nomes_empresas[id_empresa] = f"Empresa {id_empresa[:8]}..."
                
        except Exception as e:
            print(f"⚠️ Erro ao processar empresa {id_empresa}: {str(e)}")
            nomes_empresas[id_empresa] = f"Empresa {id_empresa[:8]}..."
    
    return nomes_empresas