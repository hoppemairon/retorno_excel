"""
API Contas Correntes MR
Módulo para buscar informações de contas correntes do sistema MR.
Baseado na conexão Power BI fornecida.
"""

import requests
import pandas as pd
import os
from typing import Dict, List, Optional, Any
import logging
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class ContasCorrentesMR:
    """
    Classe para integração com API de contas correntes do sistema MR.
    Baseada na conexão Power BI: /api/export/contas-corrente/{id}
    """
    
    def __init__(self):
        self.api_url = os.getenv("API_MR_URL")
        self.api_key = os.getenv("API_MR_KEY")
        self.base_endpoint = "/api/export/contas-corrente"
        
        # Cache para evitar múltiplas chamadas
        self._cache_contas = {}
        
    def buscar_contas_correntes(self, cliente_id: str) -> pd.DataFrame:
        """
        Busca contas correntes de um cliente específico.
        
        Args:
            cliente_id (str): ID do cliente no sistema MR
            
        Returns:
            DataFrame com as contas correntes do cliente
        """
        if not self.api_url or not self.api_key:
            logger.error("URL da API ou chave não configurada")
            return pd.DataFrame()
        
        # Verificar cache
        if cliente_id in self._cache_contas:
            logger.info(f"Retornando contas do cache para cliente {cliente_id}")
            return self._cache_contas[cliente_id]
        
        try:
            # Preparar headers conforme Power BI
            headers = {
                "Content-Type": "application/json",
                "mr-key": self.api_key
            }
            
            # Construir URL completa
            url = f"{self.api_url}{self.base_endpoint}/{cliente_id}"
            
            logger.info(f"Buscando contas correntes para cliente {cliente_id}")
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Processar resposta conforme estrutura Power BI
                if isinstance(data, dict) and "result" in data:
                    contas_data = data["result"]
                    
                    if isinstance(contas_data, list) and contas_data:
                        # Converter para DataFrame
                        df_contas = pd.json_normalize(contas_data)
                        
                        # Renomear colunas conforme Power BI
                        column_mapping = {
                            'nome': 'nome',
                            'tipo': 'tipo',
                            'dataInicial': 'dataInicial',
                            'valorInicial': 'valorInicial',
                            'saldoAtual': 'saldoAtual',
                            'dataEncerramento': 'dataEncerramento',
                            'clienteId': 'clienteId',
                            'contaId': 'contaId'
                        }
                        
                        # Garantir que todas as colunas existam
                        for new_col, old_col in column_mapping.items():
                            if old_col not in df_contas.columns:
                                df_contas[new_col] = None
                            else:
                                df_contas[new_col] = df_contas[old_col]
                        
                        # Selecionar apenas as colunas mapeadas
                        df_contas = df_contas[list(column_mapping.keys())]
                        
                        # Aplicar tipos conforme Power BI
                        df_contas = self._aplicar_tipos_dados(df_contas)
                        
                        # Salvar no cache
                        self._cache_contas[cliente_id] = df_contas
                        
                        logger.info(f"✅ {len(df_contas)} contas encontradas para cliente {cliente_id}")
                        return df_contas
                    
                    else:
                        logger.warning(f"Nenhuma conta encontrada para cliente {cliente_id}")
                        return pd.DataFrame()
                
                else:
                    logger.error(f"Estrutura de resposta inesperada: {data}")
                    return pd.DataFrame()
            
            else:
                logger.error(f"Erro HTTP {response.status_code}: {response.text}")
                return pd.DataFrame()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de requisição para cliente {cliente_id}: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro inesperado para cliente {cliente_id}: {str(e)}")
            return pd.DataFrame()
    
    def buscar_contas_multiplos_clientes(self, clientes_ids: List[str]) -> pd.DataFrame:
        """
        Busca contas correntes para múltiplos clientes.
        
        Args:
            clientes_ids (List[str]): Lista de IDs de clientes
            
        Returns:
            DataFrame consolidado com contas de todos os clientes
        """
        all_contas = []
        
        for cliente_id in clientes_ids:
            df_cliente = self.buscar_contas_correntes(cliente_id)
            if not df_cliente.empty:
                all_contas.append(df_cliente)
        
        if all_contas:
            df_final = pd.concat(all_contas, ignore_index=True)
            logger.info(f"✅ Total de {len(df_final)} contas encontradas para {len(clientes_ids)} clientes")
            return df_final
        else:
            logger.warning("Nenhuma conta encontrada para os clientes informados")
            return pd.DataFrame()
    
    def _aplicar_tipos_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica tipos de dados conforme definido no Power BI.
        """
        try:
            # Tipos conforme Power BI
            type_mapping = {
                'nome': 'object',  # text
                'tipo': 'int64',   # Int64.Type
                'dataInicial': 'datetime64[ns]',  # date
                'valorInicial': 'float64',  # number
                'saldoAtual': 'float64',    # number
                'dataEncerramento': 'datetime64[ns]',  # date
                'clienteId': 'object',      # text
                'contaId': 'object'         # text
            }
            
            for column, dtype in type_mapping.items():
                if column in df.columns:
                    if dtype == 'datetime64[ns]':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif dtype == 'float64':
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif dtype == 'int64':
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    else:
                        df[column] = df[column].astype(dtype, errors='ignore')
            
            return df
            
        except Exception as e:
            logger.warning(f"Erro ao aplicar tipos de dados: {str(e)}")
            return df
    
    def obter_mapeamento_bancos_ofx(self, clientes_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Cria mapeamento entre códigos OFX e contas do sistema MR.
        
        Args:
            clientes_ids (List[str]): Lista de IDs de clientes
            
        Returns:
            Dict com mapeamento código_banco_ofx -> dados_conta_mr
        """
        df_contas = self.buscar_contas_multiplos_clientes(clientes_ids)
        
        if df_contas.empty:
            return {}
        
        mapeamento = {}
        
        # Lógica para extrair código do banco do nome da conta
        # Isso pode precisar ser ajustado conforme padrão dos nomes
        for _, conta in df_contas.iterrows():
            nome_conta = str(conta.get('nome', '')).upper()
            
            # Tentar identificar banco pelo nome
            codigo_banco = self._extrair_codigo_banco_do_nome(nome_conta)
            
            if codigo_banco:
                mapeamento[codigo_banco] = {
                    'conta_id': conta.get('contaId'),
                    'cliente_id': conta.get('clienteId'),
                    'nome_conta': conta.get('nome'),
                    'tipo_conta': conta.get('tipo'),
                    'saldo_atual': conta.get('saldoAtual'),
                    'data_inicial': conta.get('dataInicial'),
                    'ativo': pd.isna(conta.get('dataEncerramento'))  # Conta ativa se não tem data encerramento
                }
        
        logger.info(f"Mapeamento criado para {len(mapeamento)} bancos")
        return mapeamento
    
    def _extrair_codigo_banco_do_nome(self, nome_conta: str) -> Optional[str]:
        """
        Extrai código do banco do nome da conta.
        Lógica básica - pode ser refinada conforme padrão real dos nomes.
        """
        # Mapeamentos comuns baseados em nomes
        bank_patterns = {
            'BANCO DO BRASIL': '001',
            'BB': '001',
            'BRADESCO': '237',
            'ITAU': '0341',
            'ITAÚ': '341',
            'SANTANDER': '033',
            'CAIXA': '104',
            'CEF': '104',
            'INTER': '077',
            'NUBANK': '260',
            'NU': '260',
            'SICOOB': '756',
            'BANRISUL': '041'
        }
        
        nome_upper = nome_conta.upper()
        
        for bank_name, code in bank_patterns.items():
            if bank_name in nome_upper:
                return code
        
        # Tentar extrair código numérico se estiver no formato "001 - NOME"
        if ' - ' in nome_conta:
            possivel_codigo = nome_conta.split(' - ')[0].strip()
            if possivel_codigo.isdigit() and len(possivel_codigo) == 3:
                return possivel_codigo
        
        return None
    
    def validar_conexao(self) -> bool:
        """
        Valida se a conexão com a API está funcionando.
        
        Returns:
            True se conexão OK, False caso contrário
        """
        if not self.api_url or not self.api_key:
            logger.error("Configuração da API incompleta")
            return False
        
        try:
            # Fazer uma chamada de teste
            headers = {
                "Content-Type": "application/json",
                "mr-key": self.api_key
            }
            
            # Tentar endpoint base ou de saúde se existir
            test_url = f"{self.api_url}/v1"
            response = requests.get(test_url, headers=headers, timeout=10)
            
            if response.status_code in [200, 404]:  # 404 pode ser normal para endpoint base
                logger.info("✅ Conexão com API MR validada")
                return True
            else:
                logger.error(f"Falha na validação da conexão: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao validar conexão: {str(e)}")
            return False
    
    def limpar_cache(self):
        """Limpa o cache de contas correntes."""
        self._cache_contas.clear()
        logger.info("Cache de contas correntes limpo")
    
    def get_estatisticas_cache(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do cache atual.
        
        Returns:
            Dict com estatísticas
        """
        total_clientes = len(self._cache_contas)
        total_contas = sum(len(df) for df in self._cache_contas.values())
        
        return {
            'clientes_em_cache': total_clientes,
            'total_contas_cache': total_contas,
            'clientes_ids': list(self._cache_contas.keys())
        }