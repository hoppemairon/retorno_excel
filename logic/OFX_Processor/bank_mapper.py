"""
Bank Mapper Module
Responsável pelo mapeamento De/Para de bancos entre OFX e sistema interno.
"""

import pandas as pd
from typing import Dict, List, Optional, Any
import logging
import os
from dotenv import load_dotenv

# Importar API de contas correntes
from ..Sistema_MR.contas_correntes_api import ContasCorrentesMR
from .manual_bank_mapper import ManualBankMapper

load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BankMapper:
    """
    Classe responsável pelo mapeamento de bancos entre arquivos OFX e sistema interno.
    Futuramente será integrada com API da MR para buscar mapeamentos dinâmicos.
    """
    
    def __init__(self):
        # Mapeamento estático inicial - será substituído pela API
        self.static_mapping = {
            # Código do banco OFX : Dados do sistema interno
            '001': {
                'codigo_sistema': 'BB001',
                'nome_sistema': 'BANCO DO BRASIL S.A.',
                'nome_ofx': 'Banco do Brasil',
                'ativo': True
            },
            '237': {
                'codigo_sistema': 'BRAD237',
                'nome_sistema': 'BANCO BRADESCO S.A.',
                'nome_ofx': 'Bradesco',
                'ativo': True
            },
            '341': {
                'codigo_sistema': 'ITAU341',
                'nome_sistema': 'BANCO ITAU UNIBANCO S.A.',
                'nome_ofx': 'Itaú Unibanco',
                'ativo': True
            },
            '033': {
                'codigo_sistema': 'SANT033',
                'nome_sistema': 'BANCO SANTANDER BRASIL S.A.',
                'nome_ofx': 'Santander',
                'ativo': True
            },
            '104': {
                'codigo_sistema': 'CEF104',
                'nome_sistema': 'CAIXA ECONOMICA FEDERAL',
                'nome_ofx': 'Caixa Econômica Federal',
                'ativo': True
            },
            '077': {
                'codigo_sistema': 'INTER077',
                'nome_sistema': 'BANCO INTER S.A.',
                'nome_ofx': 'Banco Inter',
                'ativo': True
            },
            '260': {
                'codigo_sistema': 'NUBANK260',
                'nome_sistema': 'NU PAGAMENTOS S.A.',
                'nome_ofx': 'Nubank',
                'ativo': True
            },
            '756': {
                'codigo_sistema': 'SICOOB756',
                'nome_sistema': 'BANCO COOPERATIVO SICOOB S.A.',
                'nome_ofx': 'Sicoob',
                'ativo': True
            },
            '041': {
                'codigo_sistema': 'BANRISUL041',
                'nome_sistema': 'BANCO DO ESTADO DO RIO GRANDE DO SUL S.A.',
                'nome_ofx': 'Banrisul',
                'ativo': True
            }
        }
        
        self.api_mapping = {}  # Será preenchido pela API da MR
        self.use_api = False   # Flag para usar API quando disponível
        
        # Instanciar API de contas correntes
        self.contas_api = ContasCorrentesMR()
        
        # Instanciar mapeamento manual
        self.manual_mapper = ManualBankMapper()
    
    def map_bank_ofx_to_system(self, ofx_bank_code: str) -> Optional[Dict[str, Any]]:
        """
        Mapeia código de banco do OFX para dados do sistema interno.
        Ordem de prioridade: Manual → API → Estático
        
        Args:
            ofx_bank_code (str): Código do banco no arquivo OFX
            
        Returns:
            Dict com informações do banco no sistema ou None se não encontrado
        """
        # Normalizar código do banco
        clean_code = self._normalize_bank_code(ofx_bank_code)
        
        # 1. Primeira prioridade: Mapeamento manual
        manual_mapping = self.manual_mapper.get_mapping(clean_code)
        if manual_mapping and manual_mapping.get('ativo', True):
            return {
                'codigo_sistema': manual_mapping['codigo_sistema'],
                'nome_sistema': manual_mapping['nome_banco'],
                'nome_ofx': manual_mapping['nome_banco'],
                'ativo': manual_mapping['ativo'],
                'fonte': 'manual'
            }
        
        # 2. Segunda prioridade: Mapeamento da API
        if self.use_api and clean_code in self.api_mapping:
            api_data = self.api_mapping[clean_code]
            api_data['fonte'] = 'api'
            return api_data
        
        # 3. Terceira prioridade: Mapeamento estático (fallback)
        if clean_code in self.static_mapping:
            static_data = self.static_mapping[clean_code]
            static_data['fonte'] = 'estatico'
            return static_data
        
        logger.warning(f"Banco não mapeado: {ofx_bank_code} (normalizado: {clean_code})")
        return None
    
    def _normalize_bank_code(self, bank_code: str) -> str:
        """
        Normaliza código do banco para formato padrão.
        
        Args:
            bank_code (str): Código original do banco
            
        Returns:
            Código normalizado (3 dígitos)
        """
        if not bank_code:
            return ""
        
        # Converter para string e remover espaços
        code_str = str(bank_code).strip()
        
        # Casos especiais conhecidos
        special_cases = {
            '0341': '341',  # Itaú com zero à esquerda
            '0001': '001',  # Banco do Brasil com zero à esquerda
            '0237': '237',  # Bradesco com zero à esquerda
            '0033': '033',  # Santander com zero à esquerda
            '0104': '104',  # Caixa com zero à esquerda
            '0077': '077',  # Inter com zero à esquerda
            '0260': '260',  # Nubank com zero à esquerda
            '0756': '756',  # Sicoob com zero à esquerda
            '0041': '041',  # Banrisul com zero à esquerda
        }
        
        # Verificar casos especiais primeiro
        if code_str in special_cases:
            return special_cases[code_str]
        
        # Para códigos de 4 dígitos que começam com 0, remover o 0
        if len(code_str) == 4 and code_str.startswith('0'):
            return code_str[1:]
        
        # Para códigos menores que 3 dígitos, preencher com zeros à esquerda
        if len(code_str) < 3:
            return code_str.zfill(3)
        
        # Para códigos de 3 dígitos ou mais, pegar apenas os 3 primeiros
        return code_str[:3]
    
    def map_bank_system_to_ofx(self, system_bank_code: str) -> Optional[str]:
        """
        Mapeia código do sistema interno para código OFX.
        
        Args:
            system_bank_code (str): Código do banco no sistema interno
            
        Returns:
            Código do banco no formato OFX ou None se não encontrado
        """
        # Buscar no mapeamento da API
        if self.use_api:
            for ofx_code, data in self.api_mapping.items():
                if data.get('codigo_sistema') == system_bank_code:
                    return ofx_code
        
        # Buscar no mapeamento estático
        for ofx_code, data in self.static_mapping.items():
            if data.get('codigo_sistema') == system_bank_code:
                return ofx_code
        
        logger.warning(f"Código do sistema não mapeado: {system_bank_code}")
        return None
    
    def get_all_supported_banks(self) -> List[Dict[str, Any]]:
        """
        Retorna lista de todos os bancos suportados.
        
        Returns:
            Lista de dicts com informações dos bancos
        """
        banks = []
        
        # Adicionar bancos da API
        if self.use_api:
            for code, data in self.api_mapping.items():
                if data.get('ativo', True):
                    banks.append({
                        'codigo_ofx': code,
                        'codigo_sistema': data.get('codigo_sistema'),
                        'nome_sistema': data.get('nome_sistema'),
                        'nome_ofx': data.get('nome_ofx'),
                        'fonte': 'API'
                    })
        
        # Adicionar bancos estáticos (que não estão na API)
        for code, data in self.static_mapping.items():
            if data.get('ativo', True):
                # Verificar se já não está na API
                if not self.use_api or code not in self.api_mapping:
                    banks.append({
                        'codigo_ofx': code,
                        'codigo_sistema': data.get('codigo_sistema'),
                        'nome_sistema': data.get('nome_sistema'),
                        'nome_ofx': data.get('nome_ofx'),
                        'fonte': 'Estático'
                    })
        
        return banks
    
    def load_mapping_from_api(self, clientes_ids: List[str]) -> bool:
        """
        Carrega mapeamento de bancos da API de contas correntes da MR.
        Baseado na conexão Power BI fornecida.
        
        Args:
            clientes_ids (List[str]): Lista de IDs de clientes para buscar contas
            
        Returns:
            True se carregou com sucesso, False caso contrário
        """
        try:
            logger.info("Carregando mapeamento de bancos da API de contas correntes...")
            
            # Validar conexão primeiro
            if not self.contas_api.validar_conexao():
                logger.warning("Falha na validação da conexão com API - usando mapeamento estático")
                self.use_api = False
                return False
            
            # Buscar mapeamento de bancos com tratamento robusto
            try:
                api_mapping = self.contas_api.obter_mapeamento_bancos_ofx(clientes_ids)
            except Exception as api_error:
                logger.warning(f"Erro específico da API: {str(api_error)} - continuando com mapeamento estático")
                self.use_api = False
                return False
            
            if api_mapping and len(api_mapping) > 0:
                # Converter para formato compatível
                self.api_mapping = {}
                converted_count = 0
                
                for codigo_ofx, dados_conta in api_mapping.items():
                    try:
                        # Normalizar código antes de armazenar
                        normalized_code = self._normalize_bank_code(codigo_ofx)
                        
                        self.api_mapping[normalized_code] = {
                            'codigo_sistema': dados_conta.get('conta_id', ''),
                            'nome_sistema': dados_conta.get('nome_conta', 'Nome não informado'),
                            'nome_ofx': self._get_bank_name_from_code(normalized_code),
                            'ativo': dados_conta.get('ativo', True),
                            'cliente_id': dados_conta.get('cliente_id', ''),
                            'tipo_conta': dados_conta.get('tipo_conta', ''),
                            'saldo_atual': dados_conta.get('saldo_atual', 0.0)
                        }
                        converted_count += 1
                    except Exception as mapping_error:
                        logger.warning(f"Erro ao converter mapeamento para banco {codigo_ofx}: {str(mapping_error)}")
                        continue
                
                if converted_count > 0:
                    self.use_api = True
                    logger.info(f"✅ Mapeamento da API carregado: {converted_count} bancos de {len(api_mapping)} encontrados")
                    return True
                else:
                    logger.warning("Nenhum mapeamento válido encontrado - usando mapeamento estático")
                    self.use_api = False
                    return False
            else:
                logger.warning("Nenhum mapeamento retornado da API - usando mapeamento estático")
                self.use_api = False
                return False
                
        except Exception as e:
            logger.warning(f"Erro geral ao carregar mapeamento da API: {str(e)} - continuando com mapeamento estático")
            self.use_api = False
            return False
    
    def _get_bank_name_from_code(self, bank_code: str) -> str:
        """
        Obtém nome amigável do banco a partir do código.
        """
        bank_names = {
            '001': 'Banco do Brasil',
            '237': 'Bradesco',
            '341': 'Itaú Unibanco',
            '033': 'Santander',
            '104': 'Caixa Econômica Federal',
            '077': 'Banco Inter',
            '260': 'Nubank',
            '756': 'Sicoob',
            '041': 'Banrisul'
        }
        return bank_names.get(bank_code, f'Banco {bank_code}')
    
    def validate_bank_mapping(self, df_transactions: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida se todos os bancos nas transações estão mapeados.
        
        Args:
            df_transactions (pd.DataFrame): DataFrame com transações
            
        Returns:
            Dict com resultado da validação
        """
        if df_transactions.empty or 'conta_banco' not in df_transactions.columns:
            return {
                'total_bancos': 0,
                'bancos_mapeados': 0,
                'bancos_nao_mapeados': 0,
                'detalhes': []
            }
        
        # Obter bancos únicos
        unique_banks = df_transactions['conta_banco'].unique()
        
        mapped_banks = []
        unmapped_banks = []
        
        for bank_code in unique_banks:
            if bank_code and str(bank_code).strip():
                mapping = self.map_bank_ofx_to_system(str(bank_code))
                if mapping:
                    mapped_banks.append({
                        'codigo_ofx': bank_code,
                        'nome_sistema': mapping.get('nome_sistema'),
                        'codigo_sistema': mapping.get('codigo_sistema')
                    })
                else:
                    unmapped_banks.append({
                        'codigo_ofx': bank_code,
                        'transacoes_afetadas': len(df_transactions[df_transactions['conta_banco'] == bank_code])
                    })
        
        return {
            'total_bancos': len(unique_banks),
            'bancos_mapeados': len(mapped_banks),
            'bancos_nao_mapeados': len(unmapped_banks),
            'detalhes_mapeados': mapped_banks,
            'detalhes_nao_mapeados': unmapped_banks
        }
    
    def apply_bank_mapping_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica mapeamento de bancos a um DataFrame de transações.
        
        Args:
            df (pd.DataFrame): DataFrame com transações
            
        Returns:
            DataFrame com colunas de mapeamento adicionadas
        """
        if df.empty:
            return df
        
        df_mapped = df.copy()
        
        # Adicionar colunas de mapeamento
        df_mapped['banco_codigo_sistema'] = ''
        df_mapped['banco_nome_sistema'] = ''
        df_mapped['banco_mapeado'] = False
        
        # Aplicar mapeamento para cada linha
        for idx, row in df_mapped.iterrows():
            bank_code = str(row.get('conta_banco', '')).strip()
            if bank_code:
                mapping = self.map_bank_ofx_to_system(bank_code)
                if mapping:
                    df_mapped.at[idx, 'banco_codigo_sistema'] = mapping.get('codigo_sistema', '')
                    df_mapped.at[idx, 'banco_nome_sistema'] = mapping.get('nome_sistema', '')
                    df_mapped.at[idx, 'banco_mapeado'] = True
        
        return df_mapped
    
    def get_mapping_status(self) -> Dict[str, Any]:
        """
        Retorna status atual do mapeamento para diagnóstico.
        
        Returns:
            Dict com informações sobre o estado do mapeamento
        """
        manual_mappings = self.manual_mapper.get_all_mappings()
        
        status = {
            'using_api': self.use_api,
            'manual_banks_count': len(manual_mappings),
            'api_banks_count': len(self.api_mapping) if self.api_mapping else 0,
            'static_banks_count': len(self.static_mapping),
            'total_available_banks': len(manual_mappings) + len(self.api_mapping) + len(self.static_mapping) if self.use_api else len(manual_mappings) + len(self.static_mapping),
            'api_connection_valid': False,
            'priority_order': ['Manual', 'API', 'Estático']
        }
        
        # Verificar conexão da API
        try:
            status['api_connection_valid'] = self.contas_api.validar_conexao() if self.contas_api else False
        except:
            status['api_connection_valid'] = False
        
        return status
    
    def get_available_banks_summary(self) -> List[Dict[str, str]]:
        """
        Retorna resumo de todos os bancos disponíveis.
        Ordem de prioridade: Manual → API → Estático
        
        Returns:
            Lista com informações resumidas dos bancos
        """
        banks = []
        processed_codes = set()
        
        # 1. Bancos manuais (prioridade máxima)
        manual_mappings = self.manual_mapper.get_all_mappings()
        for code, data in manual_mappings.items():
            banks.append({
                'codigo': code,
                'nome': data.get('nome_banco', 'N/A'),
                'fonte': '🔧 Manual',
                'status': 'Ativo' if data.get('ativo', True) else 'Inativo',
                'prioridade': 1
            })
            processed_codes.add(code)
        
        # 2. Bancos da API (segunda prioridade)
        if self.use_api and self.api_mapping:
            for code, data in self.api_mapping.items():
                if code not in processed_codes:  # Não duplicar se já temos manual
                    banks.append({
                        'codigo': code,
                        'nome': data.get('nome_sistema', 'N/A'),
                        'fonte': '🌐 API MR',
                        'status': 'Ativo' if data.get('ativo', True) else 'Inativo',
                        'prioridade': 2
                    })
                    processed_codes.add(code)
        
        # 3. Bancos estáticos (terceira prioridade)
        for code, data in self.static_mapping.items():
            if code not in processed_codes:  # Não duplicar se já temos manual ou API
                banks.append({
                    'codigo': code,
                    'nome': data.get('nome_sistema', 'N/A'),
                    'fonte': '📋 Estático',
                    'status': 'Ativo' if data.get('ativo', True) else 'Inativo',
                    'prioridade': 3
                })
        
        # Ordenar por prioridade e depois por código
        return sorted(banks, key=lambda x: (x['prioridade'], x['codigo']))
    
    def get_mapping_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do mapeamento atual.
        
        Returns:
            Dict com estatísticas
        """
        total_static = len([b for b in self.static_mapping.values() if b.get('ativo', True)])
        total_api = len([b for b in self.api_mapping.values() if b.get('ativo', True)]) if self.use_api else 0
        
        return {
            'usando_api': self.use_api,
            'bancos_estaticos': total_static,
            'bancos_api': total_api,
            'total_bancos': total_static + total_api,
            'fonte_primaria': 'API' if self.use_api else 'Estático'
        }