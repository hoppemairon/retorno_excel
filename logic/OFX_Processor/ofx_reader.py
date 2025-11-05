"""
OFX Reader Module
Responsável pela leitura e processamento de arquivos OFX de diferentes bancos.
"""

import pandas as pd
from ofxparse import OfxParser
from typing import List, Dict, Any, Optional
import io
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OFXReader:
    """
    Classe responsável por ler e processar arquivos OFX de múltiplos bancos.
    Extrai transações e organiza dados para processamento posterior.
    """
    
    def __init__(self):
        self.supported_banks = [
            'Banco do Brasil',
            'Bradesco',
            'Itaú',
            'Santander',
            'Caixa Econômica Federal',
            'Banco Inter',
            'Nubank',
            'Sicoob',
            'Banrisul'
        ]
        
    def read_ofx_file(self, file_content: str, file_name: str = "") -> Dict[str, Any]:
        """
        Lê um arquivo OFX e extrai as informações principais.
        
        Args:
            file_content (str): Conteúdo do arquivo OFX
            file_name (str): Nome do arquivo para identificação
            
        Returns:
            Dict com informações da conta e transações
        """
        try:
            # Converter string para arquivo em memória
            ofx_stream = io.StringIO(file_content)
            
            # Parse do arquivo OFX
            ofx = OfxParser.parse(ofx_stream)
            
            # Extrair informações da conta
            account_info = self._extract_account_info(ofx)
            account_info['source_file'] = file_name
            
            # Extrair transações
            transactions = self._extract_transactions(ofx)
            
            logger.info(f"Arquivo {file_name} processado: {len(transactions)} transações encontradas")
            
            return {
                'account_info': account_info,
                'transactions': transactions,
                'raw_ofx': ofx
            }
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo OFX {file_name}: {str(e)}")
            return {
                'account_info': {'error': str(e), 'source_file': file_name},
                'transactions': [],
                'raw_ofx': None
            }
    
    def read_multiple_ofx(self, files_data: List[Dict[str, str]]) -> pd.DataFrame:
        """
        Processa múltiplos arquivos OFX e retorna DataFrame consolidado.
        
        Args:
            files_data: Lista de dicts com 'content' e 'name' dos arquivos
            
        Returns:
            DataFrame consolidado com todas as transações
        """
        all_transactions = []
        
        for file_data in files_data:
            result = self.read_ofx_file(
                file_data['content'], 
                file_data.get('name', 'arquivo_sem_nome')
            )
            
            # Adicionar informações da conta às transações
            transactions = result['transactions']
            account_info = result['account_info']
            
            for transaction in transactions:
                transaction.update({
                    'conta_banco': account_info.get('bank_id', ''),
                    'conta_agencia': account_info.get('branch_id', ''),
                    'conta_numero': account_info.get('account_id', ''),
                    'arquivo_origem': account_info.get('source_file', ''),
                    'banco_nome': account_info.get('bank_name', '')
                })
                
            all_transactions.extend(transactions)
        
        # Converter para DataFrame
        if all_transactions:
            df = pd.DataFrame(all_transactions)
            # Ordenar por data
            if 'data' in df.columns:
                df['data'] = pd.to_datetime(df['data'], errors='coerce')
                df = df.sort_values('data')
            return df
        else:
            return pd.DataFrame()
    
    def _extract_account_info(self, ofx) -> Dict[str, str]:
        """Extrai informações da conta do arquivo OFX."""
        try:
            account = ofx.account
            return {
                'bank_id': getattr(account, 'routing_number', ''),
                'bank_name': self._identify_bank(getattr(account, 'routing_number', '')),
                'branch_id': getattr(account, 'branch_id', ''),
                'account_id': getattr(account, 'account_id', ''),
                'account_type': getattr(account, 'account_type', ''),
                'currency': getattr(account.statement, 'currency', 'BRL') if hasattr(account, 'statement') else 'BRL'
            }
        except Exception as e:
            logger.warning(f"Erro ao extrair informações da conta: {str(e)}")
            return {}
    
    def _extract_transactions(self, ofx) -> List[Dict[str, Any]]:
        """Extrai transações do arquivo OFX."""
        transactions = []
        
        try:
            account = ofx.account
            if hasattr(account, 'statement') and hasattr(account.statement, 'transactions'):
                for transaction in account.statement.transactions:
                    trans_data = {
                        'id': getattr(transaction, 'id', ''),
                        'data': getattr(transaction, 'date', None),
                        'valor': float(getattr(transaction, 'amount', 0)),
                        'tipo': getattr(transaction, 'type', ''),
                        'descricao': getattr(transaction, 'memo', ''),
                        'descricao_detalhada': getattr(transaction, 'payee', ''),
                        'saldo': float(getattr(transaction, 'balance', 0)) if hasattr(transaction, 'balance') else None
                    }
                    
                    # Classificar como débito ou crédito
                    trans_data['movimento'] = 'CREDITO' if trans_data['valor'] > 0 else 'DEBITO'
                    trans_data['valor_absoluto'] = abs(trans_data['valor'])
                    
                    transactions.append(trans_data)
                    
        except Exception as e:
            logger.warning(f"Erro ao extrair transações: {str(e)}")
            
        return transactions
    
    def _identify_bank(self, routing_number: str) -> str:
        """
        Identifica o banco com base no routing number.
        Futuramente será substituído pela API de mapeamento.
        """
        bank_codes = {
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
        
        return bank_codes.get(routing_number[:3], f'Banco não identificado ({routing_number})')
    
    def get_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Gera resumo das transações processadas.
        
        Args:
            df: DataFrame com transações
            
        Returns:
            Dict com estatísticas resumidas
        """
        if df.empty:
            return {'total_transactions': 0}
            
        summary = {
            'total_transactions': len(df),
            'total_creditos': len(df[df['movimento'] == 'CREDITO']),
            'total_debitos': len(df[df['movimento'] == 'DEBITO']),
            'valor_total_creditos': df[df['movimento'] == 'CREDITO']['valor_absoluto'].sum(),
            'valor_total_debitos': df[df['movimento'] == 'DEBITO']['valor_absoluto'].sum(),
            'periodo_inicio': df['data'].min() if 'data' in df.columns else None,
            'periodo_fim': df['data'].max() if 'data' in df.columns else None,
            'bancos_encontrados': df['banco_nome'].unique().tolist() if 'banco_nome' in df.columns else [],
            'arquivos_processados': df['arquivo_origem'].unique().tolist() if 'arquivo_origem' in df.columns else []
        }
        
        return summary