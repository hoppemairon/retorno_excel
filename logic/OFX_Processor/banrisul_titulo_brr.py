"""
Detector e Processador de Pagamentos TITULO BRR (Banrisul)
Identifica transações genéricas do OFX que precisam ser detalhadas via arquivo de retorno
"""

import pandas as pd
import re
from typing import Dict, List, Tuple, Optional
from datetime import datetime

class BanrisulTituloBRRProcessor:
    """
    Processa transações PAGAMENTO TITULO BRR do Banrisul
    Faz matching com arquivos de retorno para obter detalhes específicos
    """
    
    def __init__(self):
        self.transacoes_genericas = []
        self.arquivos_retorno = {}
        self.matches_encontrados = []
    
    def detectar_transacoes_genericas(self, df_ofx: pd.DataFrame) -> Dict:
        """
        Detecta transações OFX que são genéricas (PAGAMENTO TITULO BRR)
        
        Args:
            df_ofx: DataFrame com transações OFX processadas
            
        Returns:
            Dict com estatísticas e transações detectadas
        """
        # Filtros para detectar transações genéricas do Banrisul
        filtros = (
            (df_ofx['descricao'].str.contains('PAGAMENTO TITULO BRR', case=False, na=False)) |
            (df_ofx['descricao'].str.contains('PAGAMENTO TIT BRR', case=False, na=False)) |
            (df_ofx['descricao'].str.contains('PAG TITULO BRR', case=False, na=False))
        ) & (
            df_ofx['banco_nome'].str.contains('BANRISUL', case=False, na=False)
        )
        
        transacoes_genericas = df_ofx[filtros].copy()
        transacoes_normais = df_ofx[~filtros].copy()
        
        # Estatísticas
        total_transacoes = len(df_ofx)
        total_genericas = len(transacoes_genericas)
        total_normais = len(transacoes_normais)
        valor_total_generico = transacoes_genericas['valor_absoluto'].sum() if total_genericas > 0 else 0
        
        self.transacoes_genericas = transacoes_genericas.to_dict('records')
        
        return {
            'total_transacoes': total_transacoes,
            'transacoes_genericas': total_genericas,
            'transacoes_normais': total_normais,
            'valor_total_generico': valor_total_generico,
            'df_genericas': transacoes_genericas,
            'df_normais': transacoes_normais,
            'percentual_generico': (total_genericas / total_transacoes * 100) if total_transacoes > 0 else 0
        }
    
    def processar_arquivo_retorno(self, arquivo_content: bytes, nome_arquivo: str) -> Dict:
        """
        Processa arquivo de retorno do banco para extrair detalhes dos pagamentos
        
        Args:
            arquivo_content: Conteúdo do arquivo de retorno
            nome_arquivo: Nome do arquivo original
            
        Returns:
            Dict com detalhes extraídos
        """
        try:
            # Decodificar conteúdo do arquivo
            content_str = arquivo_content.decode('utf-8', errors='ignore')
            
            # Aqui você pode usar a lógica existente do CNAB240
            # Por enquanto, vou simular a extração
            pagamentos_detalhados = self._extrair_pagamentos_do_retorno(content_str)
            
            # Armazenar arquivo processado
            self.arquivos_retorno[nome_arquivo] = {
                'conteudo': content_str,
                'pagamentos': pagamentos_detalhados,
                'data_processamento': datetime.now()
            }
            
            return {
                'nome_arquivo': nome_arquivo,
                'total_pagamentos': len(pagamentos_detalhados),
                'valor_total': sum(p.get('valor', 0) for p in pagamentos_detalhados),
                'data_processamento': datetime.now(),
                'pagamentos': pagamentos_detalhados
            }
            
        except Exception as e:
            return {
                'erro': f"Erro ao processar arquivo {nome_arquivo}: {str(e)}",
                'nome_arquivo': nome_arquivo
            }
    
    def _extrair_pagamentos_do_retorno(self, content: str) -> List[Dict]:
        """
        Extrai pagamentos detalhados do arquivo de retorno CNAB240
        NOTA: Aqui deve usar a lógica existente do seu código CNAB
        """
        # PLACEHOLDER - implementar com sua lógica existente de CNAB240
        # Por agora, simulo alguns pagamentos para demonstrar
        pagamentos = []
        
        # Aqui você usaria sua função existente de processamento CNAB240
        # Exemplo de estrutura esperada:
        linhas = content.split('\n')
        
        for linha in linhas:
            if len(linha) >= 240:  # Linha CNAB240 válida
                # Simular extração (implementar com sua lógica real)
                if linha[7:8] == '3' and linha[13:14] == 'J':  # Segmento J
                    pagamento = {
                        'data': self._extrair_data_linha(linha),
                        'valor': self._extrair_valor_linha(linha),
                        'beneficiario': self._extrair_beneficiario_linha(linha),
                        'nosso_numero': self._extrair_nosso_numero_linha(linha),
                        'seu_numero': self._extrair_seu_numero_linha(linha),
                        'linha_original': linha
                    }
                    pagamentos.append(pagamento)
        
        return pagamentos
    
    def _extrair_data_linha(self, linha: str) -> str:
        """Extrai data da linha CNAB240 - implementar com sua lógica"""
        # PLACEHOLDER - implementar com posições corretas do CNAB240
        return "2025-10-01"  # Exemplo
    
    def _extrair_valor_linha(self, linha: str) -> float:
        """Extrai valor da linha CNAB240 - implementar com sua lógica"""
        # PLACEHOLDER - implementar com posições corretas do CNAB240
        return 1000.00  # Exemplo
    
    def _extrair_beneficiario_linha(self, linha: str) -> str:
        """Extrai beneficiário da linha CNAB240 - implementar com sua lógica"""
        # PLACEHOLDER - implementar com posições corretas do CNAB240
        return "BENEFICIARIO EXEMPLO"
    
    def _extrair_nosso_numero_linha(self, linha: str) -> str:
        """Extrai nosso número da linha CNAB240"""
        # PLACEHOLDER - implementar com posições corretas
        return "123456789"
    
    def _extrair_seu_numero_linha(self, linha: str) -> str:
        """Extrai seu número da linha CNAB240"""
        # PLACEHOLDER - implementar com posições corretas
        return "987654321"
    
    def fazer_matching_valor_data(self, tolerancia_dias: int = 2, tolerancia_valor: float = 0.01) -> List[Dict]:
        """
        Faz matching entre transações OFX genéricas e arquivos de retorno
        
        Args:
            tolerancia_dias: Tolerância em dias para matching de data
            tolerancia_valor: Tolerância percentual para matching de valor
            
        Returns:
            Lista de matches encontrados
        """
        matches = []
        
        for transacao_ofx in self.transacoes_genericas:
            data_ofx = pd.to_datetime(transacao_ofx['data']).date()
            valor_ofx = float(transacao_ofx['valor_absoluto'])
            
            # Buscar em todos os arquivos de retorno
            for nome_arquivo, dados_arquivo in self.arquivos_retorno.items():
                pagamentos = dados_arquivo['pagamentos']
                
                # Agrupar pagamentos por data e somar valores
                valor_total_arquivo = sum(p.get('valor', 0) for p in pagamentos)
                
                # Verificar se o valor total do arquivo bate com a transação OFX
                diferenca_valor = abs(valor_ofx - valor_total_arquivo) / valor_ofx
                
                if diferenca_valor <= tolerancia_valor:
                    match = {
                        'transacao_ofx': transacao_ofx,
                        'arquivo_retorno': nome_arquivo,
                        'valor_ofx': valor_ofx,
                        'valor_arquivo': valor_total_arquivo,
                        'diferenca_valor': diferenca_valor,
                        'pagamentos_detalhados': pagamentos,
                        'data_match': datetime.now(),
                        'confianca_match': 1.0 - diferenca_valor
                    }
                    matches.append(match)
                    break  # Primeiro match encontrado
        
        self.matches_encontrados = matches
        return matches
    
    def gerar_transacoes_detalhadas(self) -> pd.DataFrame:
        """
        Gera DataFrame com transações detalhadas baseadas nos matches
        
        Returns:
            DataFrame com transações expandidas dos arquivos de retorno
        """
        transacoes_detalhadas = []
        
        for match in self.matches_encontrados:
            transacao_base = match['transacao_ofx']
            pagamentos = match['pagamentos_detalhados']
            
            for pagamento in pagamentos:
                transacao_detalhada = {
                    # Dados da transação OFX original
                    'data_ofx_original': transacao_base['data'],
                    'valor_ofx_original': transacao_base['valor_absoluto'],
                    'descricao_ofx_original': transacao_base['descricao'],
                    'banco_nome': transacao_base['banco_nome'],
                    
                    # Dados detalhados do arquivo de retorno
                    'data': pagamento.get('data', transacao_base['data']),
                    'valor_absoluto': pagamento.get('valor', 0),
                    'descricao': f"PAGAMENTO - {pagamento.get('beneficiario', 'N/A')}",
                    'beneficiario': pagamento.get('beneficiario', 'N/A'),
                    'nosso_numero': pagamento.get('nosso_numero', 'N/A'),
                    'seu_numero': pagamento.get('seu_numero', 'N/A'),
                    
                    # Metadados
                    'origem': 'detalhamento_retorno',
                    'arquivo_retorno': match['arquivo_retorno'],
                    'confianca_match': match['confianca_match']
                }
                transacoes_detalhadas.append(transacao_detalhada)
        
        return pd.DataFrame(transacoes_detalhadas)
    
    def obter_estatisticas(self) -> Dict:
        """Retorna estatísticas do processamento"""
        return {
            'transacoes_genericas_detectadas': len(self.transacoes_genericas),
            'arquivos_retorno_carregados': len(self.arquivos_retorno),
            'matches_encontrados': len(self.matches_encontrados),
            'total_pagamentos_detalhados': sum(
                len(match['pagamentos_detalhados']) 
                for match in self.matches_encontrados
            )
        }


# Funções auxiliares para integração com Streamlit

def detectar_e_processar_titulo_brr(df_ofx: pd.DataFrame) -> Tuple[pd.DataFrame, Dict, BanrisulTituloBRRProcessor]:
    """
    Função principal para detectar e processar PAGAMENTO TITULO BRR
    
    Args:
        df_ofx: DataFrame com transações OFX
        
    Returns:
        Tuple com (df_processado, estatisticas, processor)
    """
    processor = BanrisulTituloBRRProcessor()
    
    # Detectar transações genéricas
    stats = processor.detectar_transacoes_genericas(df_ofx)
    
    return stats['df_normais'], stats, processor

def processar_arquivos_retorno_banrisul(arquivos_retorno: List, processor: BanrisulTituloBRRProcessor) -> Dict:
    """
    Processa lista de arquivos de retorno do Banrisul
    
    Args:
        arquivos_retorno: Lista de arquivos de retorno uploaded
        processor: Instância do processador
        
    Returns:
        Dict com resultados do processamento
    """
    resultados = []
    
    for arquivo in arquivos_retorno:
        resultado = processor.processar_arquivo_retorno(
            arquivo.getvalue(), 
            arquivo.name
        )
        resultados.append(resultado)
    
    return {
        'arquivos_processados': len(resultados),
        'resultados': resultados,
        'processor': processor
    }