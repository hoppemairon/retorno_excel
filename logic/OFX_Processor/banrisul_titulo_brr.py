"""
Detector e Processador de Pagamentos TITULO BRR (Banrisul)
Identifica transações genéricas do OFX que precisam ser detalhadas via arquivo de retorno
"""

import pandas as pd
from typing import Dict, List, Tuple
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
        # Verificar qual coluna de banco está disponível
        banco_col = None
        for col in ['banco_nome', 'banco_nome_sistema', 'banco']:
            if col in df_ofx.columns:
                banco_col = col
                break
        
        if banco_col is None:
            # Se não encontrar coluna de banco, criar uma vazia
            df_ofx = df_ofx.copy()
            df_ofx['banco_temp'] = 'BANRISUL'  # Assumir que é Banrisul se não tiver info
            banco_col = 'banco_temp'
        
        filtros = (
            (df_ofx['descricao'].str.contains('PAGAMENTO TITULO BRR', case=False, na=False)) |
            (df_ofx['descricao'].str.contains('PAGAMENTO TIT BRR', case=False, na=False)) |
            (df_ofx['descricao'].str.contains('PAG TITULO BRR', case=False, na=False))
        ) & (
            df_ofx[banco_col].str.contains('BANRISUL', case=False, na=False)
        )
        
        transacoes_genericas = df_ofx[filtros].copy()
        transacoes_normais = df_ofx[~filtros].copy()
        
        # Estatísticas
        total_transacoes = len(df_ofx)
        total_genericas = len(transacoes_genericas)
        total_normais = len(transacoes_normais)
        # Verificar qual coluna de valor está disponível
        valor_col = None
        for col in ['valor_absoluto', 'valor', 'amount']:
            if col in transacoes_genericas.columns:
                valor_col = col
                break
        
        valor_total_generico = transacoes_genericas[valor_col].sum() if total_genericas > 0 and valor_col else 0
        
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
    
    def processar_arquivo_retorno(self, arquivo_content, nome_arquivo: str = ""):
        """
        Processa arquivo de retorno do banco para extrair detalhes dos pagamentos
        
        Args:
            arquivo_content: Conteúdo do arquivo de retorno (string ou bytes)
            nome_arquivo: Nome do arquivo original
            
        Returns:
            DataFrame com pagamentos detalhados ou Dict com detalhes extraídos
        """
        try:
            # Decodificar conteúdo do arquivo se necessário
            if isinstance(arquivo_content, bytes):
                content_str = arquivo_content.decode('utf-8', errors='ignore')
            else:
                content_str = str(arquivo_content)
            
            # Usar a lógica existente do CNAB240 para extrair pagamentos
            pagamentos_detalhados = self._extrair_pagamentos_do_retorno(content_str)
            
            # Armazenar arquivo processado
            self.arquivos_retorno[nome_arquivo] = {
                'conteudo': content_str,
                'pagamentos': pagamentos_detalhados,
                'data_processamento': datetime.now()
            }
            
            # Retornar DataFrame se há pagamentos, senão DataFrame vazio
            if pagamentos_detalhados:
                df_resultado = pd.DataFrame(pagamentos_detalhados)
                return df_resultado
            else:
                # Retornar DataFrame vazio com estrutura esperada
                return pd.DataFrame(columns=['data', 'valor', 'beneficiario', 'nosso_numero', 'seu_numero'])
            
        except Exception as e:
            # Em caso de erro, retornar DataFrame vazio
            return pd.DataFrame(columns=['data', 'valor', 'beneficiario', 'nosso_numero', 'seu_numero'])
    
    def _extrair_pagamentos_do_retorno(self, content: str) -> List[Dict]:
        """
        Extrai pagamentos detalhados do arquivo de retorno CNAB240
        Usa a lógica real do sistema CNAB240 existente
        """
        pagamentos = []
        
        # Código de ocorrências CNAB240 (do arquivo original)
        codigo_ocorrencias = {
            "00": "Crédito efetuado",
            "01": "Insuficiência de fundos",
            "02": "Crédito cancelado pelo pagador/credor",
            "03": "Débito autorizado pela agência - efetuado",
            "HA": "Lote não aceito",
            "HB": "Inscrição da empresa inválida para o contrato",
            "HC": "Convênio com a empresa inexistente/inválido para o contrato",
            "HD": "Agência/conta corrente da empresa inexistente/inválida para o contrato",
            "HE": "Tipo de serviço inválido para o contrato",
            "HF": "Conta-Corrente da Empresa com saldo insuficiente",
            "H4": "Retorno de Crédito não Pago",
            "AA": "Controle inválido",
            "AB": "Tipo de operação inválido",
            "AC": "Tipo de serviço inválido",
            "AD": "Forma de lançamento inválida",
            "AE": "Tipo/número de inscrição inválido",
            "AF": "Código do convênio inválido",
            "AG": "Agência/conta corrente/Dv inválido"
        }
        
        # Processar linha por linha (lógica do Retono_Excel.py)
        for linha in content.splitlines():
            if len(linha) >= 150 and linha[13] == 'J':  # Segmento J
                nome_favorecido = linha[61:90].strip()
                data_pagamento = linha[91:100]
                valor = linha[101:114].strip()
                valor_pago = linha[27:36].strip()
                codigo_pagamento = linha[230:235].strip()
                descricao_confirmacao = codigo_ocorrencias.get(codigo_pagamento, codigo_pagamento)
                
                # Limpar caracteres zeros do nome
                if "0" in nome_favorecido:
                    nome_favorecido = nome_favorecido.replace("0", "")
                
                try:
                    data_formatada = f"{data_pagamento[4:8]}-{data_pagamento[2:4]}-{data_pagamento[0:2]}"
                    valor_formatado = int(valor) / 100 if valor.isdigit() else 0
                    valor_pago_formatado = int(valor_pago) / 100 if valor_pago.isdigit() else 0
                except ValueError:
                    data_formatada = ""
                    valor_formatado = 0
                    valor_pago_formatado = 0
                
                pagamento = {
                    'data': data_formatada,
                    'valor': valor_pago_formatado,
                    'valor_original': valor_formatado,
                    'beneficiario': nome_favorecido,
                    'codigo': codigo_pagamento,
                    'descricao': descricao_confirmacao,
                    'nosso_numero': linha[165:185].strip() if len(linha) > 185 else '',
                    'seu_numero': linha[185:205].strip() if len(linha) > 205 else '',
                    'linha_original': linha
                }
                pagamentos.append(pagamento)
        
        return pagamentos
    
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
            try:
                # Converter data OFX para comparação
                if isinstance(transacao_ofx.get('data'), str):
                    data_ofx = pd.to_datetime(transacao_ofx['data']).date()
                else:
                    data_ofx = transacao_ofx['data']
                
                # Obter valor da transação de forma flexível
                valor_ofx = float(transacao_ofx.get('valor_absoluto', 0) or 
                                transacao_ofx.get('valor', 0) or 
                                transacao_ofx.get('amount', 0))
                
                # Buscar em todos os arquivos de retorno
                for nome_arquivo, dados_arquivo in self.arquivos_retorno.items():
                    pagamentos = dados_arquivo.get('pagamentos', [])
                    
                    if not pagamentos:
                        continue
                    
                    # Calcular valor total do arquivo
                    valor_total_arquivo = sum(p.get('valor', 0) for p in pagamentos)
                    
                    # Verificar matching por valor
                    if valor_total_arquivo > 0:
                        diferenca_valor = abs(valor_ofx - valor_total_arquivo) / max(valor_ofx, valor_total_arquivo)
                        
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
                            break  # Primeiro match encontrado para esta transação
                            
            except Exception as e:
                # Log do erro mas continua processamento
                print(f"Erro no matching da transação {transacao_ofx}: {e}")
                continue
        
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
                    'valor_ofx_original': transacao_base.get('valor_absoluto', 0) or transacao_base.get('valor', 0) or transacao_base.get('amount', 0),
                    'descricao_ofx_original': transacao_base['descricao'],
                    'banco_nome': transacao_base.get('banco_nome') or transacao_base.get('banco_nome_sistema') or transacao_base.get('banco', 'BANRISUL'),
                    
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