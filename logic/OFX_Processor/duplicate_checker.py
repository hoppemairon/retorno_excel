"""
Duplicate Checker Module
Responsável por detectar transações duplicadas entre arquivos OFX e sistema MR.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime, timedelta
import unidecode
from fuzzywuzzy import fuzz

# Importar API existente
from ..Sistema_MR.API_MR import buscar_lancamentos_api

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuplicateChecker:
    """
    Classe responsável por detectar duplicatas entre transações OFX e sistema MR.
    """
    
    def __init__(self):
        # Configurações padrão para matching
        self.default_config = {
            'date_tolerance_days': 2,        # Tolerância em dias para datas
            'value_tolerance_percent': 0.01, # Tolerância percentual para valores (1%)
            'value_tolerance_absolute': 0.02, # Tolerância absoluta para valores (R$ 0.02)
            'description_similarity_threshold': 80,  # Similaridade mínima para descrições (%)
            'require_exact_value': True,     # Exigir valor exato ou usar tolerância
            'require_same_day': False,       # Exigir mesma data ou usar tolerância
            'min_confidence_score': 70       # Score mínimo para considerar duplicata
        }
        
        self.matching_config = self.default_config.copy()
    
    def check_duplicates_with_mr_api(
        self, 
        df_ofx: pd.DataFrame, 
        empresa_ids: List[str],
        anos: str = "2025",
        config: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Verifica duplicatas entre transações OFX e dados da API MR.
        
        Args:
            df_ofx (pd.DataFrame): DataFrame com transações OFX
            empresa_ids (List[str]): Lista de IDs das empresas para buscar na API
            anos (str): Anos para buscar na API (padrão: "2025")
            config (Dict): Configurações de matching (opcional)
            
        Returns:
            Dict com resultados da verificação de duplicatas
        """
        if df_ofx.empty:
            return {
                'total_ofx': 0,
                'total_mr': 0,
                'duplicatas': [],
                'novas_transacoes': [],
                'estatisticas': {}
            }
        
        # Usar configurações personalizadas se fornecidas
        if config:
            self.matching_config.update(config)
        
        logger.info(f"Iniciando verificação de duplicatas para {len(empresa_ids)} empresas")
        
        # Buscar dados da API MR
        df_mr = self._buscar_dados_mr(empresa_ids, anos)
        
        if df_mr.empty:
            logger.warning("Nenhum dado retornado da API MR")
            return {
                'total_ofx': len(df_ofx),
                'total_mr': 0,
                'duplicatas': [],
                'novas_transacoes': df_ofx.to_dict('records'),
                'estatisticas': {'erro': 'Nenhum dado da API MR'}
            }
        
        # Preparar dados para comparação
        df_ofx_prep = self._prepare_ofx_data(df_ofx)
        df_mr_prep = self._prepare_mr_data(df_mr)
        
        # Detectar duplicatas
        duplicatas, novas_transacoes = self._detect_duplicates(df_ofx_prep, df_mr_prep)
        
        # Gerar estatísticas
        estatisticas = self._generate_statistics(df_ofx_prep, df_mr_prep, duplicatas, novas_transacoes)
        
        logger.info(f"Verificação concluída: {len(duplicatas)} duplicatas, {len(novas_transacoes)} novas")
        
        return {
            'total_ofx': len(df_ofx),
            'total_mr': len(df_mr),
            'duplicatas': duplicatas,
            'novas_transacoes': novas_transacoes,
            'estatisticas': estatisticas,
            'config_usada': self.matching_config.copy()
        }
    
    def _buscar_dados_mr(self, empresa_ids: List[str], anos: str) -> pd.DataFrame:
        """Busca dados da API MR para as empresas especificadas."""
        try:
            # Converter lista para string separada por vírgulas
            ids_string = ",".join(empresa_ids)
            
            df_mr = buscar_lancamentos_api(ids_empresa=ids_string, anos=anos)
            
            if not df_mr.empty:
                logger.info(f"API MR retornou {len(df_mr)} lançamentos")
            else:
                logger.warning("API MR retornou DataFrame vazio")
                
            return df_mr
            
        except Exception as e:
            logger.error(f"Erro ao buscar dados da API MR: {str(e)}")
            return pd.DataFrame()
    
    def _prepare_ofx_data(self, df_ofx: pd.DataFrame) -> pd.DataFrame:
        """Prepara dados OFX para comparação."""
        df_prep = df_ofx.copy()
        
        # Normalizar colunas essenciais
        if 'data' in df_prep.columns:
            df_prep['data_norm'] = pd.to_datetime(df_prep['data'], errors='coerce').dt.date
        
        if 'valor_absoluto' in df_prep.columns:
            df_prep['valor_norm'] = pd.to_numeric(df_prep['valor_absoluto'], errors='coerce')
        elif 'valor' in df_prep.columns:
            df_prep['valor_norm'] = pd.to_numeric(df_prep['valor'], errors='coerce').abs()
        
        # Normalizar descrições
        desc_columns = ['descricao', 'descricao_detalhada', 'memo']
        df_prep['descricao_norm'] = ''
        
        for col in desc_columns:
            if col in df_prep.columns:
                df_prep['descricao_norm'] = df_prep['descricao_norm'] + ' ' + df_prep[col].astype(str)
        
        df_prep['descricao_norm'] = df_prep['descricao_norm'].apply(self._normalize_text)
        
        # Adicionar índice original para referência
        df_prep['ofx_index'] = df_prep.index
        
        return df_prep
    
    def _prepare_mr_data(self, df_mr: pd.DataFrame) -> pd.DataFrame:
        """Prepara dados MR para comparação."""
        df_prep = df_mr.copy()
        
        # Normalizar colunas essenciais
        if 'data' in df_prep.columns:
            df_prep['data_norm'] = pd.to_datetime(df_prep['data'], errors='coerce').dt.date
        
        if 'valor' in df_prep.columns:
            df_prep['valor_norm'] = pd.to_numeric(df_prep['valor'], errors='coerce').abs()
        
        # Normalizar descrições (usar múltiplas colunas se disponível)
        desc_columns = ['contato', 'categoria', 'observacoes', 'descricao']
        df_prep['descricao_norm'] = ''
        
        for col in desc_columns:
            if col in df_prep.columns:
                df_prep['descricao_norm'] = df_prep['descricao_norm'] + ' ' + df_prep[col].astype(str)
        
        df_prep['descricao_norm'] = df_prep['descricao_norm'].apply(self._normalize_text)
        
        # Adicionar índice original para referência
        df_prep['mr_index'] = df_prep.index
        
        return df_prep
    
    def _normalize_text(self, text: str) -> str:
        """Normaliza texto para comparação."""
        if pd.isna(text) or text == 'nan':
            return ""
        
        # Converter para string, remover acentos, converter para minúsculas
        normalized = unidecode.unidecode(str(text)).lower()
        
        # Remover caracteres especiais e múltiplos espaços
        import re
        normalized = re.sub(r'[^a-zA-Z0-9\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _detect_duplicates(
        self, 
        df_ofx: pd.DataFrame, 
        df_mr: pd.DataFrame
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Detecta duplicatas entre transações OFX e MR.
        
        Returns:
            Tuple com (duplicatas_encontradas, transacoes_novas)
        """
        duplicatas = []
        indices_ofx_duplicadas = set()
        
        # Para cada transação OFX, procurar possíveis duplicatas no MR
        for idx_ofx, row_ofx in df_ofx.iterrows():
            if pd.isna(row_ofx.get('data_norm')) or pd.isna(row_ofx.get('valor_norm')):
                continue
                
            # Filtrar MR por critérios básicos (data e valor)
            candidatos_mr = self._filter_mr_candidates(row_ofx, df_mr)
            
            if candidatos_mr.empty:
                continue
            
            # Verificar cada candidato
            best_match = None
            best_score = 0
            
            for idx_mr, row_mr in candidatos_mr.iterrows():
                score = self._calculate_similarity_score(row_ofx, row_mr)
                
                if score > best_score and score >= self.matching_config['min_confidence_score']:
                    best_score = score
                    best_match = {
                        'ofx_index': row_ofx['ofx_index'],
                        'mr_index': row_mr['mr_index'],
                        'score': score,
                        'ofx_data': row_ofx.to_dict(),
                        'mr_data': row_mr.to_dict(),
                        'match_details': self._get_match_details(row_ofx, row_mr)
                    }
            
            if best_match:
                duplicatas.append(best_match)
                indices_ofx_duplicadas.add(row_ofx['ofx_index'])
        
        # Identificar transações novas (não duplicadas)
        df_novas = df_ofx[~df_ofx['ofx_index'].isin(indices_ofx_duplicadas)]
        novas_transacoes = df_novas.to_dict('records')
        
        return duplicatas, novas_transacoes
    
    def _filter_mr_candidates(self, row_ofx: pd.Series, df_mr: pd.DataFrame) -> pd.DataFrame:
        """Filtra candidatos MR baseado em critérios básicos."""
        
        # Filtro por data
        data_ofx = row_ofx['data_norm']
        tolerance_days = self.matching_config['date_tolerance_days']
        
        if self.matching_config['require_same_day']:
            date_filter = df_mr['data_norm'] == data_ofx
        else:
            date_min = data_ofx - timedelta(days=tolerance_days)
            date_max = data_ofx + timedelta(days=tolerance_days)
            date_filter = (df_mr['data_norm'] >= date_min) & (df_mr['data_norm'] <= date_max)
        
        # Filtro por valor
        valor_ofx = row_ofx['valor_norm']
        
        if self.matching_config['require_exact_value']:
            value_filter = df_mr['valor_norm'] == valor_ofx
        else:
            # Usar tolerância percentual e absoluta
            tolerance_percent = self.matching_config['value_tolerance_percent']
            tolerance_absolute = self.matching_config['value_tolerance_absolute']
            
            tolerance_value = max(
                valor_ofx * tolerance_percent,
                tolerance_absolute
            )
            
            value_filter = (
                (df_mr['valor_norm'] >= valor_ofx - tolerance_value) & 
                (df_mr['valor_norm'] <= valor_ofx + tolerance_value)
            )
        
        return df_mr[date_filter & value_filter]
    
    def _calculate_similarity_score(self, row_ofx: pd.Series, row_mr: pd.Series) -> float:
        """Calcula score de similaridade entre duas transações."""
        scores = []
        
        # Score de data (0-30 pontos)
        date_score = self._calculate_date_score(row_ofx['data_norm'], row_mr['data_norm'])
        scores.append(('data', date_score, 30))
        
        # Score de valor (0-40 pontos)
        value_score = self._calculate_value_score(row_ofx['valor_norm'], row_mr['valor_norm'])
        scores.append(('valor', value_score, 40))
        
        # Score de descrição (0-30 pontos)
        desc_score = self._calculate_description_score(
            row_ofx['descricao_norm'], 
            row_mr['descricao_norm']
        )
        scores.append(('descricao', desc_score, 30))
        
        # Calcular score ponderado
        total_score = sum(score * weight for _, score, weight in scores)
        
        return total_score
    
    def _calculate_date_score(self, date1, date2) -> float:
        """Calcula score baseado na diferença de datas."""
        if pd.isna(date1) or pd.isna(date2):
            return 0.0
        
        diff_days = abs((date1 - date2).days)
        
        if diff_days == 0:
            return 1.0
        elif diff_days <= self.matching_config['date_tolerance_days']:
            return max(0.0, 1.0 - (diff_days / self.matching_config['date_tolerance_days']))
        else:
            return 0.0
    
    def _calculate_value_score(self, value1, value2) -> float:
        """Calcula score baseado na diferença de valores."""
        if pd.isna(value1) or pd.isna(value2):
            return 0.0
        
        if value1 == value2:
            return 1.0
        
        # Calcular diferença percentual
        diff_percent = abs(value1 - value2) / max(value1, value2)
        
        if diff_percent <= self.matching_config['value_tolerance_percent']:
            return max(0.0, 1.0 - (diff_percent / self.matching_config['value_tolerance_percent']))
        else:
            return 0.0
    
    def _calculate_description_score(self, desc1: str, desc2: str) -> float:
        """Calcula score baseado na similaridade de descrições."""
        if not desc1 or not desc2:
            return 0.0
        
        # Usar fuzzywuzzy para calcular similaridade
        similarity = fuzz.token_sort_ratio(desc1, desc2) / 100.0
        
        threshold = self.matching_config['description_similarity_threshold'] / 100.0
        
        if similarity >= threshold:
            return similarity
        else:
            return 0.0
    
    def _get_match_details(self, row_ofx: pd.Series, row_mr: pd.Series) -> Dict:
        """Gera detalhes do matching para análise."""
        return {
            'data_ofx': str(row_ofx['data_norm']),
            'data_mr': str(row_mr['data_norm']),
            'valor_ofx': float(row_ofx['valor_norm']),
            'valor_mr': float(row_mr['valor_norm']),
            'desc_ofx': row_ofx['descricao_norm'][:100],
            'desc_mr': row_mr['descricao_norm'][:100],
            'date_score': self._calculate_date_score(row_ofx['data_norm'], row_mr['data_norm']),
            'value_score': self._calculate_value_score(row_ofx['valor_norm'], row_mr['valor_norm']),
            'desc_score': self._calculate_description_score(row_ofx['descricao_norm'], row_mr['descricao_norm'])
        }
    
    def _generate_statistics(
        self, 
        df_ofx: pd.DataFrame, 
        df_mr: pd.DataFrame, 
        duplicatas: List[Dict], 
        novas: List[Dict]
    ) -> Dict:
        """Gera estatísticas do processo de verificação."""
        
        total_ofx = len(df_ofx)
        total_mr = len(df_mr)
        total_duplicatas = len(duplicatas)
        total_novas = len(novas)
        
        # Calcular scores médios
        if duplicatas:
            scores = [d['score'] for d in duplicatas]
            score_medio = np.mean(scores)
            score_min = min(scores)
            score_max = max(scores)
        else:
            score_medio = score_min = score_max = 0
        
        return {
            'total_transacoes_ofx': total_ofx,
            'total_lancamentos_mr': total_mr,
            'total_duplicatas': total_duplicatas,
            'total_novas': total_novas,
            'percentual_duplicatas': (total_duplicatas / total_ofx * 100) if total_ofx > 0 else 0,
            'percentual_novas': (total_novas / total_ofx * 100) if total_ofx > 0 else 0,
            'score_medio': score_medio,
            'score_minimo': score_min,
            'score_maximo': score_max,
            'config_utilizada': self.matching_config.copy()
        }
    
    def update_config(self, new_config: Dict):
        """Atualiza configurações de matching."""
        self.matching_config.update(new_config)
        logger.info(f"Configurações atualizadas: {new_config}")
    
    def reset_config(self):
        """Reseta configurações para os valores padrão."""
        self.matching_config = self.default_config.copy()
        logger.info("Configurações resetadas para padrão")