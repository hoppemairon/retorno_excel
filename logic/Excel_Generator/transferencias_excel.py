"""
Transferências Excel Generator
Responsável pela geração de arquivos Excel para importação de transferências entre empresas/bancos.
"""

import pandas as pd
import io
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransferenciasExcelGenerator:
    """
    Classe responsável por gerar arquivos Excel formatados para importação de transferências.
    O formato específico será definido na Fase 4 conforme especificações do usuário.
    """
    
    def __init__(self):
        # Template básico - será refinado na Fase 4
        self.template_columns = [
            'data',
            'descricao',
            'valor',
            'empresa_origem',
            'conta_origem',
            'banco_origem',
            'empresa_destino', 
            'conta_destino',
            'banco_destino',
            'tipo_transferencia',
            'observacoes'
        ]
        
        self.required_columns = [
            'data', 'valor', 'empresa_origem', 'empresa_destino', 'tipo_transferencia'
        ]
        
        # Tipos de transferência
        self.transfer_types = {
            'INTER_COMPANY': 'Entre Empresas (CNPJs diferentes)',
            'INTER_BANK': 'Entre Bancos (mesmo CNPJ)',
            'INTERNAL': 'Transferência Interna'
        }
    
    def generate_excel(self, df_transfers: pd.DataFrame, filename: str = None) -> io.BytesIO:
        """
        Gera arquivo Excel para importação de transferências.
        
        Args:
            df_transfers (pd.DataFrame): DataFrame com transferências identificadas
            filename (str): Nome do arquivo (opcional)
            
        Returns:
            BytesIO com conteúdo do Excel
        """
        if df_transfers.empty:
            logger.warning("DataFrame vazio - gerando Excel vazio para transferências")
            return self._generate_empty_excel()
        
        # Preparar dados para o formato de importação
        df_formatted = self._format_for_import(df_transfers)
        
        # Gerar Excel
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Aba principal com transferências
            df_formatted.to_excel(
                writer, 
                sheet_name='Transferencias', 
                index=False,
                startrow=1
            )
            
            # Aba com estatísticas
            self._add_statistics_sheet(writer, df_transfers)
            
            # Adicionar cabeçalho informativo
            worksheet = writer.sheets['Transferencias']
            worksheet['A1'] = f'Transferências - Gerado em {datetime.now().strftime("%d/%m/%Y %H:%M")}'
            
            # Auto-ajustar largura das colunas
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        output.seek(0)
        logger.info(f"Excel de transferências gerado com {len(df_formatted)} registros")
        return output
    
    def _format_for_import(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formata DataFrame para o padrão de importação de transferências.
        Template básico - será refinado na Fase 4.
        """
        df_formatted = pd.DataFrame()
        
        # Mapeamento básico de colunas
        column_mapping = {
            'data': 'data',
            'descricao': 'descricao',
            'valor': 'valor',
            'cnpj_origem': 'empresa_origem',
            'conta_origem': 'conta_origem',
            'banco_origem': 'banco_origem',
            'cnpj_destino': 'empresa_destino',
            'conta_destino': 'conta_destino',
            'banco_destino': 'banco_destino',
            'tipo_transferencia': 'tipo_transferencia'
        }
        
        # Aplicar mapeamento
        for target_col, source_col in column_mapping.items():
            if source_col in df.columns:
                df_formatted[target_col] = df[source_col]
            else:
                df_formatted[target_col] = ''
        
        # Formatações específicas
        if 'data' in df_formatted.columns:
            df_formatted['data'] = pd.to_datetime(df_formatted['data'], errors='coerce').dt.strftime('%d/%m/%Y')
        
        if 'valor' in df_formatted.columns:
            df_formatted['valor'] = pd.to_numeric(df_formatted['valor'], errors='coerce').round(2)
        
        # Adicionar descrições amigáveis para tipos de transferência
        if 'tipo_transferencia' in df_formatted.columns:
            df_formatted['tipo_transferencia'] = df_formatted['tipo_transferencia'].map(
                lambda x: self.transfer_types.get(x, x)
            )
        
        # Adicionar colunas padrão que faltam
        for col in self.template_columns:
            if col not in df_formatted.columns:
                df_formatted[col] = ''
        
        # Reordenar colunas conforme template
        df_formatted = df_formatted[self.template_columns]
        
        return df_formatted
    
    def _add_statistics_sheet(self, writer: pd.ExcelWriter, df: pd.DataFrame):
        """Adiciona aba com estatísticas das transferências."""
        try:
            stats_data = []
            
            # Estatísticas gerais
            stats_data.append(['Estatísticas Gerais', ''])
            stats_data.append(['Total de Transferências', len(df)])
            
            if 'tipo_transferencia' in df.columns:
                transfer_counts = df['tipo_transferencia'].value_counts()
                stats_data.append(['', ''])
                stats_data.append(['Por Tipo de Transferência', ''])
                for transfer_type, count in transfer_counts.items():
                    type_desc = self.transfer_types.get(transfer_type, transfer_type)
                    stats_data.append([type_desc, count])
            
            if 'valor' in df.columns:
                total_valor = pd.to_numeric(df['valor'], errors='coerce').sum()
                stats_data.append(['', ''])
                stats_data.append(['Valores', ''])
                stats_data.append(['Valor Total das Transferências', f'R$ {total_valor:,.2f}'])
            
            # Criar DataFrame com estatísticas
            df_stats = pd.DataFrame(stats_data, columns=['Descrição', 'Valor'])
            df_stats.to_excel(writer, sheet_name='Estatisticas', index=False)
            
        except Exception as e:
            logger.warning(f"Erro ao gerar aba de estatísticas: {str(e)}")
    
    def _generate_empty_excel(self) -> io.BytesIO:
        """Gera Excel vazio com template de transferências."""
        output = io.BytesIO()
        
        # DataFrame vazio com colunas do template
        df_empty = pd.DataFrame(columns=self.template_columns)
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_empty.to_excel(writer, sheet_name='Transferencias', index=False)
            
            # Adicionar aba com tipos de transferência
            df_types = pd.DataFrame(list(self.transfer_types.items()), 
                                   columns=['Código', 'Descrição'])
            df_types.to_excel(writer, sheet_name='Tipos', index=False)
        
        output.seek(0)
        return output
    
    def detect_potential_transfers(self, df_transactions: pd.DataFrame) -> pd.DataFrame:
        """
        Detecta potenciais transferências no DataFrame de transações.
        Implementação básica - será refinada na Fase 3.
        
        Args:
            df_transactions (pd.DataFrame): DataFrame com todas as transações
            
        Returns:
            DataFrame com transferências identificadas
        """
        if df_transactions.empty:
            return pd.DataFrame()
        
        potential_transfers = []
        
        # Algoritmo básico de detecção
        # Busca por transações com valores iguais em datas próximas
        for idx, row in df_transactions.iterrows():
            if row['movimento'] == 'DEBITO':  # Buscar débitos
                valor = abs(float(row.get('valor_absoluto', 0)))
                data = pd.to_datetime(row.get('data'), errors='coerce')
                
                if pd.notna(data) and valor > 0:
                    # Buscar créditos correspondentes
                    matches = df_transactions[
                        (df_transactions['movimento'] == 'CREDITO') &
                        (abs(pd.to_numeric(df_transactions['valor_absoluto'], errors='coerce') - valor) < 0.01) &
                        (abs((pd.to_datetime(df_transactions['data'], errors='coerce') - data).dt.days) <= 2)
                    ]
                    
                    for _, match in matches.iterrows():
                        # Determinar tipo de transferência
                        origem_cnpj = row.get('cnpj', '')
                        destino_cnpj = match.get('cnpj', '')
                        
                        if origem_cnpj != destino_cnpj and origem_cnpj and destino_cnpj:
                            transfer_type = 'INTER_COMPANY'
                        else:
                            transfer_type = 'INTER_BANK'
                        
                        potential_transfers.append({
                            'data': data.strftime('%Y-%m-%d') if pd.notna(data) else '',
                            'valor': valor,
                            'descricao': f"Transferência: {row.get('descricao', '')}",
                            'cnpj_origem': origem_cnpj,
                            'conta_origem': row.get('conta_numero', ''),
                            'banco_origem': row.get('banco_nome', ''),
                            'cnpj_destino': destino_cnpj,
                            'conta_destino': match.get('conta_numero', ''),
                            'banco_destino': match.get('banco_nome', ''),
                            'tipo_transferencia': transfer_type,
                            'confianca': 0.8  # Score de confiança da detecção
                        })
        
        return pd.DataFrame(potential_transfers)
    
    def validate_transfers_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida se os dados de transferências estão corretos.
        
        Args:
            df (pd.DataFrame): DataFrame com transferências
            
        Returns:
            Dict com resultado da validação
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'summary': {}
        }
        
        if df.empty:
            validation_result['valid'] = False
            validation_result['errors'].append('DataFrame vazio')
            return validation_result
        
        # Verificar colunas obrigatórias
        missing_required = [col for col in self.required_columns if col not in df.columns]
        if missing_required:
            validation_result['valid'] = False
            validation_result['errors'].append(f'Colunas obrigatórias ausentes: {missing_required}')
        
        # Validações específicas de transferências
        if 'empresa_origem' in df.columns and 'empresa_destino' in df.columns:
            same_origin_dest = df[df['empresa_origem'] == df['empresa_destino']]
            if not same_origin_dest.empty:
                validation_result['warnings'].append(
                    f'{len(same_origin_dest)} transferências com origem e destino iguais'
                )
        
        # Resumo
        validation_result['summary'] = {
            'total_transfers': len(df),
            'inter_company': len(df[df.get('tipo_transferencia', '') == 'INTER_COMPANY']) if 'tipo_transferencia' in df.columns else 0,
            'inter_bank': len(df[df.get('tipo_transferencia', '') == 'INTER_BANK']) if 'tipo_transferencia' in df.columns else 0,
            'columns_count': len(df.columns)
        }
        
        return validation_result
    
    def get_template_info(self) -> Dict[str, Any]:
        """
        Retorna informações sobre o template de transferências.
        
        Returns:
            Dict com informações do template
        """
        return {
            'template_columns': self.template_columns,
            'required_columns': self.required_columns,
            'optional_columns': [col for col in self.template_columns if col not in self.required_columns],
            'transfer_types': self.transfer_types,
            'description': 'Template básico para importação de transferências - será refinado na Fase 4'
        }