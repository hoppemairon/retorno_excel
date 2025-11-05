"""
Lançamentos Excel Generator
Responsável pela geração de arquivos Excel para importação de lançamentos financeiros.
"""

import pandas as pd
import io
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LancamentosExcelGenerator:
    """
    Classe responsável por gerar arquivos Excel formatados para importação de lançamentos.
    O formato específico será definido na Fase 4 conforme especificações do usuário.
    """
    
    def __init__(self):
        # Template básico - será refinado na Fase 4
        self.template_columns = [
            'data',
            'descricao',
            'valor',
            'tipo_movimento',
            'conta_origem',
            'banco_origem',
            'categoria',
            'observacoes'
        ]
        
        self.required_columns = ['data', 'descricao', 'valor', 'tipo_movimento']
    
    def generate_excel(self, df_transactions: pd.DataFrame, filename: str = None) -> io.BytesIO:
        """
        Gera arquivo Excel para importação de lançamentos.
        
        Args:
            df_transactions (pd.DataFrame): DataFrame com transações processadas
            filename (str): Nome do arquivo (opcional)
            
        Returns:
            BytesIO com conteúdo do Excel
        """
        if df_transactions.empty:
            logger.warning("DataFrame vazio - gerando Excel vazio")
            return self._generate_empty_excel()
        
        # Preparar dados para o formato de importação
        df_formatted = self._format_for_import(df_transactions)
        
        # Gerar Excel
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Aba principal com lançamentos
            df_formatted.to_excel(
                writer, 
                sheet_name='Lancamentos', 
                index=False,
                startrow=1
            )
            
            # Adicionar cabeçalho informativo
            worksheet = writer.sheets['Lancamentos']
            worksheet['A1'] = f'Lançamentos Financeiros - Gerado em {datetime.now().strftime("%d/%m/%Y %H:%M")}'
            
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
        logger.info(f"Excel de lançamentos gerado com {len(df_formatted)} registros")
        return output
    
    def _format_for_import(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formata DataFrame para o padrão de importação.
        Template básico - será refinado na Fase 4.
        """
        df_formatted = pd.DataFrame()
        
        # Mapeamento básico de colunas
        column_mapping = {
            'data': 'data',
            'descricao': 'descricao',
            'valor_absoluto': 'valor',
            'movimento': 'tipo_movimento',
            'conta_numero': 'conta_origem',
            'banco_nome': 'banco_origem'
        }
        
        # Aplicar mapeamento
        for target_col, source_col in column_mapping.items():
            if source_col in df.columns:
                df_formatted[target_col] = df[source_col]
            else:
                df_formatted[target_col] = ''
        
        # Formatações específicas
        if 'data' in df_formatted.columns:
            df_formatted['data'] = pd.to_datetime(df_formatted['data']).dt.strftime('%d/%m/%Y')
        
        if 'valor' in df_formatted.columns:
            df_formatted['valor'] = pd.to_numeric(df_formatted['valor'], errors='coerce').round(2)
        
        # Adicionar colunas padrão que faltam
        for col in self.template_columns:
            if col not in df_formatted.columns:
                df_formatted[col] = ''
        
        # Reordenar colunas conforme template
        df_formatted = df_formatted[self.template_columns]
        
        return df_formatted
    
    def _generate_empty_excel(self) -> io.BytesIO:
        """Gera Excel vazio com template."""
        output = io.BytesIO()
        
        # DataFrame vazio com colunas do template
        df_empty = pd.DataFrame(columns=self.template_columns)
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_empty.to_excel(writer, sheet_name='Lancamentos', index=False)
        
        output.seek(0)
        return output
    
    def validate_data_for_import(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida se os dados estão prontos para importação.
        
        Args:
            df (pd.DataFrame): DataFrame a ser validado
            
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
        
        # Verificar dados válidos
        if 'data' in df.columns:
            invalid_dates = df[df['data'].isna() | (df['data'] == '')].index.tolist()
            if invalid_dates:
                validation_result['warnings'].append(f'{len(invalid_dates)} registros com data inválida')
        
        if 'valor' in df.columns:
            invalid_values = df[pd.to_numeric(df['valor'], errors='coerce').isna()].index.tolist()
            if invalid_values:
                validation_result['warnings'].append(f'{len(invalid_values)} registros com valor inválido')
        
        # Resumo
        validation_result['summary'] = {
            'total_records': len(df),
            'valid_records': len(df) - len(invalid_dates) - len(invalid_values) if 'data' in df.columns and 'valor' in df.columns else len(df),
            'columns_count': len(df.columns)
        }
        
        return validation_result
    
    def get_template_info(self) -> Dict[str, Any]:
        """
        Retorna informações sobre o template de importação.
        
        Returns:
            Dict com informações do template
        """
        return {
            'template_columns': self.template_columns,
            'required_columns': self.required_columns,
            'optional_columns': [col for col in self.template_columns if col not in self.required_columns],
            'description': 'Template básico para importação de lançamentos - será refinado na Fase 4'
        }