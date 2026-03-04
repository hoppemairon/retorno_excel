"""
Manual Bank Mapper
Módulo para configuração manual do mapeamento De/Para entre códigos OFX e códigos do sistema.
"""

import json
import os
import pandas as pd
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ManualBankMapper:
    """
    Classe para gerenciamento de mapeamentos manuais entre códigos de bancos OFX e sistema.
    """
    
    def __init__(self, config_file: str = "config/manual_bank_mapping.json"):
        self.config_file = config_file
        self.mappings = {}
        self.load_mappings()
    
    def load_mappings(self) -> bool:
        """
        Carrega mapeamentos salvos do arquivo de configuração.
        
        Returns:
            True se carregou com sucesso, False caso contrário
        """
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.mappings = data.get('mappings', {})
                    logger.info(f"✅ {len(self.mappings)} mapeamentos manuais carregados")
                    return True
            else:
                logger.info("📄 Arquivo de configuração não existe - iniciando vazio")
                self.mappings = {}
                return True
        except Exception as e:
            logger.error(f"❌ Erro ao carregar mapeamentos: {str(e)}")
            self.mappings = {}
            return False
    
    def save_mappings(self) -> bool:
        """
        Salva mapeamentos no arquivo de configuração.
        
        Returns:
            True se salvou com sucesso, False caso contrário
        """
        try:
            # Criar diretório se não existir
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            
            # Preparar dados para salvar
            config_data = {
                'mappings': self.mappings,
                'last_updated': datetime.now().isoformat(),
                'version': '1.0'
            }
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ {len(self.mappings)} mapeamentos salvos em {self.config_file}")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao salvar mapeamentos: {str(e)}")
            return False
    
    def add_mapping(self, codigo_ofx: str, codigo_sistema: str, nome_banco: str, ativo: bool = True) -> bool:
        """
        Adiciona ou atualiza um mapeamento manual.
        
        Args:
            codigo_ofx (str): Código do banco no OFX
            codigo_sistema (str): Código do banco no sistema
            nome_banco (str): Nome do banco
            ativo (bool): Se o mapeamento está ativo
            
        Returns:
            True se adicionou com sucesso
        """
        try:
            # Normalizar códigos
            codigo_ofx = self._normalize_code(codigo_ofx)
            codigo_sistema = self._normalize_code(codigo_sistema)
            
            if not codigo_ofx or not codigo_sistema:
                logger.warning("Códigos não podem estar vazios")
                return False
            
            # Adicionar mapeamento
            self.mappings[codigo_ofx] = {
                'codigo_sistema': codigo_sistema,
                'nome_banco': nome_banco.strip(),
                'ativo': ativo,
                'created_at': datetime.now().isoformat(),
                'source': 'manual'
            }
            
            logger.info(f"✅ Mapeamento adicionado: {codigo_ofx} → {codigo_sistema} ({nome_banco})")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao adicionar mapeamento: {str(e)}")
            return False
    
    def remove_mapping(self, codigo_ofx: str) -> bool:
        """
        Remove um mapeamento manual.
        
        Args:
            codigo_ofx (str): Código do banco no OFX
            
        Returns:
            True se removeu com sucesso
        """
        try:
            codigo_ofx = self._normalize_code(codigo_ofx)
            
            if codigo_ofx in self.mappings:
                removed = self.mappings.pop(codigo_ofx)
                logger.info(f"🗑️ Mapeamento removido: {codigo_ofx} ({removed.get('nome_banco', 'N/A')})")
                return True
            else:
                logger.warning(f"Mapeamento não encontrado: {codigo_ofx}")
                return False
        except Exception as e:
            logger.error(f"❌ Erro ao remover mapeamento: {str(e)}")
            return False
    
    def get_mapping(self, codigo_ofx: str) -> Optional[Dict[str, Any]]:
        """
        Obtém mapeamento para um código OFX.
        
        Args:
            codigo_ofx (str): Código do banco no OFX
            
        Returns:
            Dict com dados do mapeamento ou None se não encontrado
        """
        codigo_ofx = self._normalize_code(codigo_ofx)
        return self.mappings.get(codigo_ofx)
    
    def get_all_mappings(self) -> Dict[str, Dict[str, Any]]:
        """
        Retorna todos os mapeamentos.
        
        Returns:
            Dict com todos os mapeamentos
        """
        return self.mappings.copy()
    
    def get_mappings_dataframe(self) -> pd.DataFrame:
        """
        Retorna mapeamentos como DataFrame para exibição.
        
        Returns:
            DataFrame com os mapeamentos
        """
        if not self.mappings:
            return pd.DataFrame(columns=['Código OFX', 'Código Sistema', 'Nome Banco', 'Status'])
        
        data = []
        for codigo_ofx, mapping in self.mappings.items():
            data.append({
                'Código OFX': codigo_ofx,
                'Código Sistema': mapping.get('codigo_sistema', 'N/A'),
                'Nome Banco': mapping.get('nome_banco', 'N/A'),
                'Status': 'Ativo' if mapping.get('ativo', True) else 'Inativo',
                'Origem': mapping.get('source', 'manual')
            })
        
        return pd.DataFrame(data)
    
    def validate_mappings(self) -> Dict[str, List[str]]:
        """
        Valida todos os mapeamentos e retorna problemas encontrados.
        
        Returns:
            Dict com listas de problemas por categoria
        """
        issues = {
            'duplicated_ofx': [],
            'duplicated_system': [],
            'invalid_codes': [],
            'empty_names': []
        }
        
        system_codes_used = {}
        
        for codigo_ofx, mapping in self.mappings.items():
            codigo_sistema = mapping.get('codigo_sistema', '')
            nome_banco = mapping.get('nome_banco', '')
            
            # Verificar códigos duplicados no sistema
            if codigo_sistema:
                if codigo_sistema in system_codes_used:
                    issues['duplicated_system'].append(
                        f"Código sistema '{codigo_sistema}' usado por '{codigo_ofx}' e '{system_codes_used[codigo_sistema]}'"
                    )
                else:
                    system_codes_used[codigo_sistema] = codigo_ofx
            
            # Verificar códigos inválidos
            if not codigo_ofx or not codigo_sistema:
                issues['invalid_codes'].append(f"Códigos vazios em mapeamento: OFX='{codigo_ofx}', Sistema='{codigo_sistema}'")
            
            # Verificar nomes vazios
            if not nome_banco.strip():
                issues['empty_names'].append(f"Nome do banco vazio para código OFX '{codigo_ofx}'")
        
        return issues
    
    def import_from_csv(self, csv_content: str) -> Dict[str, Any]:
        """
        Importa mapeamentos de conteúdo CSV.
        
        Args:
            csv_content (str): Conteúdo do arquivo CSV
            
        Returns:
            Dict com resultado da importação
        """
        try:
            # Tentar ler CSV
            from io import StringIO
            df = pd.read_csv(StringIO(csv_content))
            
            # Verificar colunas obrigatórias
            required_columns = ['codigo_ofx', 'codigo_sistema', 'nome_banco']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return {
                    'success': False,
                    'error': f"Colunas obrigatórias faltando: {', '.join(missing_columns)}",
                    'imported': 0
                }
            
            # Importar mapeamentos
            imported_count = 0
            errors = []
            
            for _, row in df.iterrows():
                try:
                    codigo_ofx = str(row['codigo_ofx']).strip()
                    codigo_sistema = str(row['codigo_sistema']).strip()
                    nome_banco = str(row['nome_banco']).strip()
                    ativo = bool(row.get('ativo', True))
                    
                    if self.add_mapping(codigo_ofx, codigo_sistema, nome_banco, ativo):
                        imported_count += 1
                    else:
                        errors.append(f"Erro ao importar linha {row.name + 1}")
                        
                except Exception as e:
                    errors.append(f"Linha {row.name + 1}: {str(e)}")
            
            return {
                'success': True,
                'imported': imported_count,
                'errors': errors,
                'total_mappings': len(self.mappings)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Erro ao processar CSV: {str(e)}",
                'imported': 0
            }
    
    def export_to_csv(self) -> str:
        """
        Exporta mapeamentos para formato CSV.
        
        Returns:
            String com conteúdo CSV
        """
        df = self.get_mappings_dataframe()
        
        # Renomear colunas para export
        df_export = df.copy()
        df_export.columns = ['codigo_ofx', 'codigo_sistema', 'nome_banco', 'ativo', 'origem']
        
        return df_export.to_csv(index=False, encoding='utf-8')
    
    def _normalize_code(self, code: str) -> str:
        """
        Normaliza código para formato padrão.
        
        Args:
            code (str): Código a ser normalizado
            
        Returns:
            Código normalizado
        """
        if not code:
            return ""
        
        # Remover espaços e converter para string
        normalized = str(code).strip()
        
        # Para códigos de 4 dígitos que começam com 0, manter como está
        # Para códigos menores que 3 dígitos, preencher com zeros à esquerda até 3
        if len(normalized) < 3:
            normalized = normalized.zfill(3)
        
        return normalized
    
    def clear_all_mappings(self) -> bool:
        """
        Remove todos os mapeamentos.
        
        Returns:
            True se limpou com sucesso
        """
        try:
            count = len(self.mappings)
            self.mappings = {}
            logger.info(f"🧹 {count} mapeamentos removidos")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao limpar mapeamentos: {str(e)}")
            return False