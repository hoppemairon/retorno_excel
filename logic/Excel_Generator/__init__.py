"""
Módulo Excel_Generator
Responsável pela geração de arquivos Excel em formatos específicos para importação.

Inclui:
- Geração de Excel para lançamentos financeiros
- Geração de Excel para transferências entre empresas/bancos
- Templates customizados por tipo de operação
"""

__version__ = "1.0.0"
__author__ = "Mairo"

# Importações principais do módulo
from .lancamentos_excel import LancamentosExcelGenerator
from .transferencias_excel import TransferenciasExcelGenerator

__all__ = [
    "LancamentosExcelGenerator", 
    "TransferenciasExcelGenerator"
]