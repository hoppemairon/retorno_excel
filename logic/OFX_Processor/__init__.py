"""
Módulo OFX_Processor
Responsável pelo processamento de arquivos OFX para gestão financeira.

Inclui:
- Leitura e parsing de arquivos OFX
- Mapeamento de bancos (De/Para)
- Detecção de duplicatas
- Identificação de transferências entre empresas e bancos
"""

__version__ = "1.0.0"
__author__ = "Mairo"

# Importações principais do módulo
from .ofx_reader import OFXReader
from .bank_mapper import BankMapper
from .manual_bank_mapper import ManualBankMapper
from .duplicate_checker import DuplicateChecker

__all__ = [
    "OFXReader",
    "BankMapper",
    "ManualBankMapper",
    "DuplicateChecker"
]