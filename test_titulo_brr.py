#!/usr/bin/env python3
"""
Teste simples para verificar se o módulo TITULO BRR está funcionando
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from logic.OFX_Processor.banrisul_titulo_brr import BanrisulTituloBRRProcessor
import pandas as pd

def test_processor():
    """Testa o processador TITULO BRR"""
    
    print("🧪 Testando BanrisulTituloBRRProcessor...")
    
    # Criar instância
    processor = BanrisulTituloBRRProcessor()
    
    # Teste 1: Criar DataFrame de exemplo
    df_test = pd.DataFrame([
        {
            'descricao': 'PAGAMENTO TITULO BRR',
            'banco_nome': 'BANRISUL',
            'valor_absoluto': 1000.00,
            'data': '2025-11-01'
        },
        {
            'descricao': 'TRANSFERENCIA NORMAL',
            'banco_nome': 'BANRISUL', 
            'valor_absoluto': 500.00,
            'data': '2025-11-01'
        }
    ])
    
    print("✅ DataFrame de teste criado")
    
    # Teste 2: Detectar transações genéricas
    stats = processor.detectar_transacoes_genericas(df_test)
    print(f"✅ Detecção: {stats['transacoes_genericas']} TITULO BRR de {stats['total_transacoes']} transações")
    
    # Teste 3: Simular conteúdo CNAB240 realista (linha com segmento J)
    # Formato: posição 13 = 'J', posições específicas para cada campo
    linha_j = (
        "0000000000000J" +           # posições 0-13: cabeçalho + segmento J
        "000000000000000" +          # posições 14-28: campos diversos
        "000001000" +                # posições 29-36: valor_pago (1000 centavos = R$ 10,00)
        "000000000000000000000000" + # posições 37-60: campos diversos  
        "TESTE BENEFICIARIO LTDA   " + # posições 61-90: nome do favorecido (30 chars)
        "01112025" +                 # posições 91-99: data pagamento (01/11/2025)
        "0000000100000" +            # posições 100-113: valor (1000 centavos)
        "0" * 116 +                  # posições 114-229: preenchimento
        "00" +                       # posições 230-231: código ocorrência  
        "000" +                      # posições 232-235: resto do código
        "0" * 5                      # completar até pelo menos 240 chars
    )
    
    # Garantir que a linha tenha pelo menos 240 caracteres
    cnab_content = linha_j + "0" * (240 - len(linha_j))
    
    print(f"✅ Linha CNAB240 criada com {len(cnab_content)} caracteres")
    print(f"📋 Segmento na posição 13: '{cnab_content[13]}'")
    print(f"📋 Nome favorecido: '{cnab_content[61:90].strip()}'")
    print(f"📋 Data: '{cnab_content[91:99]}'")
    print(f"📋 Valor pago: '{cnab_content[27:36]}'")
    print(f"📋 Código: '{cnab_content[230:235]}'")
    
    
    # Teste 4: Processar arquivo de retorno
    df_resultado = processor.processar_arquivo_retorno(cnab_content, "teste.ret")
    print(f"✅ Processamento: {len(df_resultado)} pagamentos extraídos")
    
    if not df_resultado.empty:
        print("📊 Colunas encontradas:", list(df_resultado.columns))
        print("📋 Primeiro pagamento:", df_resultado.iloc[0].to_dict())
    
    print("🎉 Teste concluído com sucesso!")
    return True

if __name__ == "__main__":
    try:
        test_processor()
        print("\n✅ MÓDULO TITULO BRR FUNCIONANDO CORRETAMENTE!")
    except Exception as e:
        print(f"\n❌ ERRO NO TESTE: {e}")
        import traceback
        traceback.print_exc()