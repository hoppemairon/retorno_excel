"""
Gestor Financeiro - Integração OFX
Nova funcionalidade para processamento de arquivos OFX mantendo o sistema CNAB intacto.
"""

import streamlit as st
import pandas as pd
import io
import unidecode
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv
import openpyxl

# Importar módulos novos (OFX)
from logic.OFX_Processor.ofx_reader import OFXReader
from logic.OFX_Processor.bank_mapper import BankMapper
from logic.OFX_Processor.duplicate_checker import DuplicateChecker
from logic.OFX_Processor.banrisul_titulo_brr import BanrisulTituloBRRProcessor, detectar_e_processar_titulo_brr
from logic.Excel_Generator.lancamentos_excel import LancamentosExcelGenerator
from logic.Excel_Generator.transferencias_excel import TransferenciasExcelGenerator

# Importar módulo existente (API MR)
from logic.Sistema_MR.API_MR import buscar_lancamentos_api, buscar_nomes_empresas, validar_transacoes_contra_api, buscar_lancamentos_e_transferencias_api, buscar_categorias_api, validar_transacoes_robusta

# Importar classificador de categorias
from logic.ML.classificador_categorias import ClassificadorCategorias, treinar_classificador_com_dados_mr, sugerir_categorias_para_transacoes

load_dotenv()

# Código de ocorrências CNAB240 (copiado do Retorno_Excel.py)
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
    "AG": "Agência/conta corrente/Dv inválido",
    "AH": "Número seqüencial do registro do lote inválido",
    "AI": "Código do Segmento de Detalhe inválido",
    "AJ": "Tipo de movimento inválido",
    "AK": "Código da câmara de compensação do favorecido inválido",
    "AL": "Código do Banco Favorecido, Instituição de Pagamento ou Depositário Inválido",
    "AM": "Agência mantenedora da conta corrente do favorecido inválida",
    "AN": "Conta Corrente/DV/Conta de Pagamento do Favorecido Inválido",
    "AO": "Nome do favorecido não informado",
    "AP": "Data do lançamento inválida",
    "AQ": "Tipo/quantidade de moeda inválido",
    "AR": "Valor do lançamento inválido",
    "AS": "Aviso ao favorecido - Identificação inválida",
    "AT": "Tipo/número de inscrição do favorecido inválido",
    "AU": "Logradouro do favorecido não informado",
    "AV": "Número do local do favorecido não informado",
    "AW": "Cidade do favorecido não informado",
    "AX": "Cep/complemento do favorecido inválido",
    "AY": "Sigla do estado do favorecido inválida",
    "AZ": "Código/nome do banco depositário inválido",
    "BA": "Código/nome da agência depositária não informado",
    "BB": "Seu número inválido",
    "BC": "Nosso número inválido",
    "BD": "Confirmação de pagamento agendado",
    "BE": "Código do pagamento inválido",
    "BF": "Período de competência inválido",
    "BG": "Mês de competência inválido",
    "BH": "Ano de competência inválido",
    "BI": "Competência 13 não pode ser antecipada",
    "BJ": "Identificador de pagamento inválido",
    "BK": "Valor da multa inválido",
    "BL": "Valor mínimo de GPS - R$10,00",
    "BM": "Código de Operação para o sistema BLV inválido",
    "BN": "STR006 ou TED fora do horário",
    "BO": "Pagamento em agência do mesmo estado do favorecido",
    "BP": "Erro na validação do código de barras",
    "BQ": "Inconsistência do código de barras da GPS",
    "CC": "Dígito verificador geral inválido",
    "CF": "Valor do Documento Inválido",
    "CI": "Valor de Mora Inválido",
    "CJ": "Valor da Multa Inválido",
    "DD": "Duplicidade de DOC",
    "DT": "Duplicidade de Título",
    "TA": "Lote não aceito - totais de lote com diferença.",
    "XA": "TED Agendada cancelada pelo Piloto.",
    "XC": "TED cancelada pelo Piloto.",
    "XD": "Devolução do SPB.",
    "XE": "Devolução do SPB por erro.",
    "XP": "Devolução do SPB por situação especial.",
    "XR": "Movimento entre contas inválido.",
    "YA": "Título não encontrado.",
    "ZA": "Agência / Conta do Favorecido substituído.",
    "ZI": "Beneficiário divergente",
    "57": "Divergência na indicação da agência, conta corrente, nome ou CNPJ/CPF do favorecido."
}

def ler_cnab240_segmento_j(conteudo_arquivo):
    """
    Lê arquivo CNAB240 e extrai registros do Segmento J (pagamentos)
    Função copiada do Retorno_Excel.py para manter compatibilidade
    """
    registros = []

    for linha in conteudo_arquivo.splitlines():
        if len(linha) >= 150 and linha[13] == 'J':
            nome_favorecido = linha[61:90].strip()
            data_pagamento = linha[91:100]
            valor = linha[101:114].strip()
            valor_pago = linha[27:36].strip()
            codigo_pagamento = linha[230:235].strip()
            descricao_confirmacao = codigo_ocorrencias.get(codigo_pagamento, codigo_pagamento)

            if "0" in nome_favorecido:
                nome_favorecido = nome_favorecido.replace("0", "")

            try:
                data_formatada = f"{data_pagamento[0:2]}/{data_pagamento[2:4]}/{data_pagamento[4:8]}"
                valor_formatado = int(valor) / 100 if valor.isdigit() else 0
            except:
                data_formatada = ""
                valor_formatado = 0

            registros.append({
                'Favorecido': nome_favorecido,
                'Data Pagamento': data_formatada,
                'Valor Pago (R$)': f"{int(valor_pago) / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                'Codigo':  codigo_pagamento,
                'Descrição': descricao_confirmacao
            })

    return pd.DataFrame(registros)

# Constante de empresas MR (usada em várias partes do sistema)
EMPRESAS_MR = {
    "GRUPO ROTA - ARARANGUA": "772644ba-3a49-4736-8443-f057581d6b39",
    "GRUPO ROTA - TERRA DE AREIA": "4d49850f-ebf1-433d-a32a-527b54e856aa",
    "GRUPO ROTA - CAMINHO DO SOL": "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401",
    "GRUPO ROTA - JAGUARUNA": "149653c2-f107-4c60-aad0-b034789c8504",
    "GRUPO ROTA - PARADOURO": "735b6b4e-5513-4bb5-a9c4-50d92462921d",
    "GRUPO ROTA - SÃO PAULO": "1db3be97-a6d6-484a-b75b-fc1bdc6c487a",
    "GRUPO ROTA - ELDORADO": "93f44c44-bfd4-417f-bad2-20933e5c0228",
    "GRUPO ROTA - PINHEIRO MACHADO": "a13229ca-0f8a-442a-91ab-27e0adc1810b",
    "GRUPO ROTA - SEBERI": "eb84222f-2e6b-4f68-8457-760d10e24043",
    "GRUPO ROTA - POA IPIRANGA": "85d3091d-af31-4cb5-86fc-1558aaefa19b",
    "GRUPO ROTA - CRISTAL": "7a078786-1d9e-4433-9d63-8dfc58130b5f",
    "GRUPO ROTA - PORTO ALEGRE": "73a32cc3-d7ac-48d7-91d7-9046045d0bd7",
    "GRUPO ROTA - PARADOURO REST.": "cad79622-124a-4dc0-9408-7da5227576f0",
    "GRUPO ROTA - TRANSPORTADORA": "3885ddf8-f0ac-4468-98ab-97a248e29150"
}

st.set_page_config(page_title="Gestor Financeiro - Retorno CNAB & OFX", layout="wide")

def main():
    st.title("💰 Gestor Financeiro - CNAB & OFX")
    st.markdown("Sistema integrado para processamento de arquivos financeiros")
    
    # Criar abas principais
    tab1, tab2 = st.tabs([
        "📄 CNAB240 (.RET)", 
        "🏦 Gestão Financeira (OFX)"
    ])
    
    with tab1:
        render_cnab_tab()
    
    with tab2:
        render_ofx_tab()

def render_cnab_tab():
    """Renderiza a aba CNAB - código original mantido intacto"""
    
    st.markdown("Faça o upload de um arquivo `.RET` (CNAB240) para extrair os dados de **pagamentos (Segmento J)** e gerar um arquivo Excel.")

    # Estado da sessão para CNAB
    if "df_ret" not in st.session_state:
        st.session_state.df_ret = None
    if "uploader_key_ret" not in st.session_state:
        st.session_state.uploader_key_ret = 0

    # Códigos de ocorrência (mantido do código original)
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
        "AG": "Agência/conta corrente/Dv inválido",
        "AH": "Número seqüencial do registro do lote inválido",
        "AI": "Código do Segmento de Detalhe inválido",
        "AJ": "Tipo de movimento inválido",
        "AK": "Código da câmara de compensação do favorecido inválido",
        "AL": "Código do Banco Favorecido, Instituição de Pagamento ou Depositário Inválido",
        "AM": "Agência mantenedora da conta corrente do favorecido inválida",
        "AN": "Conta Corrente/DV/Conta de Pagamento do Favorecido Inválido",
        "AO": "Nome do favorecido não informado",
        "AP": "Data do lançamento inválida",
        "AQ": "Tipo/quantidade de moeda inválido",
        "AR": "Valor do lançamento inválido",
        "AS": "Aviso ao favorecido - Identificação inválida",
        "AT": "Tipo/número de inscrição do favorecido inválido",
        "AU": "Logradouro do favorecido não informado",
        "AV": "Número do local do favorecido não informado",
        "AW": "Cidade do favorecido não informado",
        "AX": "Cep/complemento do favorecido inválido",
        "AY": "Sigla do estado do favorecido inválida",
        "AZ": "Código/nome do banco depositário inválido",
        "BA": "Código/nome da agência depositária não informado",
        "BB": "Seu número inválido",
        "BC": "Nosso número inválido",
        "BD": "Confirmação de pagamento agendado",
        "BE": "Código do pagamento inválido",
        "BF": "Período de competência inválido",
        "BG": "Mês de competência inválido",
        "BH": "Ano de competência inválido",
        "BI": "Competência 13 não pode ser antecipada",
        "BJ": "Identificador de pagamento inválido",
        "BK": "Valor da multa inválido",
        "BL": "Valor mínimo de GPS - R$10,00",
        "BM": "Código de Operação para o sistema BLV inválido",
        "BN": "STR006 ou TED fora do horário",
        "BO": "Pagamento em agência do mesmo estado do favorecido",
        "BP": "Erro na validação do código de barras",
        "BQ": "Inconsistência do código de barras da GPS",
        "CC": "Dígito verificador geral inválido",
        "CF": "Valor do Documento Inválido",
        "CI": "Valor de Mora Inválido",
        "CJ": "Valor da Multa Inválido",
        "DD": "Duplicidade de DOC",
        "DT": "Duplicidade de Título",
        "TA": "Lote não aceito - totais de lote com diferença.",
        "XA": "TED Agendada cancelada pelo Piloto.",
        "XC": "TED cancelada pelo Piloto.",
        "XD": "Devolução do SPB.",
        "XE": "Devolução do SPB por erro.",
        "XP": "Devolução do SPB por situação especial.",
        "XR": "Movimento entre contas inválido.",
        "YA": "Título não encontrado.",
        "ZA": "Agência / Conta do Favorecido substituído.",
        "ZI": "Beneficiário divergente",
        "57": "Divergência na indicação da agência, conta corrente, nome ou CNPJ/CPF do favorecido."
    }

    def ler_cnab240_segmento_j(conteudo_arquivo):
        registros = []

        for linha in conteudo_arquivo.splitlines():
            if len(linha) >= 150 and linha[13] == 'J':
                nome_favorecido = linha[61:90].strip()
                data_pagamento = linha[91:100]
                valor = linha[101:114].strip()
                valor_pago = linha[27:36].strip()
                codigo_pagamento = linha[230:235].strip()
                descricao_confirmacao = codigo_ocorrencias.get(codigo_pagamento, codigo_pagamento)

                if "0" in nome_favorecido:
                    nome_favorecido = nome_favorecido.replace("0", "")

                try:
                    data_formatada = f"{data_pagamento[0:2]}/{data_pagamento[2:4]}/{data_pagamento[4:8]}"
                    valor_formatado = int(valor) / 100 if valor.isdigit() else 0
                except:
                    data_formatada = ""
                    valor_formatado = 0

                registros.append({
                    'Favorecido': nome_favorecido,
                    'Data Pagamento': data_formatada,
                    'Valor Pago (R$)': f"{int(valor_pago) / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                    'Codigo':  codigo_pagamento,
                    'Descrição': descricao_confirmacao
                })

        return pd.DataFrame(registros)

    # Upload CNAB
    uploaded_file = st.file_uploader(
        "📁 Envie o arquivo .RET aqui",
        type=["ret", "txt"],
        key=f"uploader_ret_{st.session_state.uploader_key_ret}"
    )

    # Processamento CNAB
    if uploaded_file:
        conteudo = uploaded_file.read().decode("utf-8", errors="ignore")
        df = ler_cnab240_segmento_j(conteudo)

        if not df.empty:
            st.success(f"{len(df)} pagamentos encontrados.")
            st.session_state.df_ret = df
        else:
            st.warning("❌ Nenhum pagamento (Segmento J) foi encontrado neste arquivo.")

    # Exibição CNAB
    if st.session_state.df_ret is not None:
        st.dataframe(st.session_state.df_ret, width="stretch")

        output = io.BytesIO()
        st.session_state.df_ret.to_excel(output, index=False)
        output.seek(0)

        st.download_button(
            label="📥 Baixar Excel",
            data=output,
            file_name="pagamentos_cnab240.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Análise com API MR (código original mantido)
        with st.expander("🔍 Analisar com dados da API MR"):
            render_cnab_mr_analysis()

    # Botão limpar CNAB
    if st.session_state.df_ret is not None:
        if st.button("🧹 Limpar Tela CNAB"):
            st.session_state.df_ret = None
            st.session_state.uploader_key_ret += 1
            st.rerun()

def render_cnab_mr_analysis():
    """Renderiza análise MR para CNAB - código original mantido"""
    
    EMPRESAS_MR = {
        "GRUPO ROTA - ARARANGUA": "772644ba-3a49-4736-8443-f057581d6b39",
        "GRUPO ROTA - TERRA DE AREIA": "4d49850f-ebf1-433d-a32a-527b54e856aa",
        "GRUPO ROTA - CAMINHO DO SOL": "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401",
        "GRUPO ROTA - JAGUARUNA": "149653c2-f107-4c60-aad0-b034789c8504",
        "GRUPO ROTA - PARADOURO": "735b6b4e-5513-4bb5-a9c4-50d92462921d",
        "GRUPO ROTA - SÃO PAULO": "1db3be97-a6d6-484a-b75b-fc1bdc6c487a",
        "GRUPO ROTA - ELDORADO": "93f44c44-bfd4-417f-bad2-20933e5c0228",
        "GRUPO ROTA - PINHEIRO MACHADO": "a13229ca-0f8a-442a-91ab-27e0adc1810b",
        "GRUPO ROTA - SEBERI": "eb84222f-2e6b-4f68-8457-760d10e24043",
        "GRUPO ROTA - POA IPIRANGA": "85d3091d-af31-4cb5-86fc-1558aaefa19b",
        "GRUPO ROTA - CRISTAL": "7a078786-1d9e-4433-9d63-8dfc58130b5f",
        "GRUPO ROTA - PORTO ALEGRE": "73a32cc3-d7ac-48d7-91d7-9046045d0bd7",
        "GRUPO ROTA - PARADOURO REST.": "cad79622-124a-4dc0-9408-7da5227576f0",
        "GRUPO ROTA - TRANSPORTADORA": "3885ddf8-f0ac-4468-98ab-97a248e29150"
    }

    empresa_nome = st.selectbox("Selecione a empresa (MR):", list(EMPRESAS_MR.keys()))
    id_empresa = EMPRESAS_MR[empresa_nome]

    if st.button("🔄 Buscar dados da MR"):
        df_api_mr = buscar_lancamentos_api(ids_empresa=id_empresa, anos="2025")

        if df_api_mr.empty or "data" not in df_api_mr.columns or "valor" not in df_api_mr.columns:
            st.warning("⚠️ Nenhum dado útil retornado da API da MR ou estrutura inesperada.")
        else:
            st.success(f"{len(df_api_mr)} registros carregados da MR para a empresa selecionada.")

            # Cruzamento de dados (código original mantido)
            df_ret = st.session_state.df_ret.copy()
            df_ret = df_ret[df_ret["Codigo"] == "BD"]
            df_ret["Data"] = pd.to_datetime(df_ret["Data Pagamento"], dayfirst=True, errors="coerce").dt.date
            df_ret["Valor Pago (R$)"] = df_ret["Valor Pago (R$)"].str.replace(".", "", regex=False).str.replace(",", ".").astype(float)

            df_api_mr["data"] = pd.to_datetime(df_api_mr["data"], errors="coerce").dt.date
            df_api_mr["valor"] = pd.to_numeric(df_api_mr["valor"], errors="coerce")

            st.subheader("🧠 Cruzamento por Nome (Favorecido vs. Contato MR)")

            def normalizar_nome(texto):
                if not isinstance(texto, str):
                    return ""
                return unidecode.unidecode(texto).lower().strip()

            df_ret["nome_norm"] = df_ret["Favorecido"].apply(normalizar_nome)
            df_api_mr["contato_norm"] = df_api_mr["contato"].apply(normalizar_nome)

            resultados = []
            for _, linha_ret in df_ret.iterrows():
                possiveis = df_api_mr[df_api_mr["contato_norm"].str.contains(linha_ret["nome_norm"], na=False)]
                if not possiveis.empty:
                    linha_mr = possiveis.iloc[0]
                    resultados.append({
                        "Data": linha_ret["Data"].strftime("%d/%m/%Y"),
                        "Descrição": linha_ret["Favorecido"],
                        "Valor": linha_ret["Valor Pago (R$)"],
                        "Contato": linha_mr["contato"],
                        "Categoria": linha_mr.get("categoria", "").split("-", 1)[-1].strip() if linha_mr.get("categoria") else ""
                    })
                else:
                    resultados.append({
                        "Data": linha_ret["Data"].strftime("%d/%m/%Y"),
                        "Descrição": linha_ret["Favorecido"],
                        "Valor": linha_ret["Valor Pago (R$)"],
                        "Contato": "",
                        "Categoria": ""
                    })

            df_fuzzy = pd.DataFrame(resultados)
            st.dataframe(df_fuzzy, width="stretch")

            output_fuzzy = io.BytesIO()
            df_fuzzy.to_excel(output_fuzzy, index=False)
            output_fuzzy.seek(0)

            st.download_button(
                label="📥 Baixar Excel cruzado por nome",
                data=output_fuzzy,
                file_name="pagamentos_cruzados_por_nome.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

def render_ofx_tab():
    """Renderiza a nova aba OFX - Fase 1"""
    
    st.markdown("### 🚀 Nova Funcionalidade: Gestão Financeira OFX")
    st.info("**Atualizado**: Integração com API de contas correntes implementada!")
    
    # Estados da sessão para OFX
    if "df_ofx" not in st.session_state:
        st.session_state.df_ofx = None
    if "ofx_summary" not in st.session_state:
        st.session_state.ofx_summary = None
    if "uploader_key_ofx" not in st.session_state:
        st.session_state.uploader_key_ofx = 0
    if "api_mapping_loaded" not in st.session_state:
        st.session_state.api_mapping_loaded = False
    
    # Instanciar classes
    ofx_reader = OFXReader()
    bank_mapper = BankMapper()
    
    # Seção de configuração da API
    with st.expander("⚙️ Configurar Mapeamento via API MR", expanded=not st.session_state.api_mapping_loaded):
        st.markdown("Configure o mapeamento de bancos usando as contas correntes do sistema MR:")
        
        # Lista de empresas do Grupo Rota - Nomes otimizados para visualização
        EMPRESAS_MR = {
            "🏢 ROTA - Araranguá": "772644ba-3a49-4736-8443-f057581d6b39",
            "🏢 ROTA - Terra de Areia": "4d49850f-ebf1-433d-a32a-527b54e856aa",
            "🏢 ROTA - Caminho do Sol": "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401",
            "🏢 ROTA - Candiota": "b2107e4f-59a7-44a0-9a84-d81abaab5ad2",
            "🏢 ROTA - Jaguaruna": "149653c2-f107-4c60-aad0-b034789c8504",
            "🏢 ROTA - Paradouro": "735b6b4e-5513-4bb5-a9c4-50d92462921d",
            "🏢 ROTA - São Paulo": "1db3be97-a6d6-484a-b75b-fc1bdc6c487a",
            "🏢 ROTA - Eldorado": "93f44c44-bfd4-417f-bad2-20933e5c0228",
            "🏢 ROTA - Pinheiro Machado": "a13229ca-0f8a-442a-91ab-27e0adc1810b",
            "🏢 ROTA - Seberi": "eb84222f-2e6b-4f68-8457-760d10e24043",
            "🏢 ROTA - POA Ipiranga": "85d3091d-af31-4cb5-86fc-1558aaefa19b",
            "🏢 ROTA - Cristal": "7a078786-1d9e-4433-9d63-8dfc58130b5f",
            "🏢 ROTA - Porto Alegre": "73a32cc3-d7ac-48d7-91d7-9046045d0bd7",
            "🏢 ROTA - Paradouro Rest.": "cad79622-124a-4dc0-9408-7da5227576f0",
            "🏢 ROTA - Transportadora": "3885ddf8-f0ac-4468-98ab-97a248e29150"
        }
        
        # Seleção de empresas - usando container expandido
        with st.container():
            st.subheader("📋 Seleção de Empresas")
            empresas_selecionadas = st.multiselect(
                "Selecione as empresas para carregar contas correntes:",
                options=list(EMPRESAS_MR.keys()),
                default=list(EMPRESAS_MR.keys())[:1],  # Primeira 1 por padrão
                help="Selecione uma ou mais empresas do grupo para configurar as contas correntes"
            )
        
        col_api1, col_api2 = st.columns(2)
        
        with col_api1:
            if st.button("🔄 Carregar Mapeamento da API", type="primary"):
                if not empresas_selecionadas:
                    st.warning("Selecione pelo menos uma empresa!")
                else:
                    with st.spinner("Carregando mapeamento da API..."):
                        # Converter nomes para IDs
                        clientes_ids = [EMPRESAS_MR[empresa] for empresa in empresas_selecionadas]
                        
                        # Salvar empresas selecionadas no estado da sessão para uso posterior
                        st.session_state.empresas_selecionadas_ids = clientes_ids
                        st.session_state.empresas_selecionadas_nomes = empresas_selecionadas
                        st.session_state.empresas_api_selecionadas = empresas_selecionadas  # Para usar na leitura OFX
                        
                        # Carregar mapeamento
                        success = bank_mapper.load_mapping_from_api(clientes_ids)
                        
                        if success:
                            st.session_state.api_mapping_loaded = True
                            st.success(f"✅ Mapeamento carregado para {len(empresas_selecionadas)} empresas!")
                            
                            # Mostrar estatísticas do mapeamento
                            stats = bank_mapper.get_mapping_statistics()
                            st.info(f"📊 {stats['total_bancos']} bancos mapeados da API MR")
                        else:
                            st.error("❌ Falha ao carregar mapeamento da API")
        
        with col_api2:
            if st.button("🧹 Limpar Mapeamento"):
                st.session_state.api_mapping_loaded = False
                st.session_state.empresas_selecionadas_ids = []
                st.session_state.empresas_selecionadas_nomes = []
                st.session_state.empresas_api_selecionadas = []  # Limpar também as empresas para OFX
                st.info("🗑️ Mapeamento limpo - selecione empresas novamente")
        
        # Status do mapeamento
        if st.session_state.api_mapping_loaded:
            st.success("🔗 Usando mapeamento da API MR")
        else:
            st.warning("⚠️ Carregue o mapeamento da API para continuar")
    
    # Seção de Diagnóstico da API
    with st.expander("🔍 Diagnóstico da API", expanded=False):
        st.markdown("**Status da conexão e mapeamento de bancos:**")
        
        # Obter status do mapeamento
        mapping_status = bank_mapper.get_mapping_status()
        
        col_diag1, col_diag2, col_diag3 = st.columns(3)
        
        with col_diag1:
            if mapping_status['api_connection_valid']:
                st.success("🟢 Conexão API Válida")
            else:
                st.error("🔴 API Desconectada")
        
        with col_diag2:
            if mapping_status['using_api']:
                st.success(f"🟢 API Ativa ({mapping_status['api_banks_count']} bancos)")
            else:
                st.warning("🟡 API Não Carregada")
        
        with col_diag3:
            st.info(f"📊 Total: {mapping_status['total_available_banks']} bancos")
        
        # Tabela de bancos disponíveis
        if st.button("📋 Ver Bancos Disponíveis"):
            banks_summary = bank_mapper.get_available_banks_summary()
            if banks_summary:
                df_banks = pd.DataFrame(banks_summary)
                st.dataframe(df_banks, width="stretch", hide_index=True)
            else:
                st.warning("Nenhum banco disponível no mapeamento")
    
    st.markdown("Faça o upload de um ou múltiplos arquivos `.OFX` para processar transações financeiras.")
    
    # Verificar se API foi carregada antes de permitir upload
    if not st.session_state.api_mapping_loaded:
        st.warning("⚠️ **Carregue o mapeamento da API primeiro** para poder processar arquivos OFX.")
        st.info("👆 Use a seção 'Configurar Mapeamento via API MR' acima para carregar as contas das empresas.")
        return  # Não permite continuar sem API
    
    # Upload de múltiplos arquivos OFX
    uploaded_files = st.file_uploader(
        "📁 Envie arquivos OFX aqui",
        type=["ofx", "qfx"],
        accept_multiple_files=True,
        key=f"uploader_ofx_{st.session_state.uploader_key_ofx}"
    )
    
    if uploaded_files:
        st.info(f"📄 {len(uploaded_files)} arquivo(s) carregado(s)")
        
        # Seção de mapeamento individual por arquivo
        st.markdown("---")
        st.subheader("🔗 Mapeamento individual dos arquivos")
        st.markdown("**Selecione qual banco do sistema cada arquivo OFX representa:**")
        
        # Obter bancos disponíveis do sistema MR baseado nas empresas selecionadas
        empresas_selecionadas_ids = st.session_state.get('empresas_selecionadas_ids', [])
        
        if not empresas_selecionadas_ids:
            st.warning("⚠️ Carregue o mapeamento da API primeiro para selecionar empresas específicas.")
            # Usar empresas padrão
            empresas_selecionadas_ids = []
        else:
            # Mostrar quais empresas estão sendo usadas
            empresas_nomes = st.session_state.get('empresas_selecionadas_nomes', [])
            if empresas_nomes:
                st.info(f"📋 Usando bancos das empresas: {', '.join(empresas_nomes)}")
        
        bancos_sistema = get_available_system_banks(bank_mapper, empresas_selecionadas_ids)
        
        if not bancos_sistema:
            st.error("❌ Nenhum banco encontrado no sistema MR. Verifique a conexão com a API.")
            return
        
        # Estados para armazenar mapeamentos
        if "file_bank_mappings" not in st.session_state:
            st.session_state.file_bank_mappings = {}
        
        # Interface para mapear cada arquivo
        file_mappings = {}
        all_mapped = True
        
        for i, file in enumerate(uploaded_files):
            # Extrair informações bancárias do arquivo OFX
            ofx_info = extract_bank_info_from_ofx(file)
            
            col_file, col_bank = st.columns([1, 2])
            
            with col_file:
                st.markdown(f"**📄 {file.name}**")
                st.caption(f"Tamanho: {len(file.getvalue()):,} bytes")
                
                # Mostrar informações bancárias do OFX
                if ofx_info:
                    st.markdown("**Dados do OFX:**")
                    if ofx_info.get('banco'):
                        st.write(f"🏦 Banco: **{ofx_info['banco']}**")
                    if ofx_info.get('agencia'):
                        st.write(f"🏢 Agência: **{ofx_info['agencia']}**")
                    if ofx_info.get('conta'):
                        st.write(f"💳 Conta: **{ofx_info['conta']}**")
                    if ofx_info.get('periodo'):
                        st.write(f"📅 Período: **{ofx_info['periodo']}**")
                else:
                    st.caption("ℹ️ Não foi possível extrair dados bancários")
            
            with col_bank:
                # Criar chave única para este arquivo
                file_key = f"{file.name}_{len(file.getvalue())}"
                
                # Opções de bancos para seleção
                banco_options = ["-- Selecione o banco --"] + [
                    banco['nome'] for banco in bancos_sistema
                ]
                
                # Valor padrão se já foi mapeado anteriormente
                default_index = 0
                if file_key in st.session_state.file_bank_mappings:
                    saved_mapping = st.session_state.file_bank_mappings[file_key]
                    for idx, option in enumerate(banco_options[1:], 1):
                        if saved_mapping['codigo'] in option or saved_mapping['nome'] in option:
                            default_index = idx
                            break
                
                selected_bank = st.selectbox(
                    f"Banco do sistema para {file.name}:",
                    options=banco_options,
                    index=default_index,
                    key=f"bank_select_{i}_{st.session_state.uploader_key_ofx}",
                    help="Selecione qual conta do seu sistema este arquivo OFX representa"
                )
                
                if selected_bank != "-- Selecione o banco --":
                    # Encontrar o banco selecionado na lista
                    banco_selecionado = next(
                        (banco for banco in bancos_sistema if banco['nome'] == selected_bank),
                        None
                    )
                    
                    if banco_selecionado:
                        # Salvar mapeamento
                        file_mappings[file_key] = {
                            'file': file,
                            'codigo': banco_selecionado['codigo'],
                            'nome': banco_selecionado['nome'],
                            'empresa': banco_selecionado.get('empresa', 'N/A'),
                            'banco': banco_selecionado.get('banco', 'N/A'),
                            'banco_api_original': banco_selecionado.get('banco_api_original', banco_selecionado.get('banco', 'N/A')),
                            'ofx_info': ofx_info
                        }
                        
                        # Salvar no estado da sessão para reutilização
                        st.session_state.file_bank_mappings[file_key] = {
                            'codigo': banco_selecionado['codigo'],
                            'nome': banco_selecionado['nome']
                        }
                        
                        # Mostrar resumo do mapeamento
                        st.success(f"✅ Mapeado para: **{banco_selecionado['empresa']}** - **{banco_selecionado['banco']}**")
                else:
                    all_mapped = False
        
        # Mostrar resumo dos mapeamentos
        if file_mappings:
            st.markdown("---")
            st.markdown("**📋 Resumo dos mapeamentos:**")
            
            for file_key, mapping in file_mappings.items():
                with st.expander(f"📄 {mapping['file'].name}", expanded=False):
                    col_ofx, col_arrow, col_sistema = st.columns([2, 0.5, 2])
                    
                    with col_ofx:
                        st.markdown("**📁 Dados do OFX:**")
                        if mapping.get('ofx_info'):
                            ofx_info = mapping['ofx_info']
                            if ofx_info.get('banco'):
                                st.write(f"🏦 **{ofx_info['banco']}**")
                            if ofx_info.get('agencia'):
                                st.write(f"🏢 Ag: {ofx_info['agencia']}")
                            if ofx_info.get('conta'):
                                st.write(f"💳 Conta: {ofx_info['conta']}")
                            if ofx_info.get('tipo_conta'):
                                st.write(f"📋 Tipo: {ofx_info['tipo_conta']}")
                        else:
                            st.write("ℹ️ Dados não extraídos")
                    
                    with col_arrow:
                        st.markdown("")
                        st.markdown("")
                        st.markdown("**→**")
                    
                    with col_sistema:
                        st.markdown("**🏢 Sistema MR:**")
                        st.write(f"🏢 **{mapping.get('empresa', 'N/A')}**")
                        st.write(f"🏦 {mapping.get('banco', 'N/A')}")
                        st.caption(f"ID: {mapping['codigo']}")
        
        # Botão para processar (só aparece se todos os arquivos estiverem mapeados)
        if all_mapped and file_mappings:
            if st.button("🚀 Processar Arquivos com Mapeamento", type="primary"):
                process_ofx_files_with_mapping(file_mappings, ofx_reader, bank_mapper)
        elif uploaded_files:
            st.warning("⚠️ Mapeie todos os arquivos antes de processar!")
    
    # Exibir resultados
    if st.session_state.df_ofx is not None and not st.session_state.df_ofx.empty:
        render_ofx_results()
    
    # Botão limpar OFX
    if st.session_state.df_ofx is not None:
        if st.button("🧹 Limpar Dados OFX"):
            st.session_state.df_ofx = None
            st.session_state.ofx_summary = None
            st.session_state.uploader_key_ofx += 1
            st.rerun()

def extract_bank_info_from_ofx(file) -> dict:
    """
    Extrai informações bancárias de um arquivo OFX.
    
    Args:
        file: Arquivo OFX carregado
        
    Returns:
        Dict com informações bancárias extraídas
    """
    try:
        # Ler conteúdo do arquivo
        file.seek(0)  # Voltar ao início do arquivo
        content = file.read().decode("utf-8", errors="ignore")
        file.seek(0)  # Voltar ao início novamente para uso posterior
        
        info = {
            'banco': None,
            'agencia': None,
            'conta': None,
            'periodo': None,
            'tipo_conta': None
        }
        
        # Extrair código do banco
        import re
        
        # Procurar por BANKID (código do banco)
        bankid_match = re.search(r'<BANKID>([^<]+)', content)
        if bankid_match:
            bank_code = bankid_match.group(1).strip()
            # Mapear código para nome do banco
            bank_names = {
                '001': 'Banco do Brasil S.A.',
                '341': 'Banco Itaú Unibanco S.A.',
                '0341': 'Banco Itaú Unibanco S.A.',
                '237': 'Banco Bradesco S.A.',
                '033': 'Banco Santander Brasil S.A.',
                '104': 'Caixa Econômica Federal',
                '041': 'Banco do Estado do RS S.A.',
                '077': 'Banco Inter S.A.',
                '260': 'Nu Pagamentos S.A. (Nubank)',
                '756': 'Sicoob'
            }
            info['banco'] = bank_names.get(bank_code, f"Banco {bank_code}")
        
        # Procurar por BRANCHID (agência)
        branchid_match = re.search(r'<BRANCHID>([^<]+)', content)
        if branchid_match:
            info['agencia'] = branchid_match.group(1).strip()
        
        # Procurar por ACCTID (conta)
        acctid_match = re.search(r'<ACCTID>([^<]+)', content)
        if acctid_match:
            conta = acctid_match.group(1).strip()
            # Mostrar conta completa (sem mascarar)
            info['conta'] = conta
        
        # Procurar por tipo de conta
        accttype_match = re.search(r'<ACCTTYPE>([^<]+)', content)
        if accttype_match:
            acct_type = accttype_match.group(1).strip()
            type_mapping = {
                'CHECKING': 'Conta Corrente',
                'SAVINGS': 'Conta Poupança',
                'CREDITCARD': 'Cartão de Crédito',
                'INVESTMENT': 'Investimento'
            }
            info['tipo_conta'] = type_mapping.get(acct_type, acct_type)
        
        # Procurar por período (datas de início e fim)
        dtstart_match = re.search(r'<DTSTART>([^<]+)', content)
        dtend_match = re.search(r'<DTEND>([^<]+)', content)
        
        if dtstart_match and dtend_match:
            try:
                from datetime import datetime
                start_date = dtstart_match.group(1).strip()
                end_date = dtend_match.group(1).strip()
                
                # Converter formato YYYYMMDDHHMMSS para data legível
                if len(start_date) >= 8:
                    start_formatted = f"{start_date[6:8]}/{start_date[4:6]}/{start_date[:4]}"
                    end_formatted = f"{end_date[6:8]}/{end_date[4:6]}/{end_date[:4]}"
                    info['periodo'] = f"{start_formatted} a {end_formatted}"
            except:
                pass
        
        return info
        
    except Exception as e:
        # Se houver erro, retornar dict vazio
        return {}

def get_available_system_banks(bank_mapper, empresas_ids: list = None) -> list:
    """
    Obtém lista de bancos disponíveis no sistema MR com informações amigáveis.
    
    Args:
        bank_mapper: Instância do BankMapper
        empresas_ids: Lista de IDs das empresas selecionadas (opcional)
        
    Returns:
        Lista de dicts com código e nome amigável dos bancos    
    """
    try:
        # IDs das empresas - usar as selecionadas ou padrão
        if empresas_ids and len(empresas_ids) > 0:
            usar_empresas_ids = empresas_ids
        else:
            # Fallback para as empresas padrão
            usar_empresas_ids = [
                "772644ba-3a49-4736-8443-f057581d6b39",
                "4d49850f-ebf1-433d-a32a-527b54e856aa"
            ]
        
        # Buscar nomes reais das empresas via API
        try:
            empresas_nomes = buscar_nomes_empresas(usar_empresas_ids)
        except Exception as e:
            print(f"Erro ao buscar nomes das empresas: {e}")
            # Fallback para nomes reais das empresas (não genéricos)
            empresas_nomes = {
                "772644ba-3a49-4736-8443-f057581d6b39": "GRUPO ROTA - ARARANGUÁ",
                "4d49850f-ebf1-433d-a32a-527b54e856aa": "GRUPO ROTA - TERRA DE AREIA"
            }
        
        # Tentar obter bancos da API MR
        if hasattr(bank_mapper, 'contas_api') and bank_mapper.contas_api:
            df_contas = bank_mapper.contas_api.buscar_contas_multiplos_clientes(usar_empresas_ids)
            
            if not df_contas.empty:
                # Extrair bancos únicos das contas com informações amigáveis
                bancos = []
                contas_processadas = set()
                
                for _, conta in df_contas.iterrows():
                    nome_conta = str(conta.get('nome', '')).strip()
                    conta_id = str(conta.get('contaId', '')).strip()
                    cliente_id = str(conta.get('clienteId', '')).strip()
                    tipo_conta = conta.get('tipo', 1)
                    
                    # Obter nome real da empresa com fallback melhor
                    nome_empresa = empresas_nomes.get(cliente_id)
                    if not nome_empresa:
                        # Fallback usando o mapeamento completo de empresas
                        empresas_completo = {
                            "772644ba-3a49-4736-8443-f057581d6b39": "GRUPO ROTA - ARARANGUÁ",
                            "4d49850f-ebf1-433d-a32a-527b54e856aa": "GRUPO ROTA - TERRA DE AREIA",
                            "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401": "GRUPO ROTA - CAMINHO DO SOL",
                            "149653c2-f107-4c60-aad0-b034789c8504": "GRUPO ROTA - JAGUARUNA",
                            "735b6b4e-5513-4bb5-a9c4-50d92462921d": "GRUPO ROTA - PARADOURO",
                            "1db3be97-a6d6-484a-b75b-fc1bdc6c487a": "GRUPO ROTA - SÃO PAULO",
                            "93f44c44-bfd4-417f-bad2-20933e5c0228": "GRUPO ROTA - ELDORADO",
                            "a13229ca-0f8a-442a-91ab-27e0adc1810b": "GRUPO ROTA - PINHEIRO MACHADO",
                            "eb84222f-2e6b-4f68-8457-760d10e24043": "GRUPO ROTA - SEBERI",
                            "85d3091d-af31-4cb5-86fc-1558aaefa19b": "GRUPO ROTA - POA IPIRANGA",
                            "7a078786-1d9e-4433-9d63-8dfc58130b5f": "GRUPO ROTA - CRISTAL",
                            "73a32cc3-d7ac-48d7-91d7-9046045d0bd7": "GRUPO ROTA - PORTO ALEGRE",
                            "cad79622-124a-4dc0-9408-7da5227576f0": "GRUPO ROTA - PARADOURO REST.",
                            "3885ddf8-f0ac-4468-98ab-97a248e29150": "GRUPO ROTA - TRANSPORTADORA"
                        }
                        nome_empresa = empresas_completo.get(cliente_id, f"Empresa Não Identificada")
                    
                    # Criar identificador único para evitar duplicatas
                    identificador = f"{cliente_id}_{nome_conta}"
                    
                    if nome_conta and conta_id and identificador not in contas_processadas:
                        # Determinar tipo da conta
                        tipo_desc = "Conta Corrente" if tipo_conta == 1 else "Aplicação"
                        
                        # Criar nome amigável: "Nome Real da Empresa - Nome do Banco (Tipo)"
                        nome_amigavel = f"{nome_empresa} - {nome_conta}"
                        if tipo_desc == "Aplicação":
                            nome_amigavel += f" ({tipo_desc})"
                        
                        bancos.append({
                            'codigo': conta_id,
                            'nome': nome_amigavel,
                            'empresa': nome_empresa,
                            'banco': nome_conta,  # Nome original da API
                            'banco_api_original': nome_conta,  # Nome exato da API
                            'tipo': tipo_desc
                        })
                        contas_processadas.add(identificador)
                
                return sorted(bancos, key=lambda x: (x['empresa'], x['banco']))
    except Exception as e:
        st.error(f"Erro ao buscar bancos da API: {str(e)}")
    
    # Se chegou aqui, significa que não foi possível obter bancos da API
    return []

def process_ofx_files_with_mapping(file_mappings: dict, ofx_reader, bank_mapper):
    """
    Processa arquivos OFX com mapeamento individual por arquivo.
    
    Args:
        file_mappings: Dict com mapeamentos arquivo -> banco
        ofx_reader: Instância do OFXReader
        bank_mapper: Instância do BankMapper
    """
    try:
        with st.spinner("Processando arquivos OFX com mapeamento individual..."):
            # Preparar dados dos arquivos com mapeamento
            files_data = []
            
            for file_key, mapping in file_mappings.items():
                file = mapping['file']
                banco_codigo = mapping['codigo']
                banco_nome = mapping['nome']
                
                content = file.read().decode("utf-8", errors="ignore")
                files_data.append({
                    'content': content,
                    'name': file.name,
                    'banco_sistema': banco_codigo,
                    'banco_nome': banco_nome
                })
            
            # Processar com OFXReader
            df_ofx = ofx_reader.read_multiple_ofx(files_data)
            
            if not df_ofx.empty:
                # Aplicar mapeamento individual por arquivo
                df_ofx_mapped = apply_individual_bank_mapping(df_ofx, file_mappings)
                
                # =====================================
                # 🏦 DETECÇÃO DE PAGAMENTO TITULO BRR (BANRISUL)
                # =====================================
                
                # Detectar transações genéricas que precisam de detalhamento
                df_processado, stats_titulo_brr, processor_titulo_brr = detectar_e_processar_titulo_brr(df_ofx_mapped)
                
                # Salvar processador para uso posterior
                st.session_state.titulo_brr_processor = processor_titulo_brr
                st.session_state.titulo_brr_stats = stats_titulo_brr
                
                # Usar DataFrame processado (sem as transações genéricas)
                df_final = df_processado
                
                # Mostrar alerta se houver transações genéricas detectadas
                if stats_titulo_brr['transacoes_genericas'] > 0:
                    st.warning(f"⚠️ **{stats_titulo_brr['transacoes_genericas']} transações genéricas detectadas** (PAGAMENTO TITULO BRR)")
                    st.info(f"💰 Valor total: R$ {stats_titulo_brr['valor_total_generico']:,.2f}")
                    st.info("📁 **Para obter detalhes específicos, faça upload dos arquivos de retorno correspondentes na aba 'Detalhamento TITULO BRR'**")
                
                # =====================================
                
                # Gerar resumo
                summary = ofx_reader.get_summary(df_final)
                mapping_validation = validate_individual_mapping(df_final)
                
                # Salvar no estado da sessão
                st.session_state.df_ofx = df_final
                st.session_state.ofx_summary = {
                    'transactions': summary,
                    'banking': mapping_validation,
                    'mapping_stats': {'fonte_primaria': 'Individual', 'total_bancos': len(file_mappings)}
                }
                
                st.success(f"✅ {len(df_final)} transações processadas com mapeamento individual!")
                
                # Mostrar resumo das transações genéricas se existirem
                if stats_titulo_brr['transacoes_genericas'] > 0:
                    st.markdown("---")
                    st.markdown("### 📋 Resumo de Transações Detectadas")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("✅ Normais", stats_titulo_brr['transacoes_normais'])
                    with col2:
                        st.metric("⚠️ Genéricas", stats_titulo_brr['transacoes_genericas'])
                    with col3:
                        st.metric("📊 % Genéricas", f"{stats_titulo_brr['percentual_generico']:.1f}%")
            else:
                st.warning("❌ Nenhuma transação foi encontrada nos arquivos OFX")
                
    except Exception as e:
        st.error(f"❌ Erro ao processar arquivos: {str(e)}")

def apply_individual_bank_mapping(df_ofx: pd.DataFrame, file_mappings: dict) -> pd.DataFrame:
    """
    Aplica mapeamento individual de bancos baseado no arquivo de origem.
    
    Args:
        df_ofx: DataFrame com transações OFX
        file_mappings: Dict com mapeamentos arquivo -> banco
        
    Returns:
        DataFrame com mapeamento aplicado
    """
    df_mapped = df_ofx.copy()
    
    # Adicionar colunas de mapeamento
    df_mapped['banco_codigo_sistema'] = ''
    df_mapped['banco_nome_sistema'] = ''
    df_mapped['banco_mapeado'] = False
    df_mapped['fonte_mapeamento'] = 'individual'
    
    # Aplicar mapeamento por arquivo
    for file_key, mapping in file_mappings.items():
        file_name = mapping['file'].name
        banco_codigo = mapping['codigo']
        # Usar o nome original da API se disponível, senão usar o nome completo
        banco_nome_original = mapping.get('banco_api_original') or mapping.get('banco', mapping['nome'])
        
        # Encontrar transações deste arquivo específico
        mask = df_mapped['arquivo_origem'] == file_name
        
        df_mapped.loc[mask, 'banco_codigo_sistema'] = banco_codigo
        df_mapped.loc[mask, 'banco_nome_sistema'] = banco_nome_original
        df_mapped.loc[mask, 'banco_mapeado'] = True
    
    return df_mapped

def validate_individual_mapping(df_ofx: pd.DataFrame) -> dict:
    """
    Valida mapeamento individual aplicado.
    
    Args:
        df_ofx: DataFrame com mapeamento aplicado
        
    Returns:
        Dict com estatísticas de validação
    """
    total_transacoes = len(df_ofx)
    transacoes_mapeadas = len(df_ofx[df_ofx['banco_mapeado'] == True])
    bancos_unicos = df_ofx[df_ofx['banco_mapeado'] == True]['banco_codigo_sistema'].nunique()
    
    return {
        'total_transacoes': total_transacoes,
        'transacoes_mapeadas': transacoes_mapeadas,
        'transacoes_nao_mapeadas': total_transacoes - transacoes_mapeadas,
        'percentual_mapeado': (transacoes_mapeadas / total_transacoes * 100) if total_transacoes > 0 else 0,
        'total_bancos': bancos_unicos,
        'bancos_mapeados': bancos_unicos
    }

def processar_ofx_simplificado(df_transacoes, df_mr_historico):
    """
    Processa OFX seguindo modelo do arquivo retorno:
    - Categoriza TODAS as transações independente do score
    - Score ≥70%: Categoria + Contato preenchidos (Status: AUTO)
    - Score <70%: Categoria = "MANUAL" + Melhor contato sugerido (Status: REVISAR)
    
    Args:
        df_transacoes: DataFrame com transações OFX
        df_mr_historico: DataFrame com histórico do MR
    
    Returns:
        DataFrame pronto para Excel do Sistema MR
    """
    from difflib import SequenceMatcher
    import streamlit as st
    
    def similaridade_nomes(nome1, nome2):
        """Calcula similaridade entre dois nomes"""
        nome1 = str(nome1).upper().strip()
        nome2 = str(nome2).upper().strip()
        return SequenceMatcher(None, nome1, nome2).ratio()
    
    def extrair_nome_limpo(contato_mr):
        """Extrai nome limpo do formato 'CODIGO - NOME' do MR"""
        if " - " in str(contato_mr):
            return str(contato_mr).split(" - ", 1)[1].strip()
        return str(contato_mr).strip()
    
    def normalizar_nome(texto):
        """Normaliza nome igual ao arquivo retorno"""
        if not isinstance(texto, str):
            return ""
        import unidecode
        return unidecode.unidecode(texto).lower().strip()
    
    # Preparar resultado seguindo estrutura do Excel MR
    resultados = []
    threshold = 0.7  # Mesmo threshold, mas sem rejeitar nada
    
    print(f"🔄 Processando {len(df_transacoes)} transações OFX...")
    
    # Para cada transação, buscar melhor match e categorizar SEMPRE
    for _, transacao in df_transacoes.iterrows():
        nome_transacao = str(transacao['descricao']).upper().strip()
        nome_norm = normalizar_nome(transacao['descricao'])
        melhor_score = 0.0
        melhor_match = None
        melhor_contato_nome = None
        
        # Buscar melhor match no histórico MR (igual arquivo retorno)
        for _, lancamento_mr in df_mr_historico.iterrows():
            if 'contato' in lancamento_mr and pd.notna(lancamento_mr['contato']):
                contato_norm = normalizar_nome(lancamento_mr['contato'])
                
                # Usar lógica do arquivo retorno: buscar substring
                if nome_norm in contato_norm or contato_norm in nome_norm:
                    score = similaridade_nomes(nome_transacao, lancamento_mr['contato'])
                    if score > melhor_score:
                        melhor_score = score
                        melhor_match = lancamento_mr
                        melhor_contato_nome = extrair_nome_limpo(lancamento_mr['contato'])
        
        # SEMPRE adicionar ao resultado (diferente da versão anterior)
        if melhor_score >= threshold and melhor_match is not None:
            # Score alto: categorização automática
            categoria = melhor_match.get('categoria', 'MANUAL')
            # Limpar categoria (remover código se houver)
            if isinstance(categoria, str) and " - " in categoria:
                categoria = categoria.split(" - ", 1)[1].strip()
            
            resultado = {
                "Data": transacao['data'].strftime("%d/%m/%Y") if pd.notna(transacao['data']) else "",
                "Descrição": nome_transacao,
                "Valor": transacao['valor_absoluto'],
                "Contato": melhor_match['contato'],
                "Categoria": categoria,
                "Status": "AUTO",
                "Score_Confiança": f"{melhor_score:.1%}"
            }
            print(f"✅ AUTO: '{nome_transacao[:50]}...' → '{melhor_contato_nome}' ({melhor_score:.1%})")
        else:
            # Score baixo: sugerir melhor match, mas categorizar como MANUAL
            resultado = {
                "Data": transacao['data'].strftime("%d/%m/%Y") if pd.notna(transacao['data']) else "",
                "Descrição": nome_transacao,
                "Valor": transacao['valor_absoluto'],
                "Contato": melhor_match['contato'] if melhor_match else "",  # Sugestão
                "Categoria": "MANUAL",
                "Status": "REVISAR",
                "Score_Confiança": f"{melhor_score:.1%}" if melhor_score > 0 else "0%"
            }
            print(f"🔶 REVISAR: '{nome_transacao[:50]}...' → sugestão: '{melhor_contato_nome or 'Nenhuma'}' ({melhor_score:.1%})")
        
        resultados.append(resultado)
    
    # Converter para DataFrame final
    df_final = pd.DataFrame(resultados)
    
    # Estatísticas finais
    total = len(df_final)
    auto_count = len(df_final[df_final['Status'] == 'AUTO'])
    revisar_count = len(df_final[df_final['Status'] == 'REVISAR'])
    
    print(f"📊 RESULTADO FINAL: {total} transações processadas")
    print(f"   ✅ {auto_count} automáticas ({auto_count/total*100:.1f}%)")
    print(f"   🔶 {revisar_count} para revisão ({revisar_count/total*100:.1f}%)")
    
    # Salvar estatísticas no session state
    if hasattr(st, 'session_state'):
        st.session_state.ofx_stats = {
            'total': total,
            'auto': auto_count,
            'revisar': revisar_count,
            'percentual_auto': auto_count/total*100 if total > 0 else 0
        }
    
    return df_final

def criar_excel_mr_ofx(df_dados, banco_selecionado, nome_aba="Planilha1"):
    """
    Cria arquivo Excel formatado para importação no Sistema MR a partir de dados OFX.
    
    Args:
        df_dados: DataFrame com dados processados
        banco_selecionado: Informações do banco selecionado
        nome_aba: Nome da aba do Excel
        
    Returns:
        Tuple: (bytes_excel, dataframe_preview)
    """
    try:
        # Preparar DataFrame para o Excel MR
        df_excel = df_dados.copy()
        
        # Garantir que as colunas obrigatórias existam
        colunas_obrigatorias = ['Data', 'Descrição', 'Valor', 'Contato', 'Categoria']
        for col in colunas_obrigatorias:
            if col not in df_excel.columns:
                df_excel[col] = ''
        
        # Selecionar e reordenar colunas
        df_excel = df_excel[colunas_obrigatorias]
        
        # Criar arquivo Excel
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_excel.to_excel(writer, sheet_name=nome_aba, index=False)
            
            # Acessar a planilha para formatação
            worksheet = writer.sheets[nome_aba]
            
            # Formatação de cabeçalho
            from openpyxl.styles import Font, PatternFill, Alignment
            
            header_font = Font(bold=True, color="FFFFFF")
            header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
            header_alignment = Alignment(horizontal="center", vertical="center")
            
            # Aplicar formatação ao cabeçalho
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = header_alignment
            
            # Ajustar largura das colunas
            column_widths = {
                'A': 12,  # Data
                'B': 40,  # Descrição
                'C': 15,  # Valor
                'D': 30,  # Contato
                'E': 25   # Categoria
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
        
        output.seek(0)
        excel_bytes = output.getvalue()
        
        return excel_bytes, df_excel
        
    except Exception as e:
        st.error(f"Erro ao criar Excel: {str(e)}")
        return None, None

def render_leitura_ofx_simplificada_tab(df_ofx: pd.DataFrame):
    """Nova versão simplificada seguindo modelo do arquivo retorno"""
    
    st.subheader("📖 Leitura do OFX")
    st.markdown("**Processamento automático: Upload → Detecção Duplicatas → Categorização → Excel para Sistema MR**")
    
    # Verificar se há empresas configuradas na API
    empresas_configuradas = st.session_state.get('empresas_api_selecionadas', [])
    
    if not empresas_configuradas:
        st.warning("⚠️ **Nenhuma empresa configurada!**")
        st.info("Configure primeiro as empresas na seção **'⚙️ Configurar Mapeamento via API MR'** no topo da página.")
        return
    
    # Mostrar empresas que serão usadas
    st.success(f"✅ **Usando empresas configuradas:** {', '.join([emp.replace('🏢 ', '') for emp in empresas_configuradas])}")
    
    # Estados da sessão
    if "ofx_processado" not in st.session_state:
        st.session_state.ofx_processado = None
    
    # =====================================
    # 🚀 PROCESSAMENTO SIMPLES
    # =====================================
    
    col_btn1, col_btn2 = st.columns([3, 1])
    
    with col_btn1:
        if st.button("🚀 Processar OFX", type="primary", use_container_width=True):
            with st.spinner("🔄 Processando arquivo OFX..."):
                try:
                    # Mapear empresas selecionadas para IDs
                    EMPRESAS_MR = {
                        "🏢 ROTA - Araranguá": "772644ba-3a49-4736-8443-f057581d6b39",
                        "🏢 ROTA - Terra de Areia": "4d49850f-ebf1-433d-a32a-527b54e856aa", 
                        "🏢 ROTA - Caminho do Sol": "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401",
                        "🏢 ROTA - Candiota": "b2107e4f-59a7-44a0-9a84-d81abaab5ad2",
                        "🏢 ROTA - Jaguaruna": "149653c2-f107-4c60-aad0-b034789c8504",
                        "🏢 ROTA - Paradouro": "735b6b4e-5513-4bb5-a9c4-50d92462921d",
                        "🏢 ROTA - São Paulo": "1db3be97-a6d6-484a-b75b-fc1bdc6c487a",
                        "🏢 ROTA - Eldorado": "93f44c44-bfd4-417f-bad2-20933e5c0228",
                        "🏢 ROTA - Pinheiro Machado": "a13229ca-0f8a-442a-91ab-27e0adc1810b",
                        "🏢 ROTA - Seberi": "eb84222f-2e6b-4f68-8457-760d10e24043",
                        "🏢 ROTA - POA Ipiranga": "85d3091d-af31-4cb5-86fc-1558aaefa19b",
                        "🏢 ROTA - Cristal": "7a078786-1d9e-4433-9d63-8dfc58130b5f",
                        "🏢 ROTA - Porto Alegre": "73a32cc3-d7ac-48d7-91d7-9046045d0bd7",
                        "🏢 ROTA - Paradouro Rest.": "cad79622-124a-4dc0-9408-7da5227576f0",
                        "🏢 ROTA - Transportadora": "3885ddf8-f0ac-4468-98ab-97a248e29150"
                    }
                    
                    # Converter nomes para IDs
                    empresa_ids = [EMPRESAS_MR[empresa] for empresa in empresas_configuradas if empresa in EMPRESAS_MR]
                    
                    if not empresa_ids:
                        st.error("❌ Erro: Empresas configuradas não encontradas no mapeamento")
                        return
                    
                    # ETAPA 1: Detecção de Duplicatas (manter igual)
                    st.info("🔍 **Etapa 1/3**: Detectando transações já lançadas...")
                    duplicates_results = detectar_duplicatas_data_valor_banco(df_ofx, empresa_ids, "2025")
                    
                    if not duplicates_results:
                        st.error("❌ Erro na detecção de duplicatas")
                        return
                    
                    # ETAPA 2: Carregar histórico para categorização
                    st.info("📚 **Etapa 2/3**: Carregando histórico para categorização...")
                    ids_string = ",".join(empresa_ids)
                    from logic.Sistema_MR.API_MR import buscar_lancamentos_e_transferencias_api
                    dados_mr_completos = buscar_lancamentos_e_transferencias_api(ids_string, "2025")
                    
                    if not dados_mr_completos or 'combinado' not in dados_mr_completos:
                        st.error("❌ Erro ao carregar histórico do sistema MR")
                        return
                    
                    df_mr_historico = dados_mr_completos['combinado']
                    
                    # ETAPA 3: Categorização simplificada (NOVA LÓGICA)
                    st.info("🧠 **Etapa 3/3**: Categorizando todas as transações...")
                    
                    # Pegar transações novas (não duplicatas)
                    df_novas_transacoes = pd.DataFrame(duplicates_results['novas_transacoes'])
                    
                    if not df_novas_transacoes.empty:
                        # Usar nova função simplificada
                        df_excel_final = processar_ofx_simplificado(df_novas_transacoes, df_mr_historico)
                        
                        # Obter informação do banco para o Excel
                        banco_info = "Banco não identificado"
                        if 'banco_nome_sistema' in df_novas_transacoes.columns:
                            bancos_unicos = df_novas_transacoes['banco_nome_sistema'].dropna().unique()
                            if len(bancos_unicos) > 0:
                                banco_info = bancos_unicos[0]
                        
                        # Salvar resultado processado
                        st.session_state.ofx_processado = {
                            'df_excel': df_excel_final,
                            'banco_info': banco_info,
                            'duplicates_stats': duplicates_results['estatisticas'],
                            'stats': st.session_state.get('ofx_stats', {})
                        }
                        
                        st.success("✅ **Processamento concluído!**")
                        
                    else:
                        st.warning("⚠️ Todas as transações já estão lançadas no Sistema MR")
                        
                except Exception as e:
                    st.error(f"❌ Erro no processamento: {str(e)}")
                    import traceback
                    st.error(f"Detalhes: {traceback.format_exc()}")
    
    with col_btn2:
        if st.button("🧹 Limpar", use_container_width=True):
            st.session_state.ofx_processado = None
            st.rerun()
    
    # =====================================
    # 📊 RESULTADOS SIMPLIFICADOS
    # =====================================
    
    if st.session_state.ofx_processado:
        render_resultados_ofx_simplificados(st.session_state.ofx_processado)

def render_resultados_ofx_simplificados(dados_processados):
    """Renderiza resultados simplificados seguindo modelo do arquivo retorno"""
    
    st.markdown("---")
    st.markdown("### 📊 Resultado do Processamento")
    
    df_excel = dados_processados['df_excel']
    stats = dados_processados['stats']
    dup_stats = dados_processados['duplicates_stats']
    banco_info = dados_processados['banco_info']
    
    # Métricas simples
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    
    with col_m1:
        st.metric("📊 Total Processadas", stats.get('total', 0))
    with col_m2:
        st.metric("🏦 Já Lançadas no Sistema MR", dup_stats.get('total_duplicatas', 0))
    with col_m3:
        st.metric("✅ Categorizadas Automaticamente", stats.get('auto', 0))
    with col_m4:
        st.metric("🔶 Para Revisão", stats.get('revisar', 0))
    
    # Preview da tabela (igual arquivo retorno)
    st.markdown("### 📋 Preview do Excel para Sistema MR")
    
    col_preview1, col_preview2 = st.columns(2)
    
    with col_preview1:
        if st.button("📊 Mostrar Preview", use_container_width=True):
            st.dataframe(df_excel, use_container_width=True)
    
    with col_preview2:
        # Gerar Excel para download
        excel_mr, df_mr_preview = criar_excel_mr_ofx(df_excel, banco_info)
        
        st.download_button(
            label="📥 Download Excel para Sistema MR",
            data=excel_mr,
            file_name=f"ofx_sistema_mr_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    # Informações adicionais
    if stats.get('revisar', 0) > 0:
        st.info(f"💡 **{stats.get('revisar', 0)} transações** estão marcadas como 'MANUAL' no Excel. Você pode ajustar as categorias/contatos diretamente no arquivo antes de importar no Sistema MR.")

def render_titulo_brr_tab():
    """Função simplificada para título BRR"""
    st.info("🏦 **Funcionalidade TITULO BRR**")
    st.markdown("Esta seção permite processar arquivos de retorno específicos do Banrisul.")
    st.markdown("**Em desenvolvimento** - Funcionalidade completa será implementada em breve.")

def render_ofx_results():
    """Renderiza os resultados do processamento OFX"""
    
    df_ofx = st.session_state.df_ofx
    summary = st.session_state.ofx_summary
    
    # Resumo em colunas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Transações", summary['transactions']['total_transactions'])
    with col2:
        st.metric("Créditos", summary['transactions']['total_creditos'])
    with col3:
        st.metric("Débitos", summary['transactions']['total_debitos'])
    with col4:
        bancos_mapeados = summary['banking']['bancos_mapeados']
        total_bancos = summary['banking']['total_bancos']
        mapping_source = summary.get('mapping_stats', {}).get('fonte_primaria', 'Estático')
        st.metric("Bancos Mapeados", f"{bancos_mapeados}/{total_bancos}", delta=f"Via {mapping_source}")
    
    # Abas de resultados - Integração completa em Leitura do OFX
    tab_trans, tab_leitura_ofx, tab_titulo_brr = st.tabs([
        "📊 Transações", 
        "📝 Leitura do OFX",
        "🏦 Detalhamento TITULO BRR"
    ])
    
    with tab_trans:
        st.subheader("💳 Transações Processadas")
        
        # Filtros
        col_filter1, col_filter2 = st.columns(2)
        
        with col_filter1:
            movimento_filter = st.selectbox(
                "Filtrar por movimento:",
                ["Todos", "CREDITO", "DEBITO"]
            )
        
        with col_filter2:
            if 'banco_nome_sistema' in df_ofx.columns:
                bancos_unicos = ["Todos"] + df_ofx['banco_nome_sistema'].dropna().unique().tolist()
                banco_filter = st.selectbox("Filtrar por banco:", bancos_unicos)
            else:
                banco_filter = "Todos"
        
        # Aplicar filtros
        df_filtered = df_ofx.copy()
        
        if movimento_filter != "Todos":
            df_filtered = df_filtered[df_filtered['movimento'] == movimento_filter]
        
        if banco_filter != "Todos":
            df_filtered = df_filtered[df_filtered['banco_nome_sistema'] == banco_filter]
        
        # Preparar DataFrame para exibição com formatação brasileira
        df_display = df_filtered.copy()
        
        # Formatações brasileiras
        if 'data' in df_display.columns:
            df_display['Data'] = pd.to_datetime(df_display['data']).dt.strftime('%d/%m/%Y')
        
        if 'valor_absoluto' in df_display.columns:
            df_display['Valor'] = df_display['valor_absoluto'].apply(
                lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            )
        
        # Renomear colunas e selecionar apenas as solicitadas
        colunas_exibir = []
        
        if 'Data' in df_display.columns:
            colunas_exibir.append('Data')
        
        if 'Valor' in df_display.columns:
            colunas_exibir.append('Valor')
            
        if 'descricao' in df_display.columns:
            df_display['Descrição'] = df_display['descricao']
            colunas_exibir.append('Descrição')
            
        if 'tipo' in df_display.columns:
            df_display['Tipo'] = df_display['tipo']
            colunas_exibir.append('Tipo')
            
        if 'movimento' in df_display.columns:
            df_display['Movimento'] = df_display['movimento']
            colunas_exibir.append('Movimento')
            
        if 'banco_nome_sistema' in df_display.columns:
            # Usar o nome exato da API sem modificações
            df_display['Banco Sistema'] = df_display['banco_nome_sistema']
            colunas_exibir.append('Banco Sistema')
        
        # Exibir apenas as colunas solicitadas
        if colunas_exibir:
            df_final = df_display[colunas_exibir]
            st.dataframe(df_final, width="stretch", hide_index=True)
        else:
            st.dataframe(df_filtered, width="stretch")
            
        st.caption(f"Exibindo {len(df_filtered)} de {len(df_ofx)} transações")
    
    with tab_leitura_ofx:
        render_leitura_ofx_simplificada_tab(df_ofx)
    
    with tab_titulo_brr:
        render_titulo_brr_tab()

def detectar_duplicatas_data_valor_banco(df_ofx: pd.DataFrame, empresa_ids: list, anos: str) -> dict:
    """
    Detecta duplicatas comparando Data + Valor entre OFX e API MR.
    Verifica tanto lançamentos quanto transferências.
    
    Args:
        df_ofx: DataFrame com transações OFX
        empresa_ids: Lista de IDs das empresas para buscar na API
        anos: Ano para buscar lançamentos
        
    Returns:
        Dict com resultados da comparação
    """
    try:
        # 1. Buscar dados da API MR (lançamentos + transferências)
        st.info("🔄 Buscando lançamentos e transferências da API MR...")
        # Converter lista de IDs para string separada por vírgulas
        ids_string = ",".join(empresa_ids) if isinstance(empresa_ids, list) else empresa_ids
        
        # Buscar ambos os tipos de dados
        dados_mr = buscar_lancamentos_e_transferencias_api(ids_empresa=ids_string, anos=anos)
        df_mr = dados_mr['combinado']
        
        if df_mr.empty:
            st.warning("⚠️ Nenhum lançamento ou transferência encontrado na API MR")
            return None
        
        # Mostrar estatísticas dos dados encontrados
        total_lancamentos = len(dados_mr['lancamentos']) if not dados_mr['lancamentos'].empty else 0
        total_transferencias = len(dados_mr['transferencias']) if not dados_mr['transferencias'].empty else 0
        st.info(f"📊 Encontrados: {total_lancamentos} lançamentos + {total_transferencias} transferências = {len(df_mr)} registros totais")
        
        # 2. Preparar dados OFX
        df_ofx_prep = df_ofx.copy()
        
        # Validar se tem as colunas necessárias
        required_columns = ['data', 'valor_absoluto']
        missing_columns = [col for col in required_columns if col not in df_ofx_prep.columns]
        
        if missing_columns:
            st.error(f"❌ Colunas obrigatórias ausentes no OFX: {missing_columns}")
            return None
        
        # Converter data para formato de comparação
        df_ofx_prep['data_comp'] = pd.to_datetime(df_ofx_prep['data']).dt.date
        df_ofx_prep['valor_comp'] = pd.to_numeric(df_ofx_prep['valor_absoluto'], errors='coerce')
        # Manter banco_comp para exibição, mas não usar na comparação
        df_ofx_prep['banco_comp'] = df_ofx_prep.get('banco_codigo_sistema', 'N/A').astype(str)
        
        # 3. Preparar dados MR (lançamentos + transferências)
        df_mr_prep = df_mr.copy()
        
        # Converter data MR
        df_mr_prep['data_comp'] = pd.to_datetime(df_mr_prep['data'], errors='coerce').dt.date
        df_mr_prep['valor_comp'] = pd.to_numeric(df_mr_prep['valor'], errors='coerce')
        
        # Manter informação de banco para exibição (não para comparação)
        if 'contaId' in df_mr_prep.columns:
            df_mr_prep['banco_comp'] = df_mr_prep['contaId'].astype(str)
        else:
            df_mr_prep['banco_comp'] = 'N/A'
        
        # 4. Criar chaves de comparação usando apenas DATA + VALOR
        # Usar formato consistente com MR (:.2f)
        df_ofx_prep['chave_comparacao'] = df_ofx_prep.apply(
            lambda row: f"{row['data_comp']}_{row['valor_comp']:.2f}", axis=1
        )
        
        # Para dados MR, criar chaves usando apenas DATA + VALOR
        chaves_mr = set()
        for _, row in df_mr_prep.iterrows():
            # Para todos os registros (lançamentos e transferências)
            chave = f"{row['data_comp']}_{row['valor_comp']:.2f}"
            chaves_mr.add(chave)
        
        # 5. Identificar duplicatas e novas transações
        df_ofx_prep['eh_duplicata'] = df_ofx_prep['chave_comparacao'].isin(chaves_mr)
        
        duplicatas = df_ofx_prep[df_ofx_prep['eh_duplicata'] == True].copy()
        novas_transacoes = df_ofx_prep[df_ofx_prep['eh_duplicata'] == False].copy()
        
        # 6. Estatísticas
        total_ofx = len(df_ofx_prep)
        total_mr = len(df_mr_prep)
        total_duplicatas = len(duplicatas)
        total_novas = len(novas_transacoes)
        percentual_duplicatas = (total_duplicatas / total_ofx * 100) if total_ofx > 0 else 0
        percentual_novas = (total_novas / total_ofx * 100) if total_ofx > 0 else 0
        
        # 7. Preparar detalhes das duplicatas
        detalhes_duplicatas = []
        for _, dup in duplicatas.iterrows():
            # Buscar match nos dados MR usando apenas DATA + VALOR
            mr_matches = df_mr_prep[
                (df_mr_prep['data_comp'] == dup['data_comp']) &
                (abs(df_mr_prep['valor_comp'] - dup['valor_comp']) < 0.01)
            ]
            
            if not mr_matches.empty:
                mr_match = mr_matches.iloc[0]
                
                # Determinar o tipo de match
                tipo_match = mr_match.get('tipo_registro', 'lancamento')
                
                detalhes_duplicatas.append({
                    'transacao_ofx': dup.to_dict(),
                    'transacao_mr': mr_match.to_dict(),
                    'match_details': {
                        'data_ofx': dup['data_comp'].strftime('%d/%m/%Y'),
                        'data_mr': mr_match['data_comp'].strftime('%d/%m/%Y'),
                        'valor_ofx': dup['valor_comp'],
                        'valor_mr': mr_match['valor_comp'],
                        'banco_ofx': dup['banco_comp'],
                        'banco_mr': mr_match.get('banco_comp', mr_match.get('contaId', 'N/A')),
                        'desc_ofx': str(dup.get('descricao', 'N/A'))[:100],
                        'desc_mr': str(mr_match.get('descricao', mr_match.get('contato', 'N/A')))[:100],
                        'tipo_mr': tipo_match.title()
                    }
                })
        
        # 8. Resultado final
        resultado = {
            'estatisticas': {
                'total_transacoes_ofx': total_ofx,
                'total_lancamentos_mr': total_lancamentos,
                'total_transferencias_mr': total_transferencias,
                'total_registros_mr': total_mr,
                'total_duplicatas': total_duplicatas,
                'total_novas': total_novas,
                'percentual_duplicatas': percentual_duplicatas,
                'percentual_novas': percentual_novas
            },
            'duplicatas': detalhes_duplicatas,
            'novas_transacoes': novas_transacoes.to_dict('records'),
            'criterio_usado': 'Data + Valor (lançamentos + transferências)',
            'dados_mr_detalhados': {
                'lancamentos': total_lancamentos,
                'transferencias': total_transferencias,
                'total': total_mr
            }
        }
        
        st.success(f"🎯 Comparação concluída: {total_duplicatas} já lançadas no Sistema MR, {total_novas} novas (verificados {total_lancamentos} lançamentos + {total_transferencias} transferências)")
        return resultado
        
    except Exception as e:
        st.error(f"❌ Erro na detecção de duplicatas: {str(e)}")
        return None

def validar_transacoes_ofx_robusta(df_transacoes_ofx: pd.DataFrame, df_mr: pd.DataFrame, tolerancia_dias: int = 0, tolerancia_valor: float = 0.001) -> pd.DataFrame:
    """
    Validação robusta de transações OFX contra o sistema MR.
    Adaptada especificamente para estrutura de dados OFX.
    
    Args:
        df_transacoes_ofx: DataFrame com transações OFX (colunas: data, valor_absoluto, descricao)
        df_mr: DataFrame com dados do MR
        tolerancia_dias: Tolerância em dias para comparação de datas
        tolerancia_valor: Tolerância percentual para comparação de valores
        
    Returns:
        DataFrame com resultado da validação
    """
    
    try:
        print(f"� Iniciando validação robusta de {len(df_transacoes_ofx)} transações OFX...")
        
        # Preparar DataFrame para resultados
        df_resultado = df_transacoes_ofx.copy()
        df_resultado['Status_Validacao'] = 'NOVO'  # Default: nova transação
        df_resultado['Match_Details'] = ''
        
        # Converter datas
        df_transacoes_ofx['data'] = pd.to_datetime(df_transacoes_ofx['data'])
        df_mr['data'] = pd.to_datetime(df_mr['data'])
        
        matches_encontrados = 0
        
        for idx, transacao_ofx in df_transacoes_ofx.iterrows():
            data_ofx = transacao_ofx['data']
            valor_ofx = abs(float(transacao_ofx['valor_absoluto']))
            
            # Buscar matches por data e valor com tolerâncias
            data_min = data_ofx - pd.Timedelta(days=tolerancia_dias)
            data_max = data_ofx + pd.Timedelta(days=tolerancia_dias)
            
            # Filtrar por data
            df_mr_filtrado = df_mr[
                (df_mr['data'] >= data_min) & 
                (df_mr['data'] <= data_max)
            ]
            
            if not df_mr_filtrado.empty:
                # Buscar por valor com tolerância
                for _, lancamento_mr in df_mr_filtrado.iterrows():
                    valor_mr = abs(float(lancamento_mr['valor']))
                    
                    # Calcular diferença percentual
                    if valor_mr > 0:
                        diferenca_percentual = abs(valor_ofx - valor_mr) / valor_mr
                        
                        if diferenca_percentual <= tolerancia_valor:
                            # Match encontrado!
                            df_resultado.loc[idx, 'Status_Validacao'] = 'DUPLICATA'
                            df_resultado.loc[idx, 'Match_Details'] = f"Data MR: {lancamento_mr['data'].strftime('%d/%m/%Y')}, Valor MR: R$ {valor_mr:,.2f}"
                            matches_encontrados += 1
                            break
        
        print(f"✅ Validação concluída: {matches_encontrados} duplicatas encontradas")
        return df_resultado
        
    except Exception as e:
        print(f"❌ Erro na validação: {str(e)}")
        return df_transacoes_ofx

def render_resultados_leitura_ofx_integrada(results):
    """Renderiza os resultados integrados da leitura OFX (duplicatas + categorização)"""
    
    st.markdown("---")
    st.markdown("### 📊 Resultados da Análise Completa")
    
    duplicates_results = results['duplicates_results']
    categorization_results = results['categorization_results']
    
    # Estatísticas dos resultados
    dup_stats = duplicates_results['estatisticas']
    cat_stats = categorization_results['stats']
    
    # =====================================
    # 📈 DASHBOARD DE RESULTADOS
    # =====================================
    
    col_dash1, col_dash2, col_dash3, col_dash4 = st.columns(4)
    
    with col_dash1:
        st.metric(
            "📊 Total OFX", 
            dup_stats['total_transacoes_ofx'],
            help="Total de transações no arquivo OFX"
        )
    
    with col_dash2:
        st.metric(
            "🏦 Já Lançadas no Sistema MR", 
            dup_stats['total_duplicatas'],
            delta=f"-{dup_stats['percentual_duplicatas']:.1f}%",
            help="Transações que já foram lançadas no Sistema MR"
        )
    
    with col_dash3:
        st.metric(
            "🧠 Auto-Categorizadas", 
            cat_stats['auto_categorizados'],
            delta=f"+{(cat_stats['auto_categorizados']/cat_stats['total_novas']*100):.1f}%" if cat_stats['total_novas'] > 0 else "0%",
            help="Transações categorizadas automaticamente pela IA"
        )
    
    with col_dash4:
        st.metric(
            "✋ Precisam Manual", 
            cat_stats['precisam_manual'],
            help="Transações que precisam categorização manual"
        )
    
    # =====================================
    # 🎯 FINALIZAÇÃO - TRANSAÇÕES PRONTAS PARA IMPORTAÇÃO
    # =====================================
    
    df_novos_categorizados = categorization_results['df_novos_categorizados']
    
    if not df_novos_categorizados.empty:
        st.markdown("---")
        with st.expander(f"🎯 Finalização - Transações Prontas para Importação ({len(df_novos_categorizados)})", expanded=True):
            
            num_auto = cat_stats['auto_categorizados']
            num_manual = cat_stats['precisam_manual']
            
            # Status da categorização
            if num_auto > 0:
                st.success(f"✅ **{num_auto} transações categorizadas automaticamente**")
            
            if num_manual > 0:
                st.warning(f"⚠️ **{num_manual} transações precisam de categorização manual**")
            
            # =====================================
            # 📥 DOWNLOADS PARA SISTEMA MR
            # =====================================
            
            st.markdown("---")
            with st.expander("📥 Downloads para Sistema MR", expanded=True):
                
                st.success(f"✅ **{len(df_novos_categorizados)} transações processadas prontas para importação**")
                st.info(f"📊 **{num_auto} auto-categorizadas + {num_manual} manuais**")
                
                # Gerar Excel seguindo o padrão do TITULO BRR
                if len(df_novos_categorizados) > 0:
                    # Obter informação do banco
                    banco_info = None
                    if 'banco_nome_sistema' in df_novos_categorizados.columns:
                        bancos_unicos = df_novos_categorizados['banco_nome_sistema'].dropna().unique()
                        if len(bancos_unicos) > 0:
                            banco_info = bancos_unicos[0]
                    
                    if not banco_info:
                        banco_info = "Banco não identificado"
                    
                    # Layout de downloads seguindo padrão TITULO BRR
                    col_down1, col_down2, col_down3 = st.columns([1, 1, 1])
                    
                    with col_down1:
                        if st.button("📊 Análise Completa", type="primary", key="excel_completo_leitura", use_container_width=True):
                            excel_mr, df_mr_preview = criar_excel_mr_ofx(df_novos_categorizados, banco_info)
                            
                            st.download_button(
                                label="📥 Baixar Análise Completa",
                                data=excel_mr,
                                file_name=f"leitura_ofx_completa_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_completo_leitura",
                                use_container_width=True
                            )
                    
                    with col_down2:
                        if num_auto > 0:
                            if st.button("🧠 Auto-categorizadas", key="excel_auto_leitura", use_container_width=True):
                                df_auto = categorization_results['df_auto_categorizados']
                                excel_auto, df_auto_preview = criar_excel_mr_ofx(df_auto, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Auto-categorizadas",
                                    data=excel_auto,
                                    file_name=f"leitura_ofx_auto_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_auto_leitura",
                                    use_container_width=True
                                )
                        else:
                            st.info("Nenhuma auto-categorizada")
                    
                    with col_down3:
                        if num_manual > 0:
                            if st.button("✋ Precisam Manual", key="excel_manual_leitura", use_container_width=True):
                                df_manual = categorization_results['df_precisam_manual']
                                excel_manual, df_manual_preview = criar_excel_mr_ofx(df_manual, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Precisam Manual",
                                    data=excel_manual,
                                    file_name=f"leitura_ofx_manual_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_manual_leitura",
                                    use_container_width=True
                                )
                        else:
                            st.success("Todas categorizadas!")
                    
                    # Preview do Excel
                    with st.expander("👁️ Preview do Excel gerado", expanded=False):
                        col_preview1, col_preview2 = st.columns([1, 1])
                        
                        with col_preview1:
                            if st.button("📊 Mostrar Preview", key="preview_leitura"):
                                excel_mr, df_mr_preview = criar_excel_mr_ofx(df_novos_categorizados, banco_info)
                                st.session_state.preview_excel_leitura = excel_mr
                                st.session_state.preview_df_leitura = df_mr_preview
                        
                        with col_preview2:
                            if st.button("📥 Download Excel", key="download_preview_leitura", type="primary"):
                                excel_mr, df_mr_preview = criar_excel_mr_ofx(df_novos_categorizados, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Excel Preview",
                                    data=excel_mr,
                                    file_name=f"preview_leitura_ofx_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_preview_direct_leitura"
                                )
                        
                        # Mostrar preview se existir
                        if hasattr(st.session_state, 'preview_df_leitura') and st.session_state.preview_df_leitura is not None:
                            st.dataframe(st.session_state.preview_df_leitura, use_container_width=True)
    
    # =====================================
    # 📋 DETALHAMENTO COMPLETO
    # =====================================
    
    st.markdown("---")
    st.markdown("### 📋 Detalhamento Completo")
    
    # Abas de detalhamento
    tab_duplicatas, tab_auto, tab_manual, tab_analise = st.tabs([
        f"🏦 Já Lançadas no Sistema MR ({dup_stats['total_duplicatas']})",
        f"🧠 Auto-Categorizadas ({cat_stats['auto_categorizados']})",
        f"✋ Precisam Manual ({cat_stats['precisam_manual']})",
        f"📊 Análise de Scores"
    ])
    
    with tab_duplicatas:
        st.markdown("**Transações que já foram lançadas no Sistema MR:**")
        
        duplicatas = duplicates_results.get('duplicatas', [])
        if duplicatas:
            # Converter para DataFrame
            df_duplicatas = []
            for dup in duplicatas:
                details = dup['match_details']
                df_duplicatas.append({
                    'Data': details['data_ofx'],
                    'Valor': f"R$ {details['valor_ofx']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
                    'Tipo MR': details.get('tipo_mr', 'Lançamento'),
                    'Descrição OFX': details['desc_ofx'][:50] + "..." if len(details['desc_ofx']) > 50 else details['desc_ofx'],
                    'Descrição MR': details['desc_mr'][:50] + "..." if len(details['desc_mr']) > 50 else details['desc_mr']
                })
            
            df_dup_display = pd.DataFrame(df_duplicatas)
            st.dataframe(df_dup_display, width="stretch", hide_index=True)
            st.info("🏦 Estas transações já foram lançadas no Sistema MR e foram automaticamente excluídas")
        else:
            st.success("✅ Nenhuma transação já lançada no Sistema MR encontrada!")
    
    with tab_auto:
        st.markdown("**Transações categorizadas automaticamente pela IA:**")
        
        df_auto_categorizados = categorization_results['df_auto_categorizados']
        if not df_auto_categorizados.empty:
            # Preparar dados para exibição
            df_display = df_auto_categorizados[['data', 'descricao', 'valor_absoluto', 'Categoria_Auto', 'Contato_Auto', 'Match_Score']].copy()
            df_display.columns = ['Data', 'Descrição', 'Valor', 'Categoria', 'Contato', 'Confiança']
            
            # Formatar data e valor
            df_display['Data'] = pd.to_datetime(df_display['Data'], errors='coerce').dt.strftime('%d/%m/%Y')
            df_display['Valor'] = df_display['Valor'].apply(lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
            df_display['Confiança'] = df_display['Confiança'].apply(lambda x: f"{x:.1%}")
            
            st.dataframe(df_display, width="stretch", hide_index=True)
        else:
            st.info("Nenhuma transação foi categorizada automaticamente")
    
    with tab_manual:
        st.markdown("**Transações que precisam de categorização manual:**")
        
        df_precisam_manual = categorization_results['df_precisam_manual']
        if not df_precisam_manual.empty:
            # Preparar dados para exibição
            df_display = df_precisam_manual[['data', 'descricao', 'valor_absoluto']].copy()
            df_display.columns = ['Data', 'Descrição', 'Valor']
            
            # Formatar data e valor
            df_display['Data'] = pd.to_datetime(df_display['Data'], errors='coerce').dt.strftime('%d/%m/%Y')
            df_display['Valor'] = df_display['Valor'].apply(lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
            
            st.dataframe(df_display, width="stretch", hide_index=True)
            st.info("💡 Estas transações não tiveram similaridade ≥70% com o histórico MR e precisam de categorização manual")
        else:
            st.success("🎉 Todas as transações foram categorizadas automaticamente!")
    
    with tab_analise:
        st.markdown("**Análise Detalhada de Scores de Similaridade:**")
        st.info("Esta tabela mostra todos os resultados da análise de similaridade, incluindo scores baixos que não foram aceitos automaticamente.")
        
        # Filtros de score
        col_filter1, col_filter2 = st.columns(2)
        
        with col_filter1:
            threshold_filter = st.slider(
                "🎯 Filtrar por Score Mínimo:",
                min_value=0.0,
                max_value=1.0,
                value=0.0,
                step=0.05,
                format="%.0%",
                help="Filtrar transações com score de similaridade acima do valor selecionado"
            )
        
        with col_filter2:
            status_filter = st.selectbox(
                "📊 Filtrar por Status:",
                options=["Todos", "🟢 AUTO", "🔴 MANUAL"],
                help="Filtrar transações por status de categorização"
            )
        
        # Verificar se há análise de scores disponível
        if hasattr(st.session_state, 'analise_scores_ofx') and st.session_state.analise_scores_ofx:
            df_scores = pd.DataFrame(st.session_state.analise_scores_ofx)
            
            # Aplicar filtros
            df_filtrado = df_scores.copy()
            
            # Filtro de score
            if threshold_filter > 0:
                df_filtrado = df_filtrado[df_filtrado['Score'] >= threshold_filter]
            
            # Filtro de status
            if status_filter != "Todos":
                if status_filter == "🟢 AUTO":
                    df_filtrado = df_filtrado[df_filtrado['Status'].str.contains('AUTO')]
                elif status_filter == "🔴 MANUAL":
                    df_filtrado = df_filtrado[~df_filtrado['Status'].str.contains('AUTO')]
            
            # Formatar Score como percentual
            df_filtrado['Score (%)'] = df_filtrado['Score'].apply(lambda x: f"{x:.1%}")
            
            # Definir cores para status
            def colorir_status(status):
                if "AUTO" in status:
                    return "🟢 " + status
                else:
                    return "🔴 " + status
            
            df_filtrado['Status'] = df_filtrado['Status'].apply(colorir_status)
            
            # Mostrar resultado filtrado
            if len(df_filtrado) != len(df_scores):
                st.markdown(f"**📋 Resultados Filtrados ({len(df_filtrado)} de {len(df_scores)}):**")
                
                # Ordenar primeiro pelos dados originais, depois criar display
                df_filtrado_ordenado = df_filtrado.sort_values('Score', ascending=False).copy()
                df_filtrado_ordenado['Score (%)'] = df_filtrado_ordenado['Score'].apply(lambda x: f"{x:.1%}")
                df_filtrado_ordenado['Status'] = df_filtrado_ordenado['Status'].apply(colorir_status)
                df_display_filtrado = df_filtrado_ordenado[['Transação OFX', 'Melhor Match', 'Score (%)', 'Status', 'Categoria']].copy()
                
                st.dataframe(df_display_filtrado, width="stretch", hide_index=True)
            
            # =====================================
            # 📥 DOWNLOADS PARA SISTEMA MR
            # =====================================
            
            st.markdown("---")
            st.markdown("**📥 Downloads para Sistema MR:**")
            st.info("💡 **Mesmo com scores baixos**, você pode baixar o Excel formatado para importação manual no Sistema MR")
            
            # Reconstruir DataFrame original das transações com as informações de score
            if 'leitura_ofx_results' in st.session_state and st.session_state.leitura_ofx_results:
                results_integrados = st.session_state.leitura_ofx_results
                df_todas_transacoes = results_integrados['categorization_results']['df_novos_categorizados']
                
                if not df_todas_transacoes.empty:
                    # Obter informação do banco
                    banco_info = None
                    if 'banco_nome_sistema' in df_todas_transacoes.columns:
                        bancos_unicos = df_todas_transacoes['banco_nome_sistema'].dropna().unique()
                        if len(bancos_unicos) > 0:
                            banco_info = bancos_unicos[0]
                    
                    if not banco_info:
                        banco_info = "Banco não identificado"
                    
                    # Layout de downloads
                    col_down1, col_down2, col_down3, col_down4 = st.columns([1, 1, 1, 1])
                    
                    with col_down1:
                        if st.button("📊 Todas as Transações", key="excel_todas_analise", use_container_width=True):
                            excel_mr, df_mr_preview = criar_excel_mr_ofx(df_todas_transacoes, banco_info)
                            
                            st.download_button(
                                label="📥 Baixar Todas",
                                data=excel_mr,
                                file_name=f"todas_transacoes_analise_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_todas_analise",
                                use_container_width=True
                            )
                    
                    with col_down2:
                        # Apenas as auto-categorizadas (score >= 70%)
                        df_auto_only = df_todas_transacoes[df_todas_transacoes['Status_Categorizacao'] == 'AUTO']
                        if len(df_auto_only) > 0:
                            if st.button(f"🟢 Auto-categorizadas ({len(df_auto_only)})", key="excel_auto_analise", use_container_width=True):
                                excel_auto, df_auto_preview = criar_excel_mr_ofx(df_auto_only, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Auto",
                                    data=excel_auto,
                                    file_name=f"auto_categorizadas_analise_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_auto_analise",
                                    use_container_width=True
                                )
                        else:
                            st.info("Nenhuma auto-categorizada", icon="🟢")
                    
                    with col_down3:
                        # Apenas as que precisam de categorização manual (score < 70%)
                        df_manual_only = df_todas_transacoes[df_todas_transacoes['Status_Categorizacao'] == 'MANUAL']
                        if len(df_manual_only) > 0:
                            if st.button(f"🔴 Precisam Manual ({len(df_manual_only)})", key="excel_manual_analise", use_container_width=True):
                                excel_manual, df_manual_preview = criar_excel_mr_ofx(df_manual_only, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Manual",
                                    data=excel_manual,
                                    file_name=f"precisam_manual_analise_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_manual_analise",
                                    use_container_width=True
                                )
                        else:
                            st.success("Todas categorizadas!", icon="🎉")
                    
                    with col_down4:
                        # Transações com score acima do filtro atual
                        if threshold_filter > 0 and len(df_filtrado) > 0:
                            # Reconstruir o DataFrame filtrado com as transações originais
                            nomes_filtrados = df_filtrado['Transação OFX'].tolist()
                            df_transacoes_filtradas = df_todas_transacoes[df_todas_transacoes['descricao'].isin(nomes_filtrados)]
                            
                            if len(df_transacoes_filtradas) > 0:
                                if st.button(f"🔍 Score ≥{threshold_filter:.0%} ({len(df_transacoes_filtradas)})", key="excel_filtrado_analise", use_container_width=True):
                                    excel_filtrado, df_filtrado_preview = criar_excel_mr_ofx(df_transacoes_filtradas, banco_info)
                                    
                                    st.download_button(
                                        label="📥 Baixar Filtrado",
                                        data=excel_filtrado,
                                        file_name=f"score_filtrado_{threshold_filter:.0%}_analise_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key="download_filtrado_analise",
                                        use_container_width=True
                                    )
                            else:
                                st.info(f"Nenhuma ≥{threshold_filter:.0%}", icon="🔍")
                        else:
                            st.info("Ajuste o filtro de score", icon="🔍")
                    
                    # Informações sobre os downloads
                    st.markdown("---")
                    st.markdown("**ℹ️ Informações dos Downloads:**")
                    
                    col_info1, col_info2 = st.columns(2)
                    
                    with col_info1:
                        st.info("""
                        **🟢 Auto-categorizadas**: Score ≥70%, categoria e contato preenchidos automaticamente
                        **🔴 Precisam Manual**: Score <70%, categoria ficará "MANUAL" para você ajustar
                        """)
                    
                    with col_info2:
                        st.warning("""
                        **💡 Dica**: Mesmo com score baixo, as transações têm o "melhor match" identificado.
                        Você pode usar isso como base para categorização manual no Sistema MR.
                        """)
            
            # Botão para baixar análise completa (mantido)
            st.markdown("---")
            if st.button("📊 Download Análise de Scores", key="download_scores"):
                output = io.BytesIO()
                df_scores.to_excel(output, index=False)
                output.seek(0)
                
                st.download_button(
                    label="📥 Baixar Análise Completa",
                    data=output,
                    file_name=f"analise_scores_leitura_ofx_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_scores_direct"
                )
            
            # Exibir todas as transações sem filtros
            st.markdown("---")
            st.markdown(f"**📋 Todas as Transações Analisadas ({len(df_scores)}):**")
            
            # Formatar Score como percentual
            df_scores['Score (%)'] = df_scores['Score'].apply(lambda x: f"{x:.1%}")
            
            # Definir cores para status
            def colorir_status(status):
                if "AUTO" in status:
                    return "🟢 " + status
                else:
                    return "🔴 " + status
            
            df_scores['Status'] = df_scores['Status'].apply(colorir_status)
            
            # Reordenar colunas para melhor visualização
            df_display_scores = df_scores[['Transação OFX', 'Melhor Match', 'Score (%)', 'Status', 'Categoria']].copy()
            
            # Ordenar por Score percentual (texto) convertendo de volta para número
            df_display_scores['Score_num'] = df_scores['Score'].copy()
            df_display_scores = df_display_scores.sort_values('Score_num', ascending=False)
            df_display_scores = df_display_scores.drop('Score_num', axis=1)
            
            st.dataframe(df_display_scores, width="stretch", hide_index=True)
        else:
            st.warning("⚠️ Nenhuma análise de scores disponível. Execute o processamento primeiro.")

if __name__ == "__main__":
    main()
