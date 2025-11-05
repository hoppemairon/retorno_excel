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
        render_leitura_ofx_integrada_tab(df_ofx)
    
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

def render_leitura_ofx_integrada_tab(df_ofx: pd.DataFrame):
    """Renderiza a aba integrada de Leitura do OFX com detecção de duplicatas + categorização inteligente"""
    
    st.subheader("� Leitura Completa do OFX")
    st.markdown("**Análise integrada: Detecção de Duplicatas + Categorização Inteligente usando empresas já configuradas na API**")
    
    # Verificar se há empresas configuradas na API
    empresas_configuradas = st.session_state.get('empresas_api_selecionadas', [])
    
    if not empresas_configuradas:
        st.warning("⚠️ **Nenhuma empresa configurada!**")
        st.info("Configure primeiro as empresas na seção **'⚙️ Configurar Mapeamento via API MR'** no topo da página.")
        return
    
    # Mostrar empresas que serão usadas
    st.success(f"✅ **Usando empresas configuradas:** {', '.join([emp.replace('🏢 ', '') for emp in empresas_configuradas])}")
    
    # Estados da sessão
    if "leitura_ofx_results" not in st.session_state:
        st.session_state.leitura_ofx_results = None
    
    # =====================================
    # 🔧 CONFIGURAÇÕES DO PROCESSAMENTO
    # =====================================
    
    st.markdown("### 🔧 Configurações do Processamento")
    
    col_config1, col_config2 = st.columns(2)
    
    with col_config1:
        anos_analise = st.selectbox(
            "Período para análise:",
            options=["2025", "2024", "2023"],
            index=0,
            help="Ano dos lançamentos históricos para comparação e categorização"
        )
    
    with col_config2:
        st.info(f"**{len(empresas_configuradas)} empresas** serão analisadas automaticamente")
    
    # =====================================
    # 🚀 PROCESSAMENTO INTEGRADO
    # =====================================
    
    st.markdown("---")
    st.markdown("### 🚀 Processamento Integrado")
    
    col_process1, col_process2 = st.columns([3, 1])
    
    with col_process1:
        if st.button("🚀 Executar Análise Completa", type="primary", width="stretch"):
            with st.spinner("🔄 Executando análise completa do OFX..."):
                try:
                    # Mapear empresas selecionadas para IDs
                    EMPRESAS_MR = {
                        "🏢 ROTA - Araranguá": "772644ba-3a49-4736-8443-f057581d6b39",
                        "🏢 ROTA - Terra de Areia": "4d49850f-ebf1-433d-a32a-527b54e856aa",
                        "🏢 ROTA - Caminho do Sol": "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401",
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
                    
                    # ETAPA 1: Detecção de Duplicatas
                    st.info("🔍 **Etapa 1/3**: Detectando lançamentos no sistema MR...")
                    
                    duplicates_results = detectar_duplicatas_data_valor_banco(df_ofx, empresa_ids, anos_analise)
                    
                    if not duplicates_results:
                        st.error("❌ Erro na detecção de duplicatas")
                        return
                    
                    # ETAPA 2: Buscar histórico completo para categorização
                    st.info("📚 **Etapa 2/3**: Carregando histórico completo para categorização...")
                    
                    ids_string = ",".join(empresa_ids)
                    from logic.Sistema_MR.API_MR import buscar_lancamentos_e_transferencias_api
                    dados_mr_completos = buscar_lancamentos_e_transferencias_api(ids_string, anos_analise)
                    
                    if not dados_mr_completos or 'combinado' not in dados_mr_completos:
                        st.error("❌ Erro ao carregar histórico do sistema MR")
                        return
                    
                    df_mr_historico = dados_mr_completos['combinado']
                    total_lancamentos = len(dados_mr_completos.get('lancamentos', pd.DataFrame()))
                    total_transferencias = len(dados_mr_completos.get('transferencias', pd.DataFrame()))
                    
                    st.success(f"✅ Histórico carregado: {total_lancamentos} lançamentos + {total_transferencias} transferências = {len(df_mr_historico)} registros")
                    
                    # ETAPA 3: Categorização inteligente das transações novas
                    st.info("🧠 **Etapa 3/3**: Executando categorização inteligente...")
                    
                    # Pegar só as transações novas (não duplicatas)
                    df_novas_transacoes = pd.DataFrame(duplicates_results['novas_transacoes'])
                    
                    if not df_novas_transacoes.empty:
                        # Aplicar categorização inteligente
                        df_categorizadas = categorizar_automaticamente_por_nome_ofx(df_novas_transacoes, df_mr_historico)
                        
                        # Separar por status
                        df_auto_categorizadas = df_categorizadas[df_categorizadas['Status_Categorizacao'] == 'AUTO'].copy()
                        df_precisam_manual = df_categorizadas[df_categorizadas['Status_Categorizacao'] == 'MANUAL'].copy()
                    else:
                        df_categorizadas = pd.DataFrame()
                        df_auto_categorizadas = pd.DataFrame()
                        df_precisam_manual = pd.DataFrame()
                    
                    # Compilar resultados finais
                    results_integrados = {
                        'duplicates_results': duplicates_results,
                        'categorization_results': {
                            'df_novos_categorizados': df_categorizadas,
                            'df_auto_categorizados': df_auto_categorizadas,
                            'df_precisam_manual': df_precisam_manual,
                            'stats': {
                                'total_novas': len(df_novas_transacoes),
                                'auto_categorizados': len(df_auto_categorizadas),
                                'precisam_manual': len(df_precisam_manual)
                            }
                        },
                        'mr_data': dados_mr_completos
                    }
                    
                    # Salvar resultados
                    st.session_state.leitura_ofx_results = results_integrados
                    
                    # Mostrar resumo final
                    st.success("🎉 **Análise Completa Concluída!**")
                    
                    # Métricas finais
                    duplicates_stats = duplicates_results['estatisticas']
                    col_metric1, col_metric2, col_metric3, col_metric4 = st.columns(4)
                    
                    with col_metric1:
                        st.metric("Total OFX", len(df_ofx))
                    with col_metric2:
                        st.metric("Duplicatas", duplicates_stats['total_duplicatas'])
                    with col_metric3:
                        st.metric("Auto-Categorizadas", len(df_auto_categorizadas))
                    with col_metric4:
                        st.metric("Precisam Manual", len(df_precisam_manual))
                    
                except Exception as e:
                    st.error(f"❌ Erro no processamento: {str(e)}")
                    import traceback
                    st.error(f"Detalhes: {traceback.format_exc()}")
    
    with col_process2:
        if st.button("🧹 Limpar"):
            st.session_state.leitura_ofx_results = None
            st.rerun()
    
    # =====================================
    # 📊 RESULTADOS INTEGRADOS
    # =====================================
    
    if st.session_state.leitura_ofx_results:
        render_resultados_leitura_ofx_integrada(st.session_state.leitura_ofx_results)

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
            
            st.dataframe(df_display_scores, width="stretch", hide_index=True)
            
            # Estatísticas da análise
            st.markdown("---")
            st.markdown("**📈 Estatísticas da Análise:**")
            
            total_transacoes = len(df_scores)
            auto_categorizadas = len(df_scores[df_scores['Status'].str.contains('AUTO')])
            manuais = total_transacoes - auto_categorizadas
            
            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
            
            with col_stat1:
                st.metric("📊 Total Analisadas", total_transacoes)
            
            with col_stat2:
                st.metric("🟢 Auto-Categorizadas", auto_categorizadas, delta=f"{(auto_categorizadas/total_transacoes*100):.1f}%")
            
            with col_stat3:
                st.metric("🔴 Categorização Manual", manuais, delta=f"{(manuais/total_transacoes*100):.1f}%")
            
            with col_stat4:
                score_medio = df_scores['Score'].mean()
                st.metric("📈 Score Médio", f"{score_medio:.1%}")
            
            # Filtros para análise
            st.markdown("---")
            st.markdown("**🔍 Filtros de Análise:**")
            
            col_filter1, col_filter2 = st.columns(2)
            
            with col_filter1:
                threshold_filter = st.slider(
                    "Mostrar scores acima de:",
                    min_value=0.0,
                    max_value=1.0,
                    value=0.0,
                    step=0.1,
                    format="%.1f",
                    help="Filtrar transações com score acima do valor selecionado"
                )
            
            with col_filter2:
                status_filter = st.selectbox(
                    "Filtrar por status:",
                    ["Todos", "AUTO", "MANUAL"],
                    help="Filtrar por status de categorização"
                )
            
            # Aplicar filtros
            df_filtrado = df_scores.copy()
            
            if threshold_filter > 0:
                df_filtrado = df_filtrado[df_filtrado['Score'] >= threshold_filter]
            
            if status_filter != "Todos":
                df_filtrado = df_filtrado[df_filtrado['Status'].str.contains(status_filter)]
            
            if len(df_filtrado) != len(df_scores):
                st.markdown(f"**📋 Resultados Filtrados ({len(df_filtrado)} de {len(df_scores)}):**")
                
                # Ordenar primeiro pelos dados originais, depois formatar
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
            if st.button("� Download Análise de Scores", key="download_scores"):
                output = io.BytesIO()
                df_scores.to_excel(output, index=False)
                output.seek(0)
                
                st.download_button(
                    label="📥 Baixar Análise Completa",
                    data=output,
                    file_name=f"analise_scores_ofx_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_scores_direct"
                )
        else:
            st.warning("⚠️ Nenhuma análise de scores disponível. Execute o processamento primeiro.")

def obter_banco_banrisul_titulo_brr():
    """
    Obtém o banco Banrisul para transações TITULO BRR de forma inteligente
    Retorna apenas a parte do banco (ex: "BANRISUL ( 06.077604.0-9 )")
    """
    if 'df_ofx' not in st.session_state:
        return None
    
    df_ofx = st.session_state.df_ofx
    
    if df_ofx is None or df_ofx.empty:
        return None

    # Primeiro, tentar acessar o mapeamento de arquivos para pegar apenas a parte do banco
    if 'file_mappings' in st.session_state and st.session_state.file_mappings:
        for file_key, mapping in st.session_state.file_mappings.items():
            banco_info = mapping.get('banco', '')
            if 'BANRISUL' in banco_info.upper():
                return banco_info
    
    # Função auxiliar para extrair apenas a parte do banco de uma string completa
    def extrair_parte_banco(nome_completo):
        """
        Extrai apenas a parte do banco de um nome completo
        Ex: "GRUPO ROTA - ARARANGUA - BANRISUL ( 06.077604.0-9 )" -> "BANRISUL ( 06.077604.0-9 )"
        """
        nome_completo = str(nome_completo)
        # Procurar por BANRISUL e pegar tudo a partir daí
        import re
        match = re.search(r'BANRISUL.*', nome_completo, re.IGNORECASE)
        if match:
            return match.group().strip()
        return nome_completo

    # Filtrar apenas contas do Banrisul
    # Verificar se há mapeamento individual (coluna banco_nome_sistema)
    if 'banco_nome_sistema' in df_ofx.columns:
        bancos_banrisul = df_ofx[df_ofx['banco_nome_sistema'].str.contains('BANRISUL', case=False, na=False)]
        if not bancos_banrisul.empty:
            # Obter bancos únicos do mapeamento e extrair apenas a parte do banco
            bancos_sistemas = bancos_banrisul['banco_nome_sistema'].dropna().unique()
            bancos_sistemas = [extrair_parte_banco(banco) for banco in bancos_sistemas]
            
            if len(bancos_sistemas) == 1:
                # Apenas uma conta Banrisul - usar automaticamente
                return bancos_sistemas[0]
            elif len(bancos_sistemas) > 1:
                # Múltiplas contas - usuário escolhe
                return bancos_sistemas
            
    # Fallback: verificar nas colunas originais
    if 'banco_nome' not in df_ofx.columns:
        return None
    
    bancos_banrisul = df_ofx[df_ofx['banco_nome'].str.contains('BANRISUL', case=False, na=False)]
    
    if bancos_banrisul.empty:
        return None
    
    # Verificar se a coluna banco_sistema existe
    if 'banco_sistema' not in bancos_banrisul.columns:
        # Se não tiver banco_sistema, usar banco_nome como fallback
        bancos_sistemas = bancos_banrisul['banco_nome'].dropna().unique()
    else:
        # Obter bancos únicos
        bancos_sistemas = bancos_banrisul['banco_sistema'].dropna().unique()
    
    # Extrair apenas a parte do banco
    bancos_sistemas = [extrair_parte_banco(banco) for banco in bancos_sistemas]
    
    if len(bancos_sistemas) == 1:
        # Apenas uma conta Banrisul - usar automaticamente
        return bancos_sistemas[0]
    elif len(bancos_sistemas) > 1:
        # Múltiplas contas - usuário escolhe
        return bancos_sistemas
    else:
        return None

def categorizar_automaticamente_por_nome(df_transacoes, df_mr_historico):
    """
    Categoriza automaticamente transações baseado em similaridade de nomes
    com dados históricos do MR (independente de duplicatas)
    
    Args:
        df_transacoes: DataFrame com transações do arquivo .RET
        df_mr_historico: DataFrame com todos os lançamentos históricos do MR
    
    Returns:
        DataFrame com colunas adicionais: Categoria_Auto, Contato_Auto, Match_Score
    """
    from difflib import SequenceMatcher
    
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
    
    # Criar DataFrame resultado
    df_resultado = df_transacoes.copy()
    df_resultado['Categoria_Auto'] = None
    df_resultado['Contato_Auto'] = None
    df_resultado['Match_Score'] = 0.0
    df_resultado['Status_Categorizacao'] = 'MANUAL'
    
    # Para cada transação, buscar melhor match nos dados do MR
    for idx, transacao in df_resultado.iterrows():
        nome_transacao = str(transacao['Favorecido']).upper().strip()
        melhor_score = 0.0
        melhor_match = None
        
        # Buscar em todos os contatos do MR
        for _, lancamento_mr in df_mr_historico.iterrows():
            if 'contato' in lancamento_mr:
                nome_mr = extrair_nome_limpo(lancamento_mr['contato'])
                score = similaridade_nomes(nome_transacao, nome_mr)
                
                if score > melhor_score:
                    melhor_score = score
                    melhor_match = lancamento_mr
        
        # Se encontrou match com confiança alta (>= 80%)
        if melhor_score >= 0.8 and melhor_match is not None:
            df_resultado.loc[idx, 'Categoria_Auto'] = melhor_match.get('categoria', 'MANUAL')
            df_resultado.loc[idx, 'Contato_Auto'] = melhor_match.get('contato', nome_transacao)
            df_resultado.loc[idx, 'Match_Score'] = melhor_score
            df_resultado.loc[idx, 'Status_Categorizacao'] = 'AUTO'
        else:
            df_resultado.loc[idx, 'Status_Categorizacao'] = 'MANUAL'
    
    return df_resultado

def categorizar_automaticamente_por_nome_ofx(df_transacoes, df_mr_historico):
    """
    Categoriza automaticamente transações OFX baseado em similaridade de nomes
    com dados históricos do MR (independente de duplicatas)
    
    Args:
        df_transacoes: DataFrame com transações OFX (colunas: descricao, data, valor_absoluto)
        df_mr_historico: DataFrame com todos os lançamentos históricos do MR
    
    Returns:
        DataFrame com colunas adicionais: Categoria_Auto, Contato_Auto, Match_Score
    """
    from difflib import SequenceMatcher
    
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
    
    # Debug: Verificar estrutura dos dados
    print(f"🔍 DEBUG - Transações OFX: {len(df_transacoes)} registros")
    print(f"🔍 DEBUG - Histórico MR: {len(df_mr_historico)} registros")
    if not df_mr_historico.empty:
        print(f"🔍 DEBUG - Colunas MR: {list(df_mr_historico.columns)}")
        if 'contato' in df_mr_historico.columns:
            contatos_unicos = df_mr_historico['contato'].dropna().unique()
            print(f"🔍 DEBUG - Contatos únicos no MR: {len(contatos_unicos)}")
            if len(contatos_unicos) > 0:
                print(f"🔍 DEBUG - Exemplo de contatos: {contatos_unicos[:5]}")
    
    # Criar DataFrame resultado
    df_resultado = df_transacoes.copy()
    df_resultado['Categoria_Auto'] = None
    df_resultado['Contato_Auto'] = None
    df_resultado['Match_Score'] = 0.0
    df_resultado['Status_Categorizacao'] = 'MANUAL'
    df_resultado['Melhor_Match_Contato'] = None  # Para mostrar o melhor match mesmo se baixo
    
    # Lista para armazenar todos os resultados de análise
    analise_scores = []
    
    matches_encontrados = 0
    threshold = 0.7  # Reduzido de 0.8 para 0.7 para ser menos restritivo
    
    # Para cada transação, buscar melhor match nos dados do MR
    for idx, transacao in df_resultado.iterrows():
        # Usar 'descricao' que é a coluna padrão das transações OFX
        nome_transacao = str(transacao['descricao']).upper().strip()
        melhor_score = 0.0
        melhor_match = None
        melhor_contato_nome = None
        
        # Buscar em todos os contatos do MR
        for _, lancamento_mr in df_mr_historico.iterrows():
            if 'contato' in lancamento_mr and pd.notna(lancamento_mr['contato']):
                nome_mr = extrair_nome_limpo(lancamento_mr['contato'])
                score = similaridade_nomes(nome_transacao, nome_mr)
                
                if score > melhor_score:
                    melhor_score = score
                    melhor_match = lancamento_mr
                    melhor_contato_nome = nome_mr
        
        # Armazenar informações da análise para exibição posterior
        status_match = "✅ AUTO" if melhor_score >= threshold else "❌ MANUAL"
        analise_scores.append({
            'Transação OFX': nome_transacao,
            'Melhor Match': melhor_contato_nome if melhor_contato_nome else 'Nenhum',
            'Score': melhor_score,
            'Status': status_match,
            'Categoria': melhor_match.get('categoria', 'MANUAL') if melhor_match is not None else 'MANUAL'
        })
        
        # Se encontrou match com confiança alta (>= 70% agora)
        if melhor_score >= threshold and melhor_match is not None:
            df_resultado.loc[idx, 'Categoria_Auto'] = melhor_match.get('categoria', 'MANUAL')
            df_resultado.loc[idx, 'Contato_Auto'] = melhor_match.get('contato', nome_transacao)
            df_resultado.loc[idx, 'Match_Score'] = melhor_score
            df_resultado.loc[idx, 'Status_Categorizacao'] = 'AUTO'
            df_resultado.loc[idx, 'Melhor_Match_Contato'] = melhor_contato_nome
            matches_encontrados += 1
            print(f"✅ Match encontrado: '{nome_transacao}' → '{melhor_contato_nome}' (score: {melhor_score:.2f})")
        else:
            df_resultado.loc[idx, 'Status_Categorizacao'] = 'MANUAL'
            df_resultado.loc[idx, 'Melhor_Match_Contato'] = melhor_contato_nome
            print(f"❌ Sem match: '{nome_transacao}' (melhor score: {melhor_score:.2f})")
    
    print(f"🎯 RESULTADO: {matches_encontrados}/{len(df_transacoes)} transações categorizadas automaticamente")
    
    # Salvar análise de scores no session_state para exibição
    import streamlit as st
    if 'analise_scores_ofx' not in st.session_state:
        st.session_state.analise_scores_ofx = []
    st.session_state.analise_scores_ofx = analise_scores
    
    return df_resultado

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
        DataFrame com coluna 'status_validacao' (NOVO/DUPLICATA)
    """
    import unidecode
    from datetime import datetime, timedelta
    
    print(f"🔍 DEBUG VALIDAÇÃO - OFX: {len(df_transacoes_ofx)} transações")
    print(f"🔍 DEBUG VALIDAÇÃO - MR: {len(df_mr)} registros")
    print(f"🔍 DEBUG VALIDAÇÃO - Tolerância: {tolerancia_dias} dias, {tolerancia_valor*100}% valor")
    
    # Normalizar dados do MR
    df_mr_norm = df_mr.copy()
    df_mr_norm['data'] = pd.to_datetime(df_mr_norm['data'], errors='coerce').dt.date
    df_mr_norm['valor'] = pd.to_numeric(df_mr_norm['valor'], errors='coerce')
    df_mr_norm['contato_norm'] = df_mr_norm['contato'].apply(
        lambda x: unidecode.unidecode(str(x)).lower().strip() if pd.notna(x) else ""
    )
    
    # Normalizar transações OFX
    df_trans_norm = df_transacoes_ofx.copy()
    df_trans_norm['data_norm'] = pd.to_datetime(df_trans_norm['data'], errors='coerce').dt.date
    df_trans_norm['valor_norm'] = pd.to_numeric(df_trans_norm['valor_absoluto'], errors='coerce')
    df_trans_norm['descricao_norm'] = df_trans_norm['descricao'].apply(
        lambda x: unidecode.unidecode(str(x)).lower().strip() if pd.notna(x) else ""
    )
    
    resultados = []
    duplicatas_encontradas = 0
    
    for idx, transacao in df_trans_norm.iterrows():
        melhor_match = None
        melhor_score = 0
        tipo_match = "NOVO"
        
        data_trans = transacao['data_norm']
        valor_trans = transacao['valor_norm']
        
        if pd.isna(data_trans) or pd.isna(valor_trans):
            resultados.append({
                'index': idx,
                'status_validacao': 'NOVO',
                'match_score': 0,
                'observacao': 'Dados incompletos'
            })
            continue
        
        # Buscar matches no MR
        for _, lancamento_mr in df_mr_norm.iterrows():
            data_mr = lancamento_mr['data']
            valor_mr = lancamento_mr['valor']
            
            if pd.isna(data_mr) or pd.isna(valor_mr):
                continue
            
            # Verificar tolerância de data
            diff_dias = abs((data_trans - data_mr).days)
            if diff_dias > tolerancia_dias:
                continue
            
            # Verificar tolerância de valor
            diff_valor_perc = abs(valor_trans - valor_mr) / valor_mr if valor_mr != 0 else float('inf')
            if diff_valor_perc > tolerancia_valor:
                continue
            
            # Se chegou aqui, é um match válido
            score = 1.0 - (diff_dias / max(tolerancia_dias, 1)) * 0.3 - diff_valor_perc * 0.7
            
            if score > melhor_score:
                melhor_score = score
                melhor_match = lancamento_mr
                tipo_match = "DUPLICATA"
        
        if tipo_match == "DUPLICATA":
            duplicatas_encontradas += 1
            
        resultados.append({
            'index': idx,
            'status_validacao': tipo_match,
            'match_score': melhor_score,
            'observacao': f'Match encontrado (score: {melhor_score:.3f})' if tipo_match == 'DUPLICATA' else 'Transação nova'
        })
    
    print(f"🗑️ DEBUG VALIDAÇÃO - {duplicatas_encontradas} duplicatas encontradas")
    
    # Adicionar resultados ao DataFrame original
    df_resultado = df_transacoes_ofx.copy()
    
    # Criar DataFrame de resultados e fazer merge
    df_resultados = pd.DataFrame(resultados).set_index('index')
    df_resultado = df_resultado.join(df_resultados, how='left')
    
    # Preencher valores faltantes
    df_resultado['status_validacao'] = df_resultado['status_validacao'].fillna('NOVO')
    df_resultado['match_score'] = df_resultado['match_score'].fillna(0.0)
    df_resultado['observacao'] = df_resultado['observacao'].fillna('Sem validação')
    
    return df_resultado

def processar_titulo_brr_com_categorizacao_inteligente(df_confirmados, df_mr_historico):
    """
    Processa transações TITULO BRR com validação de duplicatas E categorização inteligente
    
    Args:
        df_confirmados: DataFrame com pagamentos confirmados do .RET
        df_mr_historico: DataFrame com histórico completo do MR
    
    Returns:
        dict com DataFrames separados por status
    """
    # ETAPA 1: Validação rigorosa de duplicatas (valor + data)
    df_com_validacao = validar_transacoes_robusta(
        df_transacoes=df_confirmados,
        df_mr=df_mr_historico,
        tolerancia_dias=0,  # Exato na data
        tolerancia_valor=0.001  # Quase exato no valor (0.1% tolerância)
    )
    
    # Separar apenas os NOVOS para categorização
    df_novos = df_com_validacao[df_com_validacao["Status"].str.contains("NOVO")].copy()
    df_duplicatas = df_com_validacao[~df_com_validacao["Status"].str.contains("NOVO")].copy()
    
    if df_novos.empty:
        return {
            'df_novos_categorizados': pd.DataFrame(),
            'df_duplicatas_descartadas': df_duplicatas,
            'resumo': 'Nenhuma transação nova encontrada'
        }
    
    # ETAPA 2: Categorização automática por similaridade de nomes (só nos novos)
    df_novos_categorizados = categorizar_automaticamente_por_nome(df_novos, df_mr_historico)
    
    # Separar categorizados automaticamente vs manuais
    df_auto_categorizados = df_novos_categorizados[df_novos_categorizados['Status_Categorizacao'] == 'AUTO'].copy()
    df_precisam_manual = df_novos_categorizados[df_novos_categorizados['Status_Categorizacao'] == 'MANUAL'].copy()
    
    return {
        'df_novos_categorizados': df_novos_categorizados,
        'df_auto_categorizados': df_auto_categorizados,
        'df_precisam_manual': df_precisam_manual,
        'df_duplicatas_descartadas': df_duplicatas,
        'stats': {
            'total_arquivo': len(df_confirmados),
            'duplicatas': len(df_duplicatas),
            'novos_total': len(df_novos),
            'auto_categorizados': len(df_auto_categorizados),
            'precisam_manual': len(df_precisam_manual)
        }
    }

def processar_ofx_com_categorizacao_inteligente(df_ofx, df_mr_historico):
    """
    Processa transações OFX com validação de duplicatas E categorização inteligente
    
    Args:
        df_ofx: DataFrame com transações OFX processadas
        df_mr_historico: DataFrame com histórico completo do MR
    
    Returns:
        dict com DataFrames separados por status
    """
    # ETAPA 1: Validação rigorosa de duplicatas (valor + data)
    df_com_validacao = validar_transacoes_ofx_robusta(
        df_transacoes_ofx=df_ofx,
        df_mr=df_mr_historico,
        tolerancia_dias=0,  # Exato na data
        tolerancia_valor=0.001  # Quase exato no valor (0.1% tolerância)
    )
    
    # Separar apenas os NOVOS para categorização
    df_novos = df_com_validacao[df_com_validacao['status_validacao'] == 'NOVO'].copy()
    df_duplicatas = df_com_validacao[df_com_validacao['status_validacao'] == 'DUPLICATA'].copy()
    
    # Estatísticas da validação
    stats_validacao = {
        'total_transacoes': len(df_ofx),
        'duplicatas_encontradas': len(df_duplicatas),
        'transacoes_novas': len(df_novos),
        'percentual_duplicatas': (len(df_duplicatas) / len(df_ofx)) * 100 if len(df_ofx) > 0 else 0
    }
    
    # ETAPA 2: Categorização automática por similaridade de nomes (só nos novos)
    df_novos_categorizados = categorizar_automaticamente_por_nome_ofx(df_novos, df_mr_historico)
    
    # Separar categorizados automaticamente vs manuais
    df_auto_categorizados = df_novos_categorizados[df_novos_categorizados['Status_Categorizacao'] == 'AUTO'].copy()
    df_precisam_manual = df_novos_categorizados[df_novos_categorizados['Status_Categorizacao'] == 'MANUAL'].copy()
    
    return {
        'df_novos_categorizados': df_novos_categorizados,
        'df_auto_categorizados': df_auto_categorizados,
        'df_precisam_manual': df_precisam_manual,
        'df_duplicatas_descartadas': df_duplicatas,
        'stats': {
            'total_arquivo': len(df_ofx),
            'duplicatas': len(df_duplicatas),
            'novos_total': len(df_novos),
            'auto_categorizados': len(df_auto_categorizados),
            'precisam_manual': len(df_precisam_manual)
        }
    }

def criar_excel_mr_titulo_brr(df_dados, banco_selecionado, nome_aba="Planilha1"):
    """
    Cria Excel no formato do sistema MR para transações TITULO BRR
    """
    # Criar DataFrame vazio primeiro
    df_mr = pd.DataFrame(index=df_dados.index)
    
    # Preencher coluna Confirmado com "sim" para todos os registros
    df_mr['Confirmado'] = 'sim'
    
    # Verificar qual coluna de data está disponível (mais opções)
    data_col = None
    for col in ['Data Pagamento', 'Data', 'data_pagamento', 'data']:
        if col in df_dados.columns:
            data_col = col
            break
    
    if data_col:
        df_mr['Data'] = pd.to_datetime(df_dados[data_col], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
    else:
        # Se não encontrar nenhuma coluna de data, usar data atual
        df_mr['Data'] = datetime.now().strftime('%d/%m/%Y')
    
    # Verificar categoria (priorizar categorização automática)
    if 'Categoria_Auto' in df_dados.columns and df_dados['Categoria_Auto'].notna().any():
        df_mr['Categoria'] = df_dados['Categoria_Auto'].fillna('MANUAL')
    elif 'Categoria_IA' in df_dados.columns:
        df_mr['Categoria'] = df_dados['Categoria_IA']
    elif 'Categoria' in df_dados.columns:
        df_mr['Categoria'] = df_dados['Categoria']
    else:
        df_mr['Categoria'] = 'MANUAL'
    
    # Verificar descrição/favorecido (mais opções)
    desc_col = None
    for col in ['Favorecido', 'Nome Favorecido', 'Descrição', 'descricao']:
        if col in df_dados.columns:
            desc_col = col
            break
    
    if desc_col:
        df_mr['Descrição'] = df_dados[desc_col]
    else:
        df_mr['Descrição'] = 'Pagamento TITULO BRR'
    
    df_mr['Conta'] = banco_selecionado
    
    # Converter valor para formato numérico (mais opções)
    valor_col = None
    for col in ['Valor', 'Valor Pago (R$)', 'valor', 'valor_pago']:
        if col in df_dados.columns:
            valor_col = col
            break
    
    if valor_col:
        if 'Valor Pago (R$)' == valor_col:
            # Converter formato brasileiro para numérico
            valores = df_dados[valor_col].str.replace(".", "", regex=False).str.replace(",", ".").astype(float)
            df_mr['Valor'] = valores
        else:
            df_mr['Valor'] = df_dados[valor_col]
    else:
        df_mr['Valor'] = 0.0
    
    # Verificar contato (priorizar categorização automática)
    if 'Contato_Auto' in df_dados.columns and df_dados['Contato_Auto'].notna().any():
        # Usar contatos da categorização automática quando disponível
        df_mr['Contato'] = df_dados['Contato_Auto'].fillna(df_mr['Descrição'])
    elif desc_col:
        contatos_originais = df_dados[desc_col]
        contatos_formatados = []
        
        # Tentar mapear cada contato com padrão do MR (CODIGO - NOME)
        for contato in contatos_originais:
            contato_str = str(contato).strip()
            
            # Se já tem formato "XXXX - NOME", manter
            if " - " in contato_str and contato_str.split(" - ")[0].isdigit():
                contatos_formatados.append(contato_str)
            else:
                # Tentar extrair código se existe no início
                palavras = contato_str.split()
                if palavras and palavras[0].isdigit():
                    codigo = palavras[0]
                    nome = " ".join(palavras[1:])
                    contatos_formatados.append(f"{codigo} - {nome}")
                else:
                    # Se não tem código, usar nome original
                    contatos_formatados.append(contato_str)
        
        df_mr['Contato'] = contatos_formatados
    else:
        df_mr['Contato'] = df_mr['Descrição']
    
    # Criar Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_mr.to_excel(writer, sheet_name=nome_aba, index=False)
    output.seek(0)
    
    return output, df_mr

def criar_excel_mr_ofx(df_dados, banco_selecionado, nome_aba="Planilha1"):
    """
    Cria Excel no formato do sistema MR para transações OFX
    
    Args:
        df_dados: DataFrame com transações OFX categorizadas
        banco_selecionado: Nome do banco para usar na coluna Conta
        nome_aba: Nome da aba no Excel
        
    Returns:
        tuple: (BytesIO do Excel, DataFrame formatado)
    """
    # Criar DataFrame vazio primeiro
    df_mr = pd.DataFrame(index=df_dados.index)
    
    # Preencher coluna Confirmado com "Sim" para todos os registros
    df_mr['Confirmado'] = 'sim'
    
    # Data - usar a coluna 'data' das transações OFX
    if 'data' in df_dados.columns:
        df_mr['Data'] = pd.to_datetime(df_dados['data'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
    else:
        # Se não encontrar coluna de data, usar data atual
        df_mr['Data'] = datetime.now().strftime('%d/%m/%Y')
    
    # Categoria - priorizar categorização automática
    if 'Categoria_Auto' in df_dados.columns and df_dados['Categoria_Auto'].notna().any():
        df_mr['Categoria'] = df_dados['Categoria_Auto'].fillna('MANUAL')
    elif 'Categoria' in df_dados.columns:
        df_mr['Categoria'] = df_dados['Categoria']
    else:
        df_mr['Categoria'] = 'MANUAL'
    
    # Descrição - usar a coluna 'descricao' das transações OFX
    if 'descricao' in df_dados.columns:
        df_mr['Descrição'] = df_dados['descricao']
    else:
        df_mr['Descrição'] = 'Transação OFX'
    
    # Conta - usar o banco selecionado
    df_mr['Conta'] = banco_selecionado
    
    # Valor - usar a coluna 'valor_absoluto' das transações OFX
    if 'valor_absoluto' in df_dados.columns:
        df_mr['Valor'] = pd.to_numeric(df_dados['valor_absoluto'], errors='coerce')
    elif 'valor' in df_dados.columns:
        df_mr['Valor'] = pd.to_numeric(df_dados['valor'], errors='coerce')
    else:
        df_mr['Valor'] = 0.0
    
    # Contato - priorizar categorização automática
    if 'Contato_Auto' in df_dados.columns and df_dados['Contato_Auto'].notna().any():
        # Usar contatos da categorização automática quando disponível
        df_mr['Contato'] = df_dados['Contato_Auto'].fillna(df_mr['Descrição'])
    else:
        # Tentar formatar contatos com base na descrição
        contatos_originais = df_dados['descricao'] if 'descricao' in df_dados.columns else df_mr['Descrição']
        contatos_formatados = []
        
        for contato in contatos_originais:
            contato_str = str(contato).strip()
            
            # Se já tem formato "XXXX - NOME", manter
            if " - " in contato_str and contato_str.split(" - ")[0].isdigit():
                contatos_formatados.append(contato_str)
            else:
                # Tentar extrair código se existe no início
                palavras = contato_str.split()
                if palavras and palavras[0].isdigit():
                    codigo = palavras[0]
                    nome = " ".join(palavras[1:])
                    contatos_formatados.append(f"{codigo} - {nome}")
                else:
                    # Se não tem código, usar nome original
                    contatos_formatados.append(contato_str)
        
        df_mr['Contato'] = contatos_formatados
    
    # Criar Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_mr.to_excel(writer, sheet_name=nome_aba, index=False)
    output.seek(0)
    
    return output, df_mr

def processar_arquivos_retorno_banrisul(arquivos_retorno, processor):
    """
    Processa arquivos de retorno para o processor TITULO BRR
    
    Args:
        arquivos_retorno: Lista de arquivos uploaded do Streamlit
        processor: Instância do BanrisulTituloBRRProcessor
    
    Returns:
        dict: Resultado do processamento
    """
    arquivos_processados = 0
    erros = []
    
    try:
        for arquivo in arquivos_retorno:
            try:
                # Ler conteúdo do arquivo
                conteudo = arquivo.read()
                
                # Tentar decodificar com diferentes encodings
                texto_arquivo = None
                for encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
                    try:
                        texto_arquivo = conteudo.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                
                if texto_arquivo is None:
                    erros.append(f"Erro de encoding no arquivo: {arquivo.name}")
                    continue
                
                # Processar arquivo de retorno
                sucesso = processor.processar_arquivo_retorno(arquivo.name, texto_arquivo)
                
                if sucesso:
                    arquivos_processados += 1
                else:
                    erros.append(f"Erro ao processar: {arquivo.name}")
                    
            except Exception as e:
                erros.append(f"Erro no arquivo {arquivo.name}: {str(e)}")
        
        return {
            'arquivos_processados': arquivos_processados,
            'erros': erros,
            'total_arquivos': len(arquivos_retorno)
        }
        
    except Exception as e:
        return {
            'arquivos_processados': 0,
            'erros': [f"Erro geral: {str(e)}"],
            'total_arquivos': len(arquivos_retorno)
        }

def render_titulo_brr_tab():
    """Renderiza a aba de detalhamento TITULO BRR"""
    
    st.subheader("🏦 Detalhamento de Pagamentos TITULO BRR (Banrisul)")
    st.markdown("Esta seção permite detalhar transações genéricas do Banrisul usando arquivos de retorno específicos.")
    
    # Verificar se existem transações genéricas detectadas
    if 'titulo_brr_processor' in st.session_state and 'titulo_brr_stats' in st.session_state:
        stats = st.session_state.titulo_brr_stats
        
        # Mostrar estatísticas das transações genéricas detectadas
        st.markdown("### 📊 Transações Genéricas Detectadas no OFX")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("🏦 Total Genéricas", stats['transacoes_genericas'])
        with col2:
            st.metric("💰 Valor Total", f"R$ {stats['valor_total_generico']:,.2f}")
        with col3:
            st.metric("📊 % do Total", f"{stats['percentual_generico']:.1f}%")
        with col4:
            st.metric("✅ Normais", stats['transacoes_normais'])
        
        # Mostrar transações genéricas detectadas
        if stats['transacoes_genericas'] > 0:
            st.markdown("### 📋 Transações TITULO BRR Detectadas")
            
            df_genericas = stats['df_genericas'].copy()
            
            # Formatar para exibição
            df_display = df_genericas[['data', 'valor_absoluto', 'descricao', 'banco_nome']].copy()
            df_display['Data'] = pd.to_datetime(df_display['data']).dt.strftime('%d/%m/%Y')
            df_display['Valor'] = df_display['valor_absoluto'].apply(
                lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            )
            df_display = df_display[['Data', 'Valor', 'descricao', 'banco_nome']]
            df_display.columns = ['Data', 'Valor', 'Descrição', 'Banco']
            
            st.dataframe(df_display, width="stretch", hide_index=True)
            
            st.markdown("---")
    
    # =====================================
    # 📁 UPLOAD E PROCESSAMENTO DE ARQUIVOS .RET
    # =====================================
    
    st.markdown("### 📁 Upload de Arquivos de Retorno (.RET)")
    st.info("**Faça upload dos arquivos de retorno (.RET) para extrair os pagamentos específicos e verificar contra o sistema MR.**")
    
    # Estado da sessão para arquivos RET
    if "df_titulo_brr_ret" not in st.session_state:
        st.session_state.df_titulo_brr_ret = None
    if "uploader_key_titulo_brr" not in st.session_state:
        st.session_state.uploader_key_titulo_brr = 0
    
    # Upload de arquivos .RET
    arquivos_ret = st.file_uploader(
        "Selecione os arquivos de retorno (.RET)",
        accept_multiple_files=True,
        type=['ret', 'txt', 'RET', 'TXT'],
        key=f"uploader_titulo_brr_{st.session_state.uploader_key_titulo_brr}",
        help="Selecione os arquivos de retorno CNAB240 com os pagamentos específicos"
    )
    
    # Processamento dos arquivos
    if arquivos_ret:
        st.success(f"✅ {len(arquivos_ret)} arquivo(s) carregado(s)")
        
        all_registros = []
        
        # Processar cada arquivo
        for arquivo in arquivos_ret:
            try:
                # Ler conteúdo do arquivo
                conteudo = arquivo.read()
                
                # Tentar decodificar com diferentes encodings
                texto_arquivo = None
                for encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
                    try:
                        texto_arquivo = conteudo.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                
                if texto_arquivo is None:
                    st.error(f"❌ Erro de encoding no arquivo: {arquivo.name}")
                    continue
                
                # Usar a mesma função do Retorno_Excel.py
                df_arquivo = ler_cnab240_segmento_j(texto_arquivo)
                
                if not df_arquivo.empty:
                    # Adicionar nome do arquivo para identificação
                    df_arquivo['Arquivo'] = arquivo.name
                    all_registros.append(df_arquivo)
                    st.success(f"📄 {arquivo.name}: {len(df_arquivo)} pagamentos encontrados")
                else:
                    st.warning(f"⚠️ {arquivo.name}: Nenhum pagamento (Segmento J) encontrado")
                    
            except Exception as e:
                st.error(f"❌ Erro ao processar {arquivo.name}: {str(e)}")
        
        # Consolidar todos os registros
        if all_registros:
            df_consolidado = pd.concat(all_registros, ignore_index=True)
            st.session_state.df_titulo_brr_ret = df_consolidado
            
            st.success(f"� **Total: {len(df_consolidado)} pagamentos extraídos de {len(all_registros)} arquivo(s)**")
        else:
            st.error("❌ Nenhum pagamento foi extraído dos arquivos enviados")
    
    # =====================================
    # 📊 EXIBIÇÃO DOS PAGAMENTOS EXTRAÍDOS
    # =====================================
    
    if st.session_state.df_titulo_brr_ret is not None:
        st.markdown("---")
        st.markdown("### 📊 Pagamentos Extraídos dos Arquivos .RET")
        
        df_ret = st.session_state.df_titulo_brr_ret.copy()
        
        # Filtrar apenas pagamentos confirmados (código BD)
        df_confirmados = df_ret[df_ret["Codigo"] == "BD"].copy()
        
        if not df_confirmados.empty:
            st.info(f"✅ **{len(df_confirmados)} pagamentos confirmados** (código BD) encontrados")
            
            # Mostrar resumo por arquivo
            resumo_arquivos = df_confirmados.groupby('Arquivo').agg({
                'Valor Pago (R$)': 'count',
                'Favorecido': 'nunique'
            }).rename(columns={
                'Valor Pago (R$)': 'Qtd Pagamentos',
                'Favorecido': 'Qtd Favorecidos'
            })
            
            st.markdown("**📋 Resumo por Arquivo:**")
            st.dataframe(resumo_arquivos, use_container_width=True)
            
            # Exibir todos os pagamentos
            st.markdown("**� Todos os Pagamentos Extraídos:**")
            
            # Preparar DataFrame para exibição
            df_display = df_confirmados[['Arquivo', 'Favorecido', 'Data Pagamento', 'Valor Pago (R$)', 'Descrição']].copy()
            
            st.dataframe(df_display, use_container_width=True)
            
            # =====================================
            # � VERIFICAÇÃO CONTRA SISTEMA MR
            # =====================================
            
            st.markdown("---")
            with st.expander("� **Verificar Pagamentos contra Sistema MR**", expanded=True):
                
                # Seleção de empresa
                empresa_nome = st.selectbox("Selecione a empresa (MR):", list(EMPRESAS_MR.keys()))
                id_empresa = EMPRESAS_MR[empresa_nome]
                
                if st.button("🔄 Buscar dados da MR e Analisar com IA"):
                    with st.spinner("🔄 Buscando dados da MR..."):
                        
                        # Buscar dados da API MR
                        df_api_mr = buscar_lancamentos_api(ids_empresa=id_empresa, anos="2024,2025")
                        
                        if df_api_mr.empty or "data" not in df_api_mr.columns or "valor" not in df_api_mr.columns:
                            st.warning("⚠️ Nenhum dado útil retornado da API da MR ou estrutura inesperada.")
                        else:
                            st.success(f"✅ {len(df_api_mr)} registros carregados da MR para análise robusta.")
                            
                            # ====================================
                            # 🧠 VALIDAÇÃO + CATEGORIZAÇÃO INTELIGENTE
                            # ====================================
                            
                            with st.spinner("🤖 Processando com IA - validação + categorização automática..."):
                                resultado_inteligente = processar_titulo_brr_com_categorizacao_inteligente(
                                    df_confirmados=df_confirmados,
                                    df_mr_historico=df_api_mr
                                )
                            
                            # Extrair resultados
                            stats = resultado_inteligente['stats']
                            df_auto_categorizados = resultado_inteligente['df_auto_categorizados']
                            df_precisam_manual = resultado_inteligente['df_precisam_manual']
                            df_duplicatas = resultado_inteligente['df_duplicatas_descartadas']
                            df_novos_categorizados = resultado_inteligente['df_novos_categorizados']
                            
                            # ====================================
                            # 📊 ESTATÍSTICAS INTELIGENTES
                            # ====================================
                            
                            st.markdown("### 🧠 Análise Inteligente - Resultados")
                            
                            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
                            with col_stat1:
                                st.metric("📊 Total Arquivo", stats['total_arquivo'])
                            with col_stat2:
                                st.metric("❌ Duplicatas", stats['duplicatas'], delta=f"-{stats['duplicatas']}")
                            with col_stat3:
                                st.metric("🤖 Auto-Categorizados", stats['auto_categorizados'], delta=f"+{stats['auto_categorizados']}")
                            with col_stat4:
                                st.metric("👤 Precisam Manual", stats['precisam_manual'], delta=f"manual: {stats['precisam_manual']}")
                            
                            # ====================================
                            # 🎯 RESULTADOS DETALHADOS
                            # ====================================
                            
                            # DUPLICATAS DESCARTADAS
                            if not df_duplicatas.empty:
                                with st.expander(f"❌ **Duplicatas Descartadas** ({len(df_duplicatas)} registros)", expanded=False):
                                    st.markdown("*Estes pagamentos já existem no MR com mesmo valor e data*")
                                    st.dataframe(df_duplicatas, use_container_width=True)
                            
                            # AUTO-CATEGORIZADOS
                            if not df_auto_categorizados.empty:
                                with st.expander(f"🤖 **Auto-Categorizados** ({len(df_auto_categorizados)} registros)", expanded=True):
                                    st.markdown("*Categorias aplicadas automaticamente baseadas no histórico do MR*")
                                    colunas_exibir = ['Favorecido', 'Valor', 'Categoria_Auto', 'Contato_Auto', 'Match_Score']
                                    colunas_disponiveis = [col for col in colunas_exibir if col in df_auto_categorizados.columns]
                                    st.dataframe(df_auto_categorizados[colunas_disponiveis], use_container_width=True)
                            
                            # PRECISAM CATEGORIZAÇÃO MANUAL
                            if not df_precisam_manual.empty:
                                with st.expander(f"👤 **Precisam Categorização Manual** ({len(df_precisam_manual)} registros)", expanded=True):
                                    st.markdown("*Estes pagamentos não encontraram match automático e precisam de categorização manual*")
                                    colunas_exibir = ['Favorecido', 'Valor', 'Status_Categorizacao']
                                    colunas_disponiveis = [col for col in colunas_exibir if col in df_precisam_manual.columns]
                                    st.dataframe(df_precisam_manual[colunas_disponiveis], use_container_width=True)
                            
                            # ====================================
                            # � CATEGORIZAÇÃO E DOWNLOAD
                            # ====================================
                            
                            if not df_novos_categorizados.empty:
                                st.markdown(f"### � Finalização - Novos Pagamentos ({len(df_novos_categorizados)})")
                                
                                # Usar df_novos_categorizados como base
                                df_novos_com_ia = df_novos_categorizados.copy()
                                
                                # Adicionar colunas de compatibilidade se não existirem
                                if 'Categoria_IA' not in df_novos_com_ia.columns:
                                    df_novos_com_ia['Categoria_IA'] = df_novos_com_ia.get('Categoria_Auto', 'MANUAL')
                                
                                if 'Confianca_IA' not in df_novos_com_ia.columns:
                                    # Converter Match_Score para confiança (>= 0.8 = alta confiança)
                                    df_novos_com_ia['Confianca_IA'] = df_novos_com_ia.get('Match_Score', 0.0)
                                
                                if 'Tipo_Categorizacao' not in df_novos_com_ia.columns:
                                    df_novos_com_ia['Tipo_Categorizacao'] = df_novos_com_ia['Status_Categorizacao'].map({
                                        'AUTO': '🤖 Automática',
                                        'MANUAL': '👤 Manual'
                                    })
                                
                                # Separar automáticos vs manuais
                                df_auto_ia = df_novos_com_ia[df_novos_com_ia['Status_Categorizacao'] == 'AUTO'].copy()
                                df_manual = df_novos_com_ia[df_novos_com_ia['Status_Categorizacao'] == 'MANUAL'].copy()
                                
                                # Mostrar automáticos
                                if not df_auto_ia.empty:
                                    st.markdown(f"#### 🤖 Categorizados Automaticamente ({len(df_auto_ia)})")
                                    st.success(f"✅ Sistema categorizou {len(df_auto_ia)} pagamentos automaticamente")
                                    st.dataframe(df_auto_ia[['Favorecido', 'Valor', 'Categoria_IA', 'Confianca_IA', 'Tipo_Categorizacao']], use_container_width=True)
                                
                                # ====================================
                                # 🎯 DECISÃO SIMPLES DO USUÁRIO  
                                # ====================================
                                
                                st.markdown("---")
                                st.markdown("### 🎯 Finalizar Categorização")
                                
                                # Verificar se há categorizações automáticas
                                tem_categorias_ia = not df_auto_ia.empty
                                tem_manuais = not df_manual.empty
                                
                                if tem_categorias_ia:
                                    st.success(f"✅ **{len(df_auto_ia)} pagamentos** categorizados automaticamente pela IA")
                                    
                                if tem_manuais:
                                    st.warning(f"⚠️ **{len(df_manual)} pagamentos** precisam de categorização manual")
                                
                                # Inicializar estado se não existir
                                if 'modo_categorizacao_titulo_brr' not in st.session_state:
                                    st.session_state.modo_categorizacao_titulo_brr = None
                                
                                # Se não há categorizações automáticas, forçar categorização manual
                                if not tem_categorias_ia:
                                    st.info("🎯 **Todos os pagamentos precisam de categorização manual**")
                                    st.session_state.modo_categorizacao_titulo_brr = 'manual'
                                else:
                                    # Dois botões principais
                                    col_btn1, col_btn2 = st.columns(2)
                                    
                                    with col_btn1:
                                        if st.button(
                                            "✅ Manter Categorização",
                                            help="Aceita as categorias da IA e prepara dados para Sistema MR",
                                            use_container_width=True,
                                            type="primary"
                                        ):
                                            st.session_state.modo_categorizacao_titulo_brr = 'manter'
                                    
                                    with col_btn2:
                                        if st.button(
                                            "👤 Categorizar Manualmente", 
                                            help="Abre interface para categorização manual dos pagamentos",
                                            use_container_width=True,
                                            type="secondary"
                                        ):
                                            st.session_state.modo_categorizacao_titulo_brr = 'manual'
                                
                                # ====================================
                                # ✅ MANTER CATEGORIZAÇÃO
                                # ====================================
                                
                                if st.session_state.modo_categorizacao_titulo_brr == 'manter' and tem_categorias_ia:
                                    st.markdown("### ✅ Preparando Excel para Sistema MR")
                                    
                                    # Obter banco Banrisul
                                    banco_info = obter_banco_banrisul_titulo_brr()
                                    
                                    if banco_info is None:
                                        st.error("❌ **Erro**: Nenhuma conta Banrisul encontrada nos dados OFX carregados")
                                        st.info("💡 **Solução**: Certifique-se de ter processado arquivos OFX do Banrisul primeiro")
                                    
                                    elif isinstance(banco_info, str):
                                        # Uma única conta Banrisul
                                        st.success(f"🏦 **Conta automática**: {banco_info}")
                                        
                                        # Criar Excel MR
                                        with st.spinner("📊 Gerando Excel para sistema MR..."):
                                            excel_mr, df_mr_preview = criar_excel_mr_titulo_brr(
                                                df_auto_ia, 
                                                banco_info
                                            )
                                        
                                        st.success("🎉 **Excel MR gerado com sucesso!**")
                                        
                                        # Preview do Excel
                                        st.markdown("#### 📄 Preview do Excel MR")
                                        st.dataframe(df_mr_preview.head(10), use_container_width=True)
                                        
                                        # Download
                                        st.download_button(
                                            label="📥 Baixar Excel TITULO BRR para MR",
                                            data=excel_mr,
                                            file_name=f"lancamentos_titulo_brr_categorizados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                        )
                                        
                                        # Salvar na sessão para consolidação futura
                                        if 'excel_mr_consolidado' not in st.session_state:
                                            st.session_state.excel_mr_consolidado = {}
                                        st.session_state.excel_mr_consolidado['titulo_brr'] = df_mr_preview
                                        
                                        st.info("💾 **Dados salvos** para consolidação na aba Excel")
                                    
                                    else:
                                        # Múltiplas contas Banrisul - usuário escolhe
                                        st.warning("🏦 **Múltiplas contas Banrisul encontradas**")
                                        
                                        banco_selecionado = st.selectbox(
                                            "Selecione a conta Banrisul para os pagamentos TITULO BRR:",
                                            banco_info,
                                            key="select_banco_titulo_brr"
                                        )
                                        
                                        # Gerar Excel automaticamente quando banco é selecionado
                                        with st.spinner("📊 Gerando Excel para sistema MR..."):
                                            excel_mr, df_mr_preview = criar_excel_mr_titulo_brr(
                                                df_auto_ia, 
                                                banco_selecionado
                                            )
                                        
                                        st.success("🎉 **Excel MR gerado com sucesso!**")
                                        
                                        # Preview do Excel
                                        st.markdown("#### 📄 Preview do Excel MR")
                                        st.dataframe(df_mr_preview.head(10), use_container_width=True)
                                        
                                        # Download
                                        st.download_button(
                                            label="📥 Baixar Excel TITULO BRR para MR",
                                            data=excel_mr,
                                            file_name=f"lancamentos_titulo_brr_categorizados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key="download_banco_selecionado",
                                            type="primary"
                                        )
                                        
                                        # Salvar na sessão para consolidação futura
                                        if 'excel_mr_consolidado' not in st.session_state:
                                            st.session_state.excel_mr_consolidado = {}
                                        st.session_state.excel_mr_consolidado['titulo_brr'] = df_mr_preview
                                        
                                        st.info("💾 **Dados salvos** para consolidação na aba Excel")
                                
                                # ====================================
                                # 👤 CATEGORIZAÇÃO MANUAL
                                # ====================================
                                
                                if st.session_state.modo_categorizacao_titulo_brr == 'manual':
                                    st.markdown("### 👤 Categorização Manual")
                                    
                                    # Botão para voltar
                                    if st.button("⬅️ Voltar", key="voltar_manual"):
                                        st.session_state.modo_categorizacao_titulo_brr = None
                                        st.rerun()
                                    
                                    # Buscar categorias disponíveis no MR
                                    with st.spinner("📋 Carregando categorias do sistema MR..."):
                                        df_categorias_mr = buscar_categorias_api()
                                    
                                    if not df_categorias_mr.empty:
                                        st.success(f"✅ {len(df_categorias_mr)} categorias carregadas do sistema MR")
                                        
                                        # Interface para categorização manual
                                        st.markdown("**Selecione categorias para os pagamentos:**")
                                        
                                        # Usar df_novos_com_ia para ter todos os pagamentos
                                        df_para_categorizar = df_novos_com_ia.copy()
                                        
                                        categorias_manuais = {}
                                        opcoes_categorias = ["[Selecionar categoria]"] + df_categorias_mr['categoria_nome'].tolist()
                                        
                                        # Usar todos os pagamentos que precisam de categorização
                                        for idx, row in df_para_categorizar.iterrows():
                                            col_info, col_cat = st.columns([2, 1])
                                            
                                            with col_info:
                                                st.write(f"**{row['Favorecido']}** - R$ {row['Valor']:,.2f}")
                                                if row['Categoria_IA'] != "MANUAL" and row['Confianca_IA'] > 0:
                                                    st.caption(f"💡 IA sugeriu: *{row['Categoria_IA']}* (confiança: {row['Confianca_IA']:.1%})")
                                            
                                            with col_cat:
                                                categoria_selecionada = st.selectbox(
                                                    "Categoria:",
                                                    opcoes_categorias,
                                                    key=f"cat_manual_{idx}",
                                                    label_visibility="collapsed"
                                                )
                                                
                                                if categoria_selecionada != "[Selecionar categoria]":
                                                    categorias_manuais[idx] = categoria_selecionada
                                        
                                        # Aplicar e gerar Excel
                                        if categorias_manuais:
                                            if st.button("✅ Finalizar Categorização Manual"):
                                                # Aplicar categorias
                                                for idx, categoria in categorias_manuais.items():
                                                    df_novos_com_ia.loc[idx, 'Categoria_IA'] = categoria
                                                    df_novos_com_ia.loc[idx, 'Tipo_Categorizacao'] = "👤 Manual"
                                                    df_novos_com_ia.loc[idx, 'Confianca_IA'] = 1.0
                                                
                                                st.success(f"✅ {len(categorias_manuais)} categorias aplicadas!")
                                                
                                                # Preparar Excel
                                                df_manual_categorizados = df_novos_com_ia[df_novos_com_ia.index.isin(categorias_manuais.keys())]
                                                
                                                # Obter banco
                                                banco_info = obter_banco_banrisul_titulo_brr()
                                                
                                                if banco_info is None:
                                                    st.error("❌ **Erro**: Nenhuma conta Banrisul encontrada")
                                                    st.info("💡 Certifique-se de ter processado arquivos OFX do Banrisul primeiro")
                                                
                                                elif isinstance(banco_info, str):
                                                    # Gerar Excel diretamente
                                                    excel_mr, df_mr_preview = criar_excel_mr_titulo_brr(
                                                        df_manual_categorizados, 
                                                        banco_info
                                                    )
                                                    
                                                    st.success("🎉 **Excel MR gerado!**")
                                                    st.dataframe(df_mr_preview, use_container_width=True)
                                                    
                                                    st.download_button(
                                                        label="📥 Baixar Excel Categorização Manual",
                                                        data=excel_mr,
                                                        file_name=f"titulo_brr_manual_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                                    )
                                                else:
                                                    st.info("🏦 **Selecione o banco** na seção anterior para gerar o Excel")
                                        
                                        else:
                                            st.warning("⚠️ Selecione pelo menos uma categoria para continuar")
                                    
                                    else:
                                        st.error("❌ Não foi possível carregar categorias do sistema MR")
                                
                                # ====================================
                                # 📥 DOWNLOAD EXCEL SISTEMA MR (SEMPRE VISÍVEL)
                                # ====================================
                                
                                # Mostrar botão de download sempre que há dados novos
                                if not df_novos_categorizados.empty:
                                    st.markdown("---")
                                    st.markdown("### 📥 Download para Sistema MR")
                                    
                                    # Usar dados da categorização inteligente
                                    df_para_download = df_novos_categorizados
                                    
                                    col_info_mr, col_btn_mr = st.columns([2, 1])
                                    
                                    with col_info_mr:
                                        if not df_auto_categorizados.empty:
                                            st.success(f"📊 **{len(df_para_download)} lançamentos processados** prontos para importação no Sistema MR")
                                            st.caption(f"✅ {len(df_auto_categorizados)} auto-categorizados + {len(df_precisam_manual)} manuais")
                                        else:
                                            st.info(f"📊 **{len(df_para_download)} lançamentos novos** prontos para importação no Sistema MR")
                                            st.caption("⚠️ Todos precisam de categorização manual")
                                    
                                    with col_btn_mr:
                                        # Obter banco Banrisul
                                        banco_info = obter_banco_banrisul_titulo_brr()
                                        
                                        if banco_info is None:
                                            st.error("❌ **Erro**: Nenhuma conta Banrisul encontrada")
                                            st.info("💡 Certifique-se de ter processado arquivos OFX do Banrisul primeiro")
                                        
                                        elif isinstance(banco_info, str):
                                            # Uma conta - preparar Excel imediatamente
                                            with st.spinner("📊 Preparando Excel para Sistema MR..."):
                                                # Criar Excel MR apenas com os NOVOS
                                                excel_mr, df_mr_preview = criar_excel_mr_titulo_brr(
                                                    df_para_download, 
                                                    banco_info
                                                )
                                            
                                            st.success(f"✅ **Excel gerado** com {len(df_mr_preview)} lançamentos!")
                                            
                                            # Download direto sem botão intermediário
                                            st.download_button(
                                                label="📥 Baixar Excel Sistema MR",
                                                data=excel_mr,
                                                file_name=f"lancamentos_sistema_mr_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                key="download_mr_final",
                                                type="primary"
                                            )
                                            
                                            # Preview
                                            with st.expander("👀 **Preview do Excel gerado**"):
                                                st.dataframe(df_mr_preview, use_container_width=True)
                                        
                                        elif isinstance(banco_info, (list, tuple)) and len(banco_info) > 1:
                                            # Múltiplas contas - usuário escolhe
                                            banco_selecionado = st.selectbox(
                                                "Conta Banrisul:",
                                                banco_info,
                                                key="select_banco_mr_principal"
                                            )
                                            
                                            # Preparar Excel com banco selecionado
                                            with st.spinner("📊 Preparando Excel para Sistema MR..."):
                                                excel_mr, df_mr_preview = criar_excel_mr_titulo_brr(
                                                    df_para_download, 
                                                    banco_selecionado
                                                )
                                            
                                            st.success(f"✅ **Excel gerado** com {len(df_mr_preview)} lançamentos!")
                                            
                                            st.download_button(
                                                label="📥 Baixar Excel Sistema MR",
                                                data=excel_mr,
                                                file_name=f"lancamentos_sistema_mr_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                key="download_mr_multi_final",
                                                type="primary"
                                            )
                                            
                                            with st.expander("👀 **Preview do Excel gerado**"):
                                                st.dataframe(df_mr_preview, use_container_width=True)
                                        
                                        else:
                                            st.error("❌ **Erro**: Nenhuma conta Banrisul encontrada")
                                            st.info("💡 Certifique-se de ter processado arquivos OFX do Banrisul primeiro")
                                
                                if 'Status_Final' in df_novos_com_ia.columns:
                                    st.markdown("---")
                                    st.markdown("### 📊 Resumo Final da Categorização")
                                    
                                    df_finalizados = df_novos_com_ia[df_novos_com_ia['Status_Final'].notna()]
                                    df_pendentes = df_novos_com_ia[df_novos_com_ia['Status_Final'].isna()]
                                    
                                    col_final1, col_final2, col_final3 = st.columns(3)
                                    
                                    with col_final1:
                                        st.metric("✅ Categorizados", len(df_finalizados))
                                    with col_final2:
                                        st.metric("⏳ Pendentes", len(df_pendentes))
                                    with col_final3:
                                        st.metric("� Total", len(df_novos_com_ia))
                                    
                                    if not df_finalizados.empty:
                                        st.success("🎉 **Categorização concluída!**")
                                        st.dataframe(df_finalizados[['Favorecido', 'Valor', 'Categoria_IA', 'Tipo_Categorizacao', 'Status_Final']], use_container_width=True)
                                    
                                    if not df_pendentes.empty:
                                        st.warning(f"⚠️ **{len(df_pendentes)} pagamentos ainda precisam de categorização**")
                                
                                # ====================================
                                # 📥 DOWNLOADS CATEGORIZADOS
                                # ====================================
                                
                                st.markdown("---")
                                st.markdown("### 📥 Downloads Categorizados")
                                
                                col_down1, col_down2, col_down3 = st.columns(3)
                                
                                with col_down1:
                                    if not df_novos_categorizados.empty:
                                        output_completo = io.BytesIO()
                                        df_novos_categorizados.to_excel(output_completo, index=False)
                                        output_completo.seek(0)
                                        
                                        st.download_button(
                                            label="📊 Análise Completa",
                                            data=output_completo,
                                            file_name=f"analise_titulo_brr_completa_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                        )
                                
                                with col_down2:
                                    if not df_auto_categorizados.empty:
                                        output_auto = io.BytesIO()
                                        df_auto_categorizados.to_excel(output_auto, index=False)
                                        output_auto.seek(0)
                                        
                                        st.download_button(
                                            label="🤖 Auto-Categorizados",
                                            data=output_auto,
                                            file_name=f"titulo_brr_auto_categorizados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                        )
                                
                                with col_down3:
                                    if not df_duplicatas.empty:
                                        output_duplicatas = io.BytesIO()
                                        df_duplicatas.to_excel(output_duplicatas, index=False)
                                        output_duplicatas.seek(0)
                                        
                                        st.download_button(
                                            label="❌ Duplicatas",
                                            data=output_duplicatas,
                                            file_name=f"titulo_brr_duplicatas_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                        )
                            
                            else:
                                st.info("🎉 **Todos os pagamentos já existem no sistema MR!**")
                                st.balloons()
                        
                        # Salvar resultados na sessão para uso posterior
                        st.session_state.resultado_titulo_brr = {
                            'df_completo': df_novos_categorizados,
                            'df_novos': df_novos_categorizados,
                            'df_auto_categorizados': df_auto_categorizados,
                            'df_duplicatas': df_duplicatas
                        }
            
            # Download dos pagamentos brutos
            st.markdown("---")
            st.markdown("### � Download dos Pagamentos Extraídos")
            
            output_ret = io.BytesIO()
            df_confirmados.to_excel(output_ret, index=False)
            output_ret.seek(0)
            
            st.download_button(
                label="📥 Baixar Pagamentos Extraídos",
                data=output_ret,
                file_name=f"pagamentos_titulo_brr_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        else:
            st.warning("⚠️ Nenhum pagamento confirmado (código BD) encontrado nos arquivos")
            
            # Mostrar todos os registros para debug
            st.markdown("**🔍 Todos os Registros (para análise):**")
            st.dataframe(df_ret, use_container_width=True)
    
    # =====================================
    # 🧹 LIMPEZA
    # =====================================
    
    if st.session_state.df_titulo_brr_ret is not None:
        st.markdown("---")
        if st.button("🧹 Limpar Dados TITULO BRR"):
            st.session_state.df_titulo_brr_ret = None
            st.session_state.uploader_key_titulo_brr += 1
            st.rerun()

def render_categorizacao_inteligente_ofx_tab(df_ofx):
    """Renderiza a aba de categorização inteligente para transações OFX seguindo o padrão do TITULO BRR"""
    
    st.subheader("🧠 Categorização Inteligente OFX")
    st.markdown("**Processa transações OFX com eliminação automática de duplicatas + categorização baseada no histórico do sistema MR**")
    
    # Estados da sessão para categorização inteligente
    if "categorizacao_inteligente_results" not in st.session_state:
        st.session_state.categorizacao_inteligente_results = None
    if "modo_categorizacao_ofx" not in st.session_state:
        st.session_state.modo_categorizacao_ofx = "inteligente"
    
    # =====================================
    # 📚 SEÇÃO 1: CARREGAMENTO DO HISTÓRICO MR
    # =====================================
    
    st.markdown("### � Carregamento do Histórico MR")
    st.info("**Selecione as empresas para carregar o histórico que será usado na categorização automática.**")
    
    # Seleção de empresas para buscar histórico MR
    EMPRESAS_MR = {
        "🏢 ROTA - Araranguá": "772644ba-3a49-4736-8443-f057581d6b39",
        "🏢 ROTA - Terra de Areia": "4d49850f-ebf1-433d-a32a-527b54e856aa",
        "🏢 ROTA - Caminho do Sol": "d5ecbd61-8d4a-4ac6-8cc9-7c4919ead401",
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
    
    # Configuração
    with st.container():
        col_config1, col_config2 = st.columns(2)
        
        with col_config1:
            empresas_selecionadas = st.multiselect(
                "Selecione empresas para histórico MR:",
                options=list(EMPRESAS_MR.keys()),
                default=list(EMPRESAS_MR.keys())[:3],  # Primeiras 3 por padrão
                help="Selecione as empresas cujo histórico será usado para categorização automática"
            )
        
        with col_config2:
            anos_historico = st.selectbox(
                "Período do histórico:",
                options=["2025", "2024", "2023"],
                index=0,
                help="Ano dos lançamentos históricos para comparação"
            )
    
    # =====================================
    # 🧠 SEÇÃO 2: PROCESSAMENTO INTELIGENTE
    # =====================================
    
    st.markdown("---")
    st.markdown("### 🧠 Processamento com Categorização Inteligente")
    
    # Botão principal de processamento
    col_process1, col_process2 = st.columns([3, 1])
    
    with col_process1:
        if st.button("🧠 Processar com Categorização Inteligente", type="primary", width="stretch"):
            if not empresas_selecionadas:
                st.warning("Selecione pelo menos uma empresa!")
            else:
                with st.spinner("🔄 Processando transações com IA..."):
                    try:
                        # Converter nomes para IDs
                        empresa_ids = [EMPRESAS_MR[empresa] for empresa in empresas_selecionadas]
                        
                        # Buscar histórico do MR (lançamentos + transferências)
                        st.info("📚 Carregando histórico completo do sistema MR...")
                        
                        # Converter lista de IDs para string separada por vírgulas
                        ids_string = ",".join(empresa_ids)
                        
                        # Buscar dados completos (lançamentos + transferências)
                        from logic.Sistema_MR.API_MR import buscar_lancamentos_e_transferencias_api
                        dados_mr_completos = buscar_lancamentos_e_transferencias_api(ids_string, anos_historico)
                        
                        if dados_mr_completos and 'combinado' in dados_mr_completos:
                            df_mr_historico = dados_mr_completos['combinado']
                            
                            # Mostrar estatísticas
                            total_lancamentos = len(dados_mr_completos.get('lancamentos', pd.DataFrame()))
                            total_transferencias = len(dados_mr_completos.get('transferencias', pd.DataFrame()))
                            st.info(f"✅ Histórico carregado: {total_lancamentos} lançamentos + {total_transferencias} transferências = {len(df_mr_historico)} registros")
                            
                            if df_mr_historico is not None and not df_mr_historico.empty:
                                st.info(f"✅ {len(df_mr_historico)} registros históricos carregados")
                                
                                # Processar com categorização inteligente
                                st.info("🧠 Executando categorização inteligente...")
                                results = processar_ofx_com_categorizacao_inteligente(df_ofx, df_mr_historico)
                                
                                # Salvar resultados
                                st.session_state.categorizacao_inteligente_results = results
                                st.session_state.modo_categorizacao_ofx = "inteligente"
                                
                                # Mostrar resumo
                                stats = results['stats']
                                st.success("🎉 **Processamento concluído!**")
                                
                                col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
                                with col_stat1:
                                    st.metric("Total OFX", stats['total_arquivo'])
                                with col_stat2:
                                    st.metric("Duplicatas", stats['duplicatas'])
                                with col_stat3:
                                    st.metric("Auto-Categorizadas", stats['auto_categorizados'])
                                with col_stat4:
                                    st.metric("Precisam Manual", stats['precisam_manual'])
                            else:
                                st.error("❌ Histórico MR vazio ou não encontrado")
                        else:
                            st.error("❌ Não foi possível carregar histórico do sistema MR")
                            
                    except Exception as e:
                        st.error(f"❌ Erro no processamento: {str(e)}")
    
    with col_process2:
        if st.button("🧹 Limpar"):
            st.session_state.categorizacao_inteligente_results = None
            st.session_state.modo_categorizacao_ofx = "inteligente"
            st.rerun()
    
    # =====================================
    # 📊 SEÇÃO 3: RESULTADOS DA CATEGORIZAÇÃO
    # =====================================
    
    if st.session_state.categorizacao_inteligente_results:
        render_resultados_categorizacao_ofx(st.session_state.categorizacao_inteligente_results)

def render_resultados_categorizacao_ofx(results):
    """Renderiza os resultados da categorização inteligente OFX seguindo o padrão do TITULO BRR"""
    
    st.markdown("---")
    st.markdown("### 📊 Resultados da Categorização Inteligente")
    
    stats = results['stats']
    df_novos_categorizados = results['df_novos_categorizados']
    df_auto_categorizados = results['df_auto_categorizados'] 
    df_precisam_manual = results['df_precisam_manual']
    df_duplicatas = results['df_duplicatas_descartadas']
    
    # =====================================
    # 📈 ESTATÍSTICAS GERAIS
    # =====================================
    
    col_summary1, col_summary2, col_summary3, col_summary4 = st.columns(4)
    
    with col_summary1:
        st.metric(
            "📊 Total Processadas", 
            stats['total_arquivo'],
            help="Total de transações no arquivo OFX"
        )
    
    with col_summary2:
        st.metric(
            "🏦 Já Lançadas no Sistema MR", 
            stats['duplicatas'],
            delta=f"-{(stats['duplicatas']/stats['total_arquivo']*100):.1f}%" if stats['total_arquivo'] > 0 else "0%",
            help="Transações que já foram lançadas no Sistema MR e foram automaticamente excluídas"
        )
    
    with col_summary3:
        st.metric(
            "🧠 Auto-Categorizadas", 
            stats['auto_categorizados'],
            delta=f"+{(stats['auto_categorizados']/stats['novos_total']*100):.1f}%" if stats['novos_total'] > 0 else "0%",
            help="Transações categorizadas automaticamente pela IA (≥70% similaridade)"
        )
    
    with col_summary4:
        st.metric(
            "✋ Categorização Manual", 
            stats['precisam_manual'],
            help="Transações que precisam de categorização manual"
        )
    
    # =====================================
    # 🎯 FINALIZAÇÃO - NOVOS PAGAMENTOS
    # =====================================
    
    if not df_novos_categorizados.empty:
        st.markdown("---")
        with st.expander(f"🎯 Finalização - Novos Pagamentos ({len(df_novos_categorizados)})", expanded=True):
            
            # Separar por tipo de categorização
            num_auto = len(df_auto_categorizados)
            num_manual = len(df_precisam_manual)
            
            if num_auto > 0:
                st.success(f"✅ **Categorizados Automaticamente ({num_auto})**")
                st.success(f"Sistema categorizou {num_auto} pagamentos automaticamente")
            
            if num_manual > 0:
                st.warning(f"⚠️ **{num_manual} pagamentos precisam de categorização manual**")
                
                # Botões para escolher modo
                col_modo1, col_modo2 = st.columns(2)
                
                with col_modo1:
                    if st.button("✅ Manter Categorização", type="primary", key="manter_ofx"):
                        st.session_state.modo_categorizacao_ofx = "manter"
                        st.rerun()
                
                with col_modo2:
                    if st.button("👤 Categorizar Manualmente", key="manual_ofx"):
                        st.session_state.modo_categorizacao_ofx = "manual"
                        st.rerun()
            
            # =====================================
            # 📥 DOWNLOAD PARA SISTEMA MR
            # =====================================
            
            st.markdown("---")
            with st.expander("📥 Download para Sistema MR", expanded=True):
                
                st.success(f"✅ **{len(df_novos_categorizados)} lançamentos processados prontos para importação no Sistema MR**")
                st.info(f"📊 **{num_auto} auto-categorizados + {num_manual} manuais**")
                
                # Gerar Excel seguindo o padrão do TITULO BRR com múltiplas opções
                if len(df_novos_categorizados) > 0:
                    # Obter informação do banco
                    banco_info = None
                    if 'banco_nome_sistema' in df_novos_categorizados.columns:
                        bancos_unicos = df_novos_categorizados['banco_nome_sistema'].dropna().unique()
                        if len(bancos_unicos) > 0:
                            banco_info = bancos_unicos[0]  # Usar o primeiro banco
                    
                    if not banco_info:
                        banco_info = "Banco não identificado"
                    
                    # Layout de downloads seguindo padrão TITULO BRR
                    col_down1, col_down2, col_down3 = st.columns([1, 1, 1])
                    
                    with col_down1:
                        if st.button("� Análise Completa", type="primary", key="excel_completo_ofx", use_container_width=True):
                            excel_mr, df_mr_preview = criar_excel_mr_ofx(df_novos_categorizados, banco_info)
                            
                            st.download_button(
                                label="📥 Baixar Análise Completa",
                                data=excel_mr,
                                file_name=f"ofx_analise_completa_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_completo_ofx",
                                use_container_width=True
                            )
                    
                    with col_down2:
                        if num_auto > 0:
                            if st.button("🧠 Auto-categorizados", key="excel_auto_ofx", use_container_width=True):
                                excel_auto, df_auto_preview = criar_excel_mr_ofx(df_auto_categorizados, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Auto-categorizados",
                                    data=excel_auto,
                                    file_name=f"ofx_auto_categorizados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_auto_ofx",
                                    use_container_width=True
                                )
                        else:
                            st.info("Nenhum auto-categorizado")
                    
                    with col_down3:
                        if num_manual > 0:
                            if st.button("✋ Precisam Manual", key="excel_manual_ofx", use_container_width=True):
                                excel_manual, df_manual_preview = criar_excel_mr_ofx(df_precisam_manual, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Precisam Manual",
                                    data=excel_manual,
                                    file_name=f"ofx_precisam_manual_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_manual_ofx",
                                    use_container_width=True
                                )
                        else:
                            st.success("Todos categorizados!")
                    
                    # Preview do Excel
                    with st.expander("👁️ Preview do Excel gerado", expanded=False):
                        col_preview1, col_preview2 = st.columns([1, 1])
                        
                        with col_preview1:
                            if st.button("📊 Mostrar Preview", key="preview_ofx"):
                                excel_mr, df_mr_preview = criar_excel_mr_ofx(df_novos_categorizados, banco_info)
                                st.session_state.preview_excel_ofx = excel_mr
                                st.session_state.preview_df_ofx = df_mr_preview
                        
                        with col_preview2:
                            if st.button("📥 Download Excel", key="download_preview_ofx", type="primary"):
                                excel_mr, df_mr_preview = criar_excel_mr_ofx(df_novos_categorizados, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Excel Preview",
                                    data=excel_mr,
                                    file_name=f"preview_categorizacao_ofx_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_preview_direct_ofx"
                                )
                        
                        # Mostrar preview se existir
                        if hasattr(st.session_state, 'preview_df_ofx') and st.session_state.preview_df_ofx is not None:
                            st.dataframe(st.session_state.preview_df_ofx, use_container_width=True)
    
    # =====================================
    # 📋 DETALHAMENTO POR CATEGORIA
    # =====================================
    
    st.markdown("---")
    st.markdown("### 📋 Detalhamento por Categoria")
    
    # Abas de detalhamento seguindo padrão do TITULO BRR
    tab_auto, tab_manual, tab_duplicatas, tab_analise_cat = st.tabs([
        f"🧠 Auto-Categorizadas ({stats['auto_categorizados']})",
        f"✋ Precisam Manual ({stats['precisam_manual']})", 
        f"🏦 Já Lançadas no Sistema MR ({stats['duplicatas']})",
        f"📊 Análise de Scores"
    ])
    
    with tab_auto:
        st.markdown("**Transações categorizadas automaticamente pela IA:**")
        
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
    
    with tab_duplicatas:
        st.markdown("**Transações que já foram lançadas no Sistema MR:**")
        
        if not df_duplicatas.empty:
            # Preparar dados para exibição
            df_display = df_duplicatas[['data', 'descricao', 'valor_absoluto']].copy()
            df_display.columns = ['Data', 'Descrição', 'Valor']
            
            # Formatar data e valor
            df_display['Data'] = pd.to_datetime(df_display['Data'], errors='coerce').dt.strftime('%d/%m/%Y')
            df_display['Valor'] = df_display['Valor'].apply(lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
            
            st.dataframe(df_display, width="stretch", hide_index=True)
            st.info("🏦 Estas transações já foram lançadas no Sistema MR e foram automaticamente excluídas")
        else:
            st.success("✅ Nenhuma transação já lançada no Sistema MR encontrada!")
    
    with tab_analise_cat:
        st.markdown("**Análise Detalhada de Scores de Similaridade:**")
        st.info("Esta tabela mostra todos os resultados da análise de similaridade, incluindo scores baixos que não foram aceitos automaticamente.")
        
        # Verificar se há análise de scores disponível
        if hasattr(st.session_state, 'analise_scores_ofx') and st.session_state.analise_scores_ofx:
            df_scores = pd.DataFrame(st.session_state.analise_scores_ofx)
            
            # Ordenar primeiro por Score original (antes de formatar)
            df_scores = df_scores.sort_values('Score', ascending=False)
            
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
            
            st.dataframe(df_display_scores, width="stretch", hide_index=True)
            
            # Estatísticas da análise
            st.markdown("---")
            st.markdown("**📈 Estatísticas da Análise:**")
            
            total_transacoes = len(df_scores)
            auto_categorizadas = len(df_scores[df_scores['Status'].str.contains('AUTO')])
            manuais = total_transacoes - auto_categorizadas
            
            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
            
            with col_stat1:
                st.metric("📊 Total Analisadas", total_transacoes)
            
            with col_stat2:
                st.metric("🟢 Auto-Categorizadas", auto_categorizadas, delta=f"{(auto_categorizadas/total_transacoes*100):.1f}%")
            
            with col_stat3:
                st.metric("🔴 Categorização Manual", manuais, delta=f"{(manuais/total_transacoes*100):.1f}%")
            
            with col_stat4:
                score_medio = df_scores['Score'].mean()
                st.metric("📈 Score Médio", f"{score_medio:.1%}")
            
            # =====================================
            # 📥 DOWNLOADS PARA SISTEMA MR
            # =====================================
            
            st.markdown("---")
            st.markdown("**📥 Downloads para Sistema MR:**")
            st.info("💡 **Mesmo com scores baixos**, você pode baixar o Excel formatado para importação manual no Sistema MR")
            
            # Obter dados categorizados da sessão
            if 'results_categorizacao' in st.session_state and st.session_state.results_categorizacao:
                results_cat = st.session_state.results_categorizacao
                df_todas_transacoes = results_cat.get('df_novos_categorizados', pd.DataFrame())
                
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
                        if st.button("📊 Todas as Transações", key="excel_todas_cat", use_container_width=True):
                            excel_mr, df_mr_preview = criar_excel_mr_ofx(df_todas_transacoes, banco_info)
                            
                            st.download_button(
                                label="📥 Baixar Todas",
                                data=excel_mr,
                                file_name=f"todas_transacoes_cat_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_todas_cat",
                                use_container_width=True
                            )
                    
                    with col_down2:
                        # Apenas as auto-categorizadas (score >= 70%)
                        df_auto_only = df_todas_transacoes[df_todas_transacoes['Status_Categorizacao'] == 'AUTO']
                        if len(df_auto_only) > 0:
                            if st.button(f"🟢 Auto-categorizadas ({len(df_auto_only)})", key="excel_auto_cat", use_container_width=True):
                                excel_auto, df_auto_preview = criar_excel_mr_ofx(df_auto_only, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Auto",
                                    data=excel_auto,
                                    file_name=f"auto_categorizadas_cat_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_auto_cat",
                                    use_container_width=True
                                )
                        else:
                            st.info("Nenhuma auto-categorizada", icon="🟢")
                    
                    with col_down3:
                        # Apenas as que precisam de categorização manual (score < 70%)
                        df_manual_only = df_todas_transacoes[df_todas_transacoes['Status_Categorizacao'] == 'MANUAL']
                        if len(df_manual_only) > 0:
                            if st.button(f"🔴 Precisam Manual ({len(df_manual_only)})", key="excel_manual_cat", use_container_width=True):
                                excel_manual, df_manual_preview = criar_excel_mr_ofx(df_manual_only, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Manual",
                                    data=excel_manual,
                                    file_name=f"precisam_manual_cat_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_manual_cat",
                                    use_container_width=True
                                )
                        else:
                            st.success("Todas categorizadas!", icon="🎉")
                    
                    with col_down4:
                        # Opção customizada - só scores altos
                        df_scores_altos = df_todas_transacoes[
                            (df_todas_transacoes['Status_Categorizacao'] == 'AUTO') |
                            (df_todas_transacoes.get('score_similaridade', 0) >= 0.5)  # scores >= 50%
                        ]
                        
                        if len(df_scores_altos) > 0:
                            if st.button(f"⭐ Scores Altos ({len(df_scores_altos)})", key="excel_altos_cat", use_container_width=True):
                                excel_altos, df_altos_preview = criar_excel_mr_ofx(df_scores_altos, banco_info)
                                
                                st.download_button(
                                    label="📥 Baixar Altos",
                                    data=excel_altos,
                                    file_name=f"scores_altos_cat_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_altos_cat",
                                    use_container_width=True
                                )
                        else:
                            st.info("Sem scores altos", icon="⭐")
                    
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
            if st.button("� Download Análise de Scores", key="download_scores_cat"):
                output = io.BytesIO()
                df_scores.to_excel(output, index=False)
                output.seek(0)
                
                st.download_button(
                    label="📥 Baixar Análise Completa",
                    data=output,
                    file_name=f"analise_scores_categorizacao_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_scores_cat_direct"
                )
        else:
            st.warning("⚠️ Nenhuma análise de scores disponível. Execute o processamento primeiro.")

if __name__ == "__main__":
    main()