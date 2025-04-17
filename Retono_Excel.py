import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="Leitor CNAB240 .RET", layout="centered")

st.title("📄 Leitor de Arquivo CNAB240 (.RET)")
st.markdown("Faça o upload de um arquivo `.RET` (CNAB240) para extrair os dados de **pagamentos (Segmento J)** e gerar um arquivo Excel.")

def ler_cnab240_segmento_j(conteudo_arquivo):
    registros = []

    for linha in conteudo_arquivo.splitlines():
        if len(linha) >= 150 and linha[13] == 'J':
            nome_favorecido = linha[61:90].strip()
            data_pagamento = linha[91:100]
            valor = linha[101:115].strip()

            # Limpa os zeros do nome se existirem
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
                'Valor (R$)': f"{valor_formatado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            })

    return pd.DataFrame(registros)

# Upload do arquivo
uploaded_file = st.file_uploader("📁 Envie o arquivo .RET aqui", type=["ret", "txt"])

if uploaded_file:
    conteudo = uploaded_file.read().decode("utf-8", errors="ignore")
    df = ler_cnab240_segmento_j(conteudo)

    if not df.empty:
        st.success(f"{len(df)} pagamentos encontrados.")
        st.dataframe(df, use_container_width=True)

        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        st.download_button(
            label="📥 Baixar Excel",
            data=output,
            file_name="pagamentos_cnab240.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("❌ Nenhum pagamento (Segmento J) foi encontrado neste arquivo.")