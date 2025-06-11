import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

@st.cache_resource
def conectar_bd():
    engine = create_engine("postgresql+psycopg2://compras:pecist%40compr%40s2024@srvdados:5432/postgres")
    return engine.connect()

def carregar_campanhas():
    try:
        return pd.read_csv("campanhas.csv")
    except FileNotFoundError:
        return pd.DataFrame(columns=["nome", "fornecedor", "grupos", "inicio", "fim"])

@st.cache_data(show_spinner=False)
def calcular_resultado(_conn, inicio, fim, fornecedor, grupos):
    params = {
        "inicio": inicio,
        "fim": fim,
        "fornecedor": fornecedor,
        "loja": '08',
        "grupos": grupos
    }
    consulta = f"""
        WITH vendas AS (
            SELECT SUM(pp.qtde_ven) AS total_qtde_vendida
            FROM \"D-1\".prod_ped pp
            LEFT JOIN \"D-1\".cliente c ON pp.codcli = c.codcli
            LEFT JOIN \"D-1\".produto p ON pp.cod_pro = p.codpro
            LEFT JOIN \"D-1\".grupo gru ON p.codgru = gru.codgru
            WHERE pp.tipped = 'V'
            AND pp.dt_emissao BETWEEN :inicio AND :fim
            AND pp.codvde NOT IN ('0100', '0001', '0006', '2319')
            AND pp.cd_loja = :loja
            AND c.codarea <> '112'
            AND c.codcid <> '0501'
            AND p.fantasia = :fornecedor
            AND gru.grupo = ANY(:grupos)
        ),
        devolucoes AS (
            SELECT SUM(pe.qt_devolve) AS total_qtde_devolvida
            FROM \"D-1\".prod_ent pe
            INNER JOIN \"D-1\".entrada e ON e.cd_loja = pe.cd_loja AND e.sg_serie = pe.sg_serie AND e.nu_nota = pe.nu_nota
            INNER JOIN \"D-1\".cliente cli ON cli.codcli = pe.cd_cliente
            INNER JOIN \"D-1\".produto pro ON pro.codpro = pe.cd_produto
            INNER JOIN \"D-1\".grupo gru ON pro.codgru = gru.codgru
            WHERE pe.dt_emissao BETWEEN :inicio AND :fim
            AND e.in_cancela = 'N'
            AND e.in_clifor = 'C'
            AND pe.cd_cfop NOT IN ('1949', '2949', '1603')
            AND pe.cd_loja = :loja
            AND cli.codcid <> '0501'
            AND cli.codarea <> '112'
            AND pro.fantasia = :fornecedor
            AND gru.grupo = ANY(:grupos)
        )
        SELECT 
            COALESCE(v.total_qtde_vendida, 0),
            COALESCE(d.total_qtde_devolvida, 0),
            COALESCE(v.total_qtde_vendida, 0) - COALESCE(d.total_qtde_devolvida, 0) AS total_liquido
        FROM vendas v, devolucoes d;
    """
    resultado = _conn.execute(text(consulta), params).fetchone()
    return resultado if resultado else (0, 0, 0)

st.title("📈 Painel de Campanhas de Vendas")
aba = st.sidebar.radio("Menu", ["Campanhas Ativas", "Nova Campanha"])

conn = conectar_bd()
campanhas = carregar_campanhas()

if aba == "Nova Campanha":
    st.subheader("➕ Cadastrar Nova Campanha")
    nome = st.text_input("Nome da Campanha")
    fornecedor = st.text_input("Fornecedor")
    grupos = st.text_area("Grupos de Peças (separados por ponto e vírgula)")
    inicio = st.date_input("Data de Início", value=date.today())
    fim = st.date_input("Data de Fim", value=date.today())

    if st.button("Salvar Campanha"):
        nova = pd.DataFrame.from_dict([{
            "nome": nome,
            "fornecedor": fornecedor,
            "grupos": grupos,
            "inicio": inicio,
            "fim": fim
        }])
        campanhas = pd.concat([campanhas, nova], ignore_index=True)
        campanhas.to_csv("campanhas.csv", index=False)
        st.success("Campanha cadastrada com sucesso!")
        st.rerun()

if aba == "Campanhas Ativas":
    st.subheader("🔁 Campanhas em Andamento")
    if st.button("🔄 Recarregar Dados"):
        st.rerun()

    if campanhas.empty:
        st.info("Nenhuma campanha cadastrada.")
    else:
        resultados = []
        for _, row in campanhas.iterrows():
            grupos_list = [g.strip() for g in row['grupos'].split(';') if g.strip()]
            qtde, devolucao, liquido = calcular_resultado(conn, row['inicio'], row['fim'], row['fornecedor'], grupos_list)
            resultados.append({
                "Nome": row['nome'],
                "Fornecedor": row['fornecedor'],
                "Período": f"{row['inicio']} a {row['fim']}",
                "Total Vendido": int(qtde or 0),
                "Total Devolvido": int(devolucao or 0),
                "Total Líquido": int(liquido or 0),
                "Grupos": row['grupos']
            })
        st.dataframe(pd.DataFrame(resultados))

