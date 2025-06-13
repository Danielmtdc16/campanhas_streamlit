import streamlit as st
import pandas as pd
import json
import os
import shutil
from sqlalchemy import create_engine, text
from datetime import date

# ——————————————————————————
# Conexão com o banco
# ——————————————————————————
@st.cache_resource
def conectar_bd():
    engine = create_engine(
        "postgresql+psycopg2://compras:pecist%40compr%40s2024@srvdados:5432/postgres"
    )
    return engine.connect()

# ——————————————————————————
# Atualiza lista de lojas (CD_LOJA e LOJA)
# ——————————————————————————
def atualizar_lojas():
    conn = conectar_bd()
    try:
        conn.rollback()
    except:
        pass
    sql = text('SELECT "CD_LOJA", "LOJA" FROM "D-1".lojas ORDER BY "LOJA"')
    rows = conn.execute(sql).fetchall()
    df = pd.DataFrame(rows, columns=["cd_loja", "nome_loja"])
    df.to_csv("lojas.csv", index=False, encoding="utf-8")

# ——————————————————————————
# Carrega CSV de lojas
# ——————————————————————————
def carregar_lojas():
    if not os.path.exists("lojas.csv"):
        atualizar_lojas()
    return pd.read_csv("lojas.csv", dtype=str)

# ——————————————————————————
# Atualiza fornecedores e grupos em CSV
# ——————————————————————————
def atualizar_fornecedores_e_grupos():
    if os.path.exists("grupos"):
        shutil.rmtree("grupos")
    os.makedirs("grupos", exist_ok=True)

    conn = conectar_bd()
    try:
        conn.rollback()
    except:
        pass
    sql = text(
        """
        SELECT DISTINCT f.fantasia, gru.grupo
        FROM "D-1".fornec f
        JOIN "D-1".prod_ped pp ON f.codfor = pp.codfor
        JOIN "D-1".produto p ON pp.cod_pro = p.codpro
        JOIN "D-1".grupo gru ON p.codgru = gru.codgru
        WHERE f.fantasia IS NOT NULL
          AND f.in_tipofor = 'P'
          AND gru.grupo IS NOT NULL
        ORDER BY f.fantasia, gru.grupo
        """
    )
    rows = conn.execute(sql).fetchall()
    df = pd.DataFrame(rows, columns=["fantasia", "grupo"]);
    valid = []
    for fantasia, grp in df.groupby("fantasia")["grupo"]:
        lista = grp.tolist()
        if not lista:
            continue
        safe = fantasia.replace('/', '_').replace('\\', '_')
        pd.DataFrame({"grupo": lista}).to_csv(f"grupos/{safe}.csv", index=False)
        valid.append(fantasia)
    pd.DataFrame({"fantasia": valid}).to_csv("fornecedores.csv", index=False, encoding="utf-8")

# ——————————————————————————
# Carrega fornecedores e grupos cacheados
# ——————————————————————————
@st.cache_data
def carregar_fornecedores():
    if os.path.exists("fornecedores.csv"):
        return pd.read_csv("fornecedores.csv", dtype=str)["fantasia"].tolist()
    return []

@st.cache_data
def carregar_grupos_do_fornecedor(fantasia):
    safe = fantasia.replace('/', '_').replace('\\', '_')
    path = f"grupos/{safe}.csv"
    if os.path.exists(path):
        return pd.read_csv(path, dtype=str)["grupo"].tolist()
    return []

# ——————————————————————————
# Carrega campanhas
# ——————————————————————————
def carregar_campanhas():
    cols = ["nome", "fornecedor", "grupos", "inicio", "fim", "personalizado", "tipo", "meta_geral", "metas_por_loja"]
    try:
        df = pd.read_csv("campanhas.csv", dtype=str)
    except FileNotFoundError:
        return pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = "{}" if c == "metas_por_loja" else ""
    df["metas_por_loja"] = df["metas_por_loja"].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip() else {})
    return df

# ——————————————————————————
# Consulta e agrega em memória
# ——————————————————————————
@st.cache_data(show_spinner=False)
def calcular_agrupado(inicio, fim, fornecedor, grupos, lojas):
    conn = conectar_bd()
    try:
        conn.rollback()
    except:
        pass
    sql = text(
        """
        WITH vendas AS (
            SELECT pp.cd_loja, gru.grupo, SUM(pp.qtde_ven) AS vend
            FROM "D-1".prod_ped pp
            JOIN "D-1".produto p ON pp.cod_pro = p.codpro
            JOIN "D-1".grupo gru ON p.codgru = gru.codgru
            WHERE pp.tipped = 'V'
              AND pp.dt_emissao BETWEEN :inicio AND :fim
              AND pp.codvde NOT IN ('0100','0001','0006','2319')
              AND p.fantasia = :fornecedor
              AND gru.grupo = ANY(:grupos)
            GROUP BY pp.cd_loja, gru.grupo
        ),
        dev AS (
            SELECT pe.cd_loja, gru.grupo, SUM(pe.qt_devolve) AS dev
            FROM "D-1".prod_ent pe
            JOIN "D-1".entrada e ON e.cd_loja = pe.cd_loja AND e.sg_serie = pe.sg_serie AND e.nu_nota = pe.nu_nota
            JOIN "D-1".produto p ON pe.cd_produto = p.codpro
            JOIN "D-1".grupo gru ON p.codgru = gru.codgru
            WHERE pe.dt_emissao BETWEEN :inicio AND :fim
              AND e.in_cancela = 'N'
              AND e.in_clifor = 'C'
              AND pe.cd_cfop NOT IN ('1949','2949','1603')
              AND p.fantasia = :fornecedor
              AND gru.grupo = ANY(:grupos)
            GROUP BY pe.cd_loja, gru.grupo
        )
        SELECT v.cd_loja::text AS loja, v.grupo, COALESCE(v.vend, 0) - COALESCE(d.dev, 0) AS liquido
        FROM vendas v
        LEFT JOIN dev d ON v.cd_loja = d.cd_loja AND v.grupo = d.grupo;
        """
    )
    df = pd.read_sql(sql, conn, params={"inicio": inicio, "fim": fim, "fornecedor": fornecedor, "grupos": grupos})
    df["loja"] = df["loja"].astype(str)
    return df[df["loja"].isin(lojas)]

# ——————————————————————————
# Interface Streamlit
# ——————————————————————————
st.title("📈 Painel de Campanhas de Vendas")
aba = st.sidebar.radio("Menu", ["Campanhas Ativas", "Nova Campanha", "Atualizar Dados"])

campanhas = carregar_campanhas()
df_lojas = carregar_lojas()

if aba == "Atualizar Dados":
    st.subheader("🔄 Atualização")
    if st.button("Atualizar Tudo"):
        with st.spinner("Atualizando..."):
            atualizar_fornecedores_e_grupos()
            atualizar_lojas()
        st.cache_data.clear()
        st.success("Dados atualizados!")

elif aba == "Nova Campanha":
    st.subheader("➕ Nova Campanha")
    tipo = st.radio("Tipo", ["Geral", "Por Loja"])
    nomes = df_lojas["nome_loja"].tolist()
    metas_por_loja = {}
    if tipo == "Geral":
        meta_geral = st.number_input("Meta geral (unidades)", min_value=0)
        lojas_para_query = df_lojas["cd_loja"].tolist()
    else:
        sel = st.multiselect("Selecione as lojas", nomes)
        lojas_para_query = []
        for nome_loja in sel:
            cod = df_lojas.loc[df_lojas["nome_loja"] == nome_loja, "cd_loja"].iat[0]
            lojas_para_query.append(cod)
            metas_por_loja[cod] = st.number_input(f"Meta para {nome_loja}", min_value=0, key=f"m_{cod}")
        meta_geral = None
    fornecedor = st.selectbox("Fornecedor", carregar_fornecedores())
    grupos = st.multiselect("Grupos de Peças", carregar_grupos_do_fornecedor(fornecedor))
    personalizado = st.checkbox("Período personalizado")
    if personalizado:
        inicio = st.date_input("Início", value=date.today())
        fim = st.date_input("Fim", value=date.today())
    else:
        hoje = date.today()
        inicio = date(hoje.year, hoje.month, 1)
        fim = (date(hoje.year, hoje.month+1, 1) - pd.Timedelta(days=1)) if hoje.month < 12 else date(hoje.year, 12, 31)
    nome_campanha = st.text_input("Nome da campanha")
    if st.button("Salvar Campanha"):
        nova = pd.DataFrame([{
            'nome': nome_campanha,
            'fornecedor': fornecedor,
            'grupos': '; '.join(grupos),
            'inicio': inicio,
            'fim': fim,
            'personalizado': personalizado,
            'tipo': tipo,
            'meta_geral': meta_geral or 0,
            'metas_por_loja': json.dumps(metas_por_loja, ensure_ascii=False)
        }])
        campanhas = pd.concat([campanhas, nova], ignore_index=True)
        campanhas.to_csv("campanhas.csv", index=False, encoding="utf-8")
        st.success("Campanha cadastrada com sucesso!")

elif aba == "Campanhas Ativas":
    st.subheader("🔁 Em Andamento")
    if st.button("Recarregar"):
        st.rerun()
    if campanhas.empty:
        st.info("Sem campanhas.")
    else:
        campanhas['mes_ano'] = pd.to_datetime(campanhas['inicio']).dt.strftime('%B/%Y').str.upper()
        for mes, grupo_mes in campanhas.groupby('mes_ano'):
            st.markdown(f"## 📅 Campanhas {mes}")
            for _, row in grupo_mes.iterrows():
                st.markdown(f"### {row['nome']}")
                st.markdown(f"Fornecedor: {row['fornecedor']}")
                if row['personalizado']:
                    st.markdown(f"Período: {row['inicio']} até {row['fim']}")
                st.markdown(f"Grupos: {row['grupos']}")
                lojas_para_query = df_lojas['cd_loja'].tolist() if row['tipo'] == 'Geral' else list(row['metas_por_loja'].keys())
                df_ag = calcular_agrupado(row['inicio'], row['fim'], row['fornecedor'], row['grupos'].split('; '), lojas_para_query)
                if row['tipo'] == 'Geral':
                    st.metric("Meta Geral", int(row['meta_geral'] or 0))
                    total = int(df_ag['liquido'].sum())
                    pct = (100 * total / int(row['meta_geral'])) if row['meta_geral'] else 0
                    st.metric("Total líquido:", total)
                    st.metric("% Atingido", f"{pct:.1f}%")
                else:
                    st.markdown("**Metas por Loja:**")
                    for cod, meta in row['metas_por_loja'].items():
                        nome_loja = df_lojas.loc[df_lojas['cd_loja'] == cod, 'nome_loja'].iat[0]
                        vend = int(df_ag.loc[df_ag['loja'] == cod, 'liquido'].sum())
                        pct = (100 * vend / meta) if meta else 0
                        st.write(f"- {nome_loja}: {vend}/{meta} ({pct:.1f}%)")
                st.markdown("---")
