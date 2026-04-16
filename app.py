import os
import dash
from dash import html, dcc, dash_table, Input, Output, State, callback, no_update, ctx
import dash_bootstrap_components as dbc
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime, timezone
from flask import request as flask_request

# ─────────────────────────────────────────────────────────────────────────────
# Connection config
# ─────────────────────────────────────────────────────────────────────────────

RULES_TABLE = "public.rules_metadata"
EXEC_TABLE  = "public.rules_execution"
TESTS_TABLE = "public.tests_metadata"

def _conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("LAKEBASE_USER"),
        password=os.getenv("LAKEBASE_PASSWORD"),
        sslmode=os.getenv("PGSSLMODE"),
        connect_timeout=10,
    )

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers — Rules
# ─────────────────────────────────────────────────────────────────────────────

def get_current_user() -> str:
    try:
        return (
            flask_request.headers.get("X-Forwarded-Email") or
            flask_request.headers.get("X-Forwarded-User") or
            ""
        )
    except RuntimeError:
        return ""


def fetch_rules() -> pd.DataFrame:
    try:
        with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"""
                SELECT rule_id, ruletype, rulename, ruledescription,
                       rulesql, rule_logic, message_level, createdby, createdtime, version
                FROM {RULES_TABLE}
                ORDER BY rule_id, version DESC
            """)
            rows = cur.fetchall()
            return pd.DataFrame([dict(r) for r in rows])
    except Exception as e:
        print(f"[fetch_rules] {e}")
        return pd.DataFrame(columns=["rule_id","ruletype","rulename","ruledescription",
                                     "rulesql","rule_logic","message_level","createdby","createdtime","version"])


def get_next_version(rule_id: str) -> int:
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(version), 0) FROM {RULES_TABLE} WHERE rule_id = %s", (rule_id,))
            return cur.fetchone()[0] + 1
    except Exception as e:
        print(f"[get_next_version] {e}")
        return 1


def insert_rule(rule_id, rule_type, rule_name, rule_desc, rule_sql, rule_logic, message_level, created_by):
    try:
        version = get_next_version(rule_id)
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {RULES_TABLE}
                        (rule_id, ruletype, rulename, ruledescription, rulesql, rule_logic, message_level, createdby, createdtime, version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (rule_id, rule_type, rule_name, rule_desc, rule_sql, rule_logic, message_level, created_by, now_utc(), version))
            conn.commit()
        return True, f"Rule {rule_id} v{version} saved successfully."
    except Exception as e:
        return False, f"Insert failed: {e}"


def delete_rule(rule_id, version):
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {RULES_TABLE} WHERE rule_id = %s AND version = %s",
                            (rule_id, int(version)))
            conn.commit()
        return True, f"Rule {rule_id} v{version} deleted."
    except Exception as e:
        return False, f"Delete failed: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers — Executions
# ─────────────────────────────────────────────────────────────────────────────

def fetch_executions() -> pd.DataFrame:
    try:
        with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"""
                SELECT e.test_id, e.rule_id, r.rulename, e.ruleexecutionorder,
                       e.createdby, e.createdtime
                FROM {EXEC_TABLE} e
                LEFT JOIN (
                    SELECT DISTINCT ON (rule_id) rule_id, rulename
                    FROM {RULES_TABLE}
                    ORDER BY rule_id, version DESC
                ) r ON e.rule_id = r.rule_id
                ORDER BY e.test_id, e.ruleexecutionorder
            """)
            rows = cur.fetchall()
            return pd.DataFrame([dict(r) for r in rows])
    except Exception as e:
        print(f"[fetch_executions] {e}")
        return pd.DataFrame(columns=["test_id","rule_id","rulename",
                                     "ruleexecutionorder","createdby","createdtime"])


def fetch_rule_ids() -> list[dict]:
    """Return latest version of each rule_id for the dropdown."""
    try:
        with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"""
                SELECT DISTINCT ON (rule_id) rule_id, rulename
                FROM {RULES_TABLE}
                ORDER BY rule_id, version DESC
            """)
            return [{"label": f"{r['rule_id']} — {r['rulename']}", "value": r["rule_id"]}
                    for r in cur.fetchall()]
    except Exception as e:
        print(f"[fetch_rule_ids] {e}")
        return []


def fetch_test_ids() -> list[dict]:
    """Return distinct test IDs from tests_metadata for the dropdown."""
    try:
        with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"""
                SELECT DISTINCT ON (test_id) test_id, test_title
                FROM {TESTS_TABLE}
                ORDER BY test_id, version DESC
            """)
            return [{"label": f"{r['test_id']} — {r['test_title']}", "value": r["test_id"]}
                    for r in cur.fetchall()]
    except Exception as e:
        print(f"[fetch_test_ids] {e}")
        return []


def get_next_exec_order(test_id: int) -> int:
    """Return max(ruleexecutionorder) + 1 for the given test_id."""
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(ruleexecutionorder), 0) FROM {EXEC_TABLE} WHERE test_id = %s",
                        (int(test_id),))
            return cur.fetchone()[0] + 1
    except Exception as e:
        print(f"[get_next_exec_order] {e}")
        return 1


def insert_execution(test_id, rule_id, order, created_by):
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {EXEC_TABLE}
                        (test_id, rule_id, ruleexecutionorder, createdby, createdtime)
                    VALUES (%s, %s, %s, %s, %s)
                """, (int(test_id), rule_id, int(order), created_by, now_utc()))
            conn.commit()
        return True, f"Execution row saved (Test {test_id} / Rule {rule_id} / Order {order})."
    except Exception as e:
        return False, f"Insert failed: {e}"


def delete_execution(test_id, rule_id):
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {EXEC_TABLE} WHERE test_id = %s AND rule_id = %s",
                            (int(test_id), rule_id))
            conn.commit()
        return True, f"Execution entry deleted."
    except Exception as e:
        return False, f"Delete failed: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers — Tests Metadata
# ─────────────────────────────────────────────────────────────────────────────

def fetch_tests() -> pd.DataFrame:
    try:
        with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"""
                SELECT test_id, version, test_title, business_line, createdby, createdtime
                FROM {TESTS_TABLE}
                ORDER BY test_id, version DESC
            """)
            rows = cur.fetchall()
            return pd.DataFrame([dict(r) for r in rows])
    except Exception as e:
        print(f"[fetch_tests] {e}")
        return pd.DataFrame(columns=["test_id","version","test_title","business_line","createdby","createdtime"])


def get_next_test_version(test_id: int) -> int:
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(version), 0) FROM {TESTS_TABLE} WHERE test_id = %s", (test_id,))
            return cur.fetchone()[0] + 1
    except Exception as e:
        print(f"[get_next_test_version] {e}")
        return 1


def insert_test(test_id, test_title, business_line, created_by):
    try:
        version = get_next_test_version(int(test_id))
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {TESTS_TABLE}
                        (test_id, version, test_title, business_line, createdby, createdtime)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (int(test_id), version, test_title, business_line, created_by, now_utc()))
            conn.commit()
        return True, f"Test {test_id} v{version} saved successfully."
    except Exception as e:
        return False, f"Insert failed: {e}"


def delete_test(test_id, version):
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {TESTS_TABLE} WHERE test_id = %s AND version = %s",
                            (int(test_id), int(version)))
            conn.commit()
        return True, f"Test {test_id} v{version} deleted."
    except Exception as e:
        return False, f"Delete failed: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# App init
# ─────────────────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap",
    ],
    title="Azimuth GRC Rules Manager",
    suppress_callback_exceptions=True,
    prevent_initial_callbacks="initial_duplicate",
)
server = app.server


# ─────────────────────────────────────────────────────────────────────────────
# Styles
# ─────────────────────────────────────────────────────────────────────────────

MONO = "IBM Plex Mono, monospace"
SANS = "IBM Plex Sans, sans-serif"

C = {
    "bg0":    "#f5f7ff",   # soft indigo-tinted page background
    "bg1":    "#ffffff",   # card surface
    "bg2":    "#f0f2fc",   # input background
    "bg3":    "#e5e8f8",   # table header / muted surface
    "border": "#d1d5f0",   # subtle border
    "text":   "#111827",   # near-black body text
    "text2":  "#374151",   # secondary text
    "text3":  "#6b7280",   # muted / label text
    "accent": "#4f46e5",   # indigo-600 — primary action color
    "accent2": "#eef2ff",  # indigo-50 — selected row highlight
    "nav":    "#1e1b4b",   # indigo-950 — dark navbar
    "green":  "#059669",
    "amber":  "#d97706",
    "red":    "#dc2626",
}

card = {
    "backgroundColor": C["bg1"],
    "border": f"1px solid {C['border']}",
    "borderRadius": "10px",
    "padding": "22px 24px",
    "marginBottom": "20px",
    "fontFamily": SANS,
    "boxShadow": "0 1px 4px rgba(79,70,229,0.06), 0 0 0 1px rgba(79,70,229,0.04)",
}

inp = {
    "backgroundColor": C["bg2"],
    "border": f"1px solid {C['border']}",
    "borderRadius": "6px",
    "color": C["text"],
    "fontSize": "13px",
    "fontFamily": SANS,
}

code_inp = {
    **inp,
    "fontFamily": MONO,
    "fontSize": "12px",
    "height": "150px",
    "lineHeight": "1.7",
    "resize": "vertical",
}

lbl = {
    "fontSize": "11px",
    "fontWeight": "600",
    "color": C["text3"],
    "textTransform": "uppercase",
    "letterSpacing": "0.07em",
    "marginBottom": "4px",
}

tbl_header = {
    "backgroundColor": C["bg3"],
    "color": C["text2"],
    "fontWeight": "700",
    "fontSize": "11px",
    "textTransform": "uppercase",
    "letterSpacing": "0.06em",
    "border": f"1px solid {C['border']}",
    "fontFamily": SANS,
    "padding": "10px 12px",
}

tbl_cell = {
    "backgroundColor": C["bg1"],
    "color": C["text"],
    "border": f"1px solid {C['border']}",
    "fontSize": "12px",
    "fontFamily": SANS,
    "textAlign": "left",
    "padding": "9px 12px",
    "whiteSpace": "normal",
    "height": "auto",
    "overflow": "hidden",
    "textOverflow": "ellipsis",
    "maxWidth": "260px",
}

def sec(title):
    return html.Div(title, style={
        "fontSize": "11px", "fontWeight": "700", "color": C["accent"],
        "textTransform": "uppercase", "letterSpacing": "0.09em",
        "paddingBottom": "10px", "marginBottom": "14px",
        "borderBottom": f"2px solid {C['accent2']}",
    })

def fld(label, component, required=False):
    star = html.Span(" *", style={"color": C["amber"]}) if required else None
    return html.Div([
        html.Label([label, star] if star else label, style=lbl),
        component,
    ], style={"marginBottom": "14px"})


def make_toast(tid, header, icon):
    return dbc.Toast(id=tid, header=header, is_open=False, dismissable=True, duration=4000, icon=icon,
                     style={"position":"fixed","top":"20px","right":"20px","width":"340px",
                            "zIndex":9999,"backgroundColor":C["bg1"],
                            "border":f"1px solid {C['border']}","fontFamily":SANS,"fontSize":"13px"})


# ─────────────────────────────────────────────────────────────────────────────
# Layout
# ─────────────────────────────────────────────────────────────────────────────

navbar = html.Div(html.Div([
    html.Div([
        html.Div("RM", style={
            "width":"30px","height":"30px","background":C["accent"],"borderRadius":"8px",
            "display":"flex","alignItems":"center","justifyContent":"center",
            "fontSize":"11px","fontWeight":"700","color":"#fff","fontFamily":MONO,
            "boxShadow": "0 0 0 2px rgba(255,255,255,0.15)",
        }),
        html.Span("Azimuth GRC Rules Manager", style={"fontSize":"15px","fontWeight":"600",
                                           "color":"#f1f5f9","fontFamily":SANS}),
    ], style={"display":"flex","alignItems":"center","gap":"12px"}),
    html.Span("Databricks · Lakebase",
              style={"fontSize":"11px","color":"#94a3b8","fontFamily":MONO}),
], style={"display":"flex","alignItems":"center","justifyContent":"space-between",
          "padding":"0 28px","height":"54px"}),
style={"backgroundColor":C["nav"],"borderBottom":"none","marginBottom":"24px",
       "boxShadow":"0 2px 12px rgba(30,27,75,0.25)"})


# ── Tab 1: Rules Library ──────────────────────────────────────────────────────

rules_form = html.Div([
    sec("Add / Update Rule"),
    dbc.Row([
        dbc.Col(fld("Rule ID", dbc.Input(id="r-id", placeholder="e.g. C0001", style=inp), required=True), md=3),
        dbc.Col(fld("Rule Type", dcc.Dropdown(id="r-type", options=[
                        {"label":"Calculation","value":"Calculation"},
                        {"label":"Filter","value":"Filter"},
                    ], placeholder="Select type…",
                    style={"fontSize":"13px"}), required=True), md=3),
        dbc.Col(fld("Rule Name", dbc.Input(id="r-name", placeholder="e.g. FILE_GENERATION_DT_FROM", style=inp), required=True), id="r-name-col", md=4),
        dbc.Col(fld("Created By", dbc.Input(id="r-createdby", disabled=True, style={**inp, "backgroundColor":C["bg3"], "color":C["text3"]}), required=False), md=2),
    ]),
    fld("Rule Description", dbc.Input(id="r-desc", placeholder="Brief description", style=inp)),
    dbc.Row([
        dbc.Col(fld("Rule Logic", dbc.Textarea(id="r-logic", placeholder="Description of the rule logic…",
                    style={**inp, "height":"80px","resize":"vertical"})), md=9),
        dbc.Col(fld("Message Level", dcc.Dropdown(id="r-msglevel", options=[
                        {"label":"LOAN",   "value":"LOAN"},
                        {"label":"ESCROW", "value":"ESCROW"},
                    ], value="LOAN", clearable=False,
                    style={"fontSize":"13px"}), required=True), md=3),
    ]),
    fld("Rule SQL", dbc.Textarea(id="r-sql", placeholder="CASE\n  WHEN ...\nEND", style=code_inp), required=True),
    html.Div([
        dbc.Button("＋ Add Rule", id="btn-rule-add", color="primary", size="sm", className="me-2"),
        dbc.Button("✎ Save as New Version", id="btn-rule-update", color="warning", size="sm", className="me-2"),
        dbc.Button("↺ Clear", id="btn-rule-clear", color="secondary", size="sm"),
    ], style={"display":"flex","gap":"6px","flexWrap":"wrap"}),
], style=card)

rules_table_card = html.Div([
    html.Div([
        sec("Rules Library"),
        dbc.Button("⟳ Refresh", id="btn-rules-refresh", color="secondary", size="sm"),
    ], style={"display":"flex","alignItems":"center","justifyContent":"space-between",
              "paddingBottom":"10px","marginBottom":"14px","borderBottom":f"1px solid {C['border']}"}),
    dash_table.DataTable(
        id="rules-table",
        page_size=10,
        row_selectable="single",
        selected_rows=[],
        style_table={"overflowX":"auto"},
        style_header=tbl_header,
        style_cell=tbl_cell,
        style_data_conditional=[
            {"if":{"state":"selected"},"backgroundColor":C["accent2"],"border":f"1px solid {C['accent']}"},
            {"if":{"column_id":"rule_id"},"fontFamily":MONO,"color":C["accent"],"fontWeight":"600"},
            {"if":{"column_id":"version"},"fontFamily":MONO,"color":C["text3"]},
            {"if":{"column_id":"ruletype","filter_query":'{ruletype} = "Calculation"'},"color":C["amber"]},
            {"if":{"column_id":"ruletype","filter_query":'{ruletype} = "Filter"'},"color":C["red"]},
        ],
        tooltip_data=[],
        tooltip_duration=None,
    ),
    html.Div([
        dbc.Button("✕ Delete Selected", id="btn-rule-delete", color="danger", size="sm"),
    ], style={"marginTop":"12px"}),
], style=card)


# ── Tab 2: Rule Execution ─────────────────────────────────────────────────────

exec_form = html.Div([
    sec("Add Execution Row"),
    dbc.Row([
        dbc.Col(fld("Test ID", dcc.Dropdown(id="e-testid", placeholder="Select test…",
                                             value=1,
                                             style={"fontSize":"13px"}), required=True), md=3),
        dbc.Col(fld("Rule ID", dcc.Dropdown(id="e-ruleid", placeholder="Select rule…",
                                             style={"fontSize":"13px"}), required=True), md=5),
        dbc.Col(fld("Execution Sequence", dbc.Input(id="e-order", type="number", min=1, placeholder="1", style=inp), required=True), md=2),
        dbc.Col(fld("Created By", dbc.Input(id="e-createdby", disabled=True, style={**inp, "backgroundColor":C["bg3"], "color":C["text3"]}), required=False), md=2),
    ]),
    html.Div([
        dbc.Button("＋ Add Execution Row", id="btn-exec-add", color="primary", size="sm", className="me-2"),
        dbc.Button("↺ Clear", id="btn-exec-clear", color="secondary", size="sm"),
    ], style={"display":"flex","gap":"6px"}),
], style=card)

exec_table_card = html.Div([
    html.Div([
        sec("Rule Executions"),
        dbc.Button("⟳ Refresh", id="btn-exec-refresh", color="secondary", size="sm"),
    ], style={"display":"flex","alignItems":"center","justifyContent":"space-between",
              "paddingBottom":"10px","marginBottom":"14px","borderBottom":f"1px solid {C['border']}"}),
    dash_table.DataTable(
        id="exec-table",
        page_size=15,
        row_selectable="single",
        selected_rows=[],
        style_table={"overflowX":"auto"},
        style_header=tbl_header,
        style_cell=tbl_cell,
        style_data_conditional=[
            {"if":{"state":"selected"},"backgroundColor":C["accent2"],"border":f"1px solid {C['accent']}"},
            {"if":{"column_id":"test_id"},"fontFamily":MONO,"fontWeight":"600","color":C["amber"]},
            {"if":{"column_id":"rule_id"},"fontFamily":MONO,"color":C["accent"]},
            {"if":{"column_id":"ruleexecutionorder"},"fontFamily":MONO,"color":C["text3"],"textAlign":"center"},
        ],
    ),
    html.Div([
        dbc.Button("✕ Delete Selected", id="btn-exec-delete", color="danger", size="sm"),
    ], style={"marginTop":"12px"}),
], style=card)


# ── Tab 3: Tests Metadata ─────────────────────────────────────────────────────

tests_form = html.Div([
    sec("Add / Update Test"),
    dbc.Row([
        dbc.Col(fld("Test ID", dbc.Input(id="t-id", type="number", placeholder="e.g. 5376", style=inp), required=True), md=2),
        dbc.Col(fld("Test Title", dbc.Input(id="t-title", placeholder="e.g. Adverse Action Review Q1", style=inp), required=True), md=6),
        dbc.Col(fld("Business Line", dbc.Input(id="t-bizline", placeholder="e.g. BusinessLoanLending", style=inp), required=True), md=2),
        dbc.Col(fld("Created By", dbc.Input(id="t-createdby", disabled=True, style={**inp, "backgroundColor":C["bg3"], "color":C["text3"]}), required=False), md=2),
    ]),
    html.Div([
        dbc.Button("＋ Add Test", id="btn-test-add", color="primary", size="sm", className="me-2"),
        dbc.Button("✎ Save as New Version", id="btn-test-update", color="warning", size="sm", className="me-2"),
        dbc.Button("↺ Clear", id="btn-test-clear", color="secondary", size="sm"),
    ], style={"display":"flex","gap":"6px","flexWrap":"wrap"}),
], style=card)

tests_table_card = html.Div([
    html.Div([
        sec("Tests Metadata"),
        dbc.Button("⟳ Refresh", id="btn-tests-refresh", color="secondary", size="sm"),
    ], style={"display":"flex","alignItems":"center","justifyContent":"space-between",
              "paddingBottom":"10px","marginBottom":"14px","borderBottom":f"1px solid {C['border']}"}),
    dash_table.DataTable(
        id="tests-table",
        page_size=10,
        row_selectable="single",
        selected_rows=[],
        style_table={"overflowX":"auto"},
        style_header=tbl_header,
        style_cell=tbl_cell,
        style_data_conditional=[
            {"if":{"state":"selected"},"backgroundColor":C["accent2"],"border":f"1px solid {C['accent']}"},
            {"if":{"column_id":"test_id"},"fontFamily":MONO,"color":C["amber"],"fontWeight":"600"},
            {"if":{"column_id":"version"},"fontFamily":MONO,"color":C["text3"]},
        ],
    ),
    html.Div([
        dbc.Button("✕ Delete Selected", id="btn-test-delete", color="danger", size="sm"),
    ], style={"marginTop":"12px"}),
], style=card)


# ── Full layout ───────────────────────────────────────────────────────────────

app.layout = html.Div([
    navbar,
    make_toast("toast-success", "Success", "success"),
    make_toast("toast-error",   "Error",   "danger"),
    dcc.Store(id="store-selected-rule"),
    dcc.Store(id="store-selected-exec"),

    html.Div(
        dbc.Tabs([
            dbc.Tab(
                html.Div([rules_form, rules_table_card], style={"padding":"24px"}),
                label="Rules Library", tab_id="tab-rules",
            ),
            dbc.Tab(
                html.Div([tests_form, tests_table_card], style={"padding":"24px"}),
                label="Test Details", tab_id="tab-tests",
            ),
            dbc.Tab(
                html.Div([exec_form, exec_table_card], style={"padding":"24px"}),
                label="Test Execution Details", tab_id="tab-exec",
            ),
        ], id="tabs", active_tab="tab-rules"),
    ),

    html.Div("Created by Dennis Williams", style={
        "textAlign":"center","padding":"16px","fontSize":"11px","color":C["text3"],
        "fontFamily":MONO,"borderTop":f"1px solid {C['border']}","marginTop":"8px",
        "backgroundColor":C["bg1"],
    }),
], style={"backgroundColor":C["bg0"],"minHeight":"100vh","fontFamily":SANS})


# ─────────────────────────────────────────────────────────────────────────────
# Callbacks — Rules
# ─────────────────────────────────────────────────────────────────────────────

# Pre-fill Created By on both tabs with the current user's email
@callback(
    Output("r-createdby", "value", allow_duplicate=True),
    Output("e-createdby", "value", allow_duplicate=True),
    Output("t-createdby", "value", allow_duplicate=True),
    Input("tabs", "active_tab"),
    prevent_initial_call="initial_duplicate",
)
def prefill_user(_):
    user = get_current_user()
    return user, user, user

@callback(
    Output("r-name-col", "style"),
    Output("r-name",     "value", allow_duplicate=True),
    Input("r-type", "value"),
    prevent_initial_call=True,
)
def toggle_rule_name(rtype):
    if rtype == "Filter":
        return {"display": "none"}, ""
    return {"display": "block"}, no_update


@callback(
    Output("rules-table", "data"),
    Output("rules-table", "columns"),
    Output("rules-table", "tooltip_data"),
    Input("btn-rules-refresh", "n_clicks"),
    Input("toast-success",     "is_open"),
)
def load_rules(_, __):
    df = fetch_rules()
    if df.empty:
        cols = ["rule_id","ruletype","rulename","ruledescription","rulesql","createdby","createdtime","version"]
        return [], [{"name":c,"id":c} for c in cols], []
    # Truncate SQL for display
    df["sql_preview"] = df["rulesql"].str[:80].str.replace("\n"," ") + "…"
    df["createdtime"]  = pd.to_datetime(df["createdtime"], utc=True, errors="coerce") \
                           .dt.strftime("%Y-%m-%d %H:%M:%S")
    display_cols = ["rule_id","version","ruletype","rulename","ruledescription","message_level","sql_preview","createdby","createdtime"]
    columns = [
        {"name":"Rule ID",       "id":"rule_id"},
        {"name":"Ver",           "id":"version"},
        {"name":"Type",          "id":"ruletype"},
        {"name":"Name",          "id":"rulename"},
        {"name":"Description",   "id":"ruledescription"},
        {"name":"Msg Level",     "id":"message_level"},
        {"name":"SQL Logic",     "id":"sql_preview"},
        {"name":"Created By",    "id":"createdby"},
        {"name":"Created Time",  "id":"createdtime"},
    ]
    # Full SQL on tooltip
    tooltip = [{
        "sql_preview": {"value": row["rulesql"], "type": "markdown"}
    } for _, row in df.iterrows()]
    return df[display_cols].to_dict("records"), columns, tooltip


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-rule-add", "n_clicks"),
    State("r-id",        "value"),
    State("r-type",      "value"),
    State("r-name",      "value"),
    State("r-desc",      "value"),
    State("r-logic",     "value"),
    State("r-msglevel",  "value"),
    State("r-sql",       "value"),
    State("r-createdby", "value"),
    prevent_initial_call=True,
)
def add_rule(n, rid, rtype, rname, rdesc, rlogic, rmsglevel, rsql, rby):
    if not n: return no_update, no_update, no_update, no_update
    missing = [f for f,v in [("Rule ID",rid),("Rule Type",rtype),
                               ("Rule SQL",rsql),("Created By",rby)] if not (v or "").strip()]
    if rtype != "Filter" and not (rname or "").strip():
        missing.append("Rule Name")
    if missing: return False,"",True,f"Required: {', '.join(missing)}"
    ok, msg = insert_rule(rid.strip(), rtype, rname or "", rdesc or "", rsql.strip(),
                          rlogic or "", rmsglevel or "LOAN", rby.strip())
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-rule-update", "n_clicks"),
    State("r-id",        "value"),
    State("r-type",      "value"),
    State("r-name",      "value"),
    State("r-desc",      "value"),
    State("r-logic",     "value"),
    State("r-msglevel",  "value"),
    State("r-sql",       "value"),
    State("r-createdby", "value"),
    prevent_initial_call=True,
)
def update_rule(n, rid, rtype, rname, rdesc, rlogic, rmsglevel, rsql, rby):
    if not n: return no_update, no_update, no_update, no_update
    missing = [f for f,v in [("Rule ID",rid),("Rule Type",rtype),
                               ("Rule SQL",rsql),("Created By",rby)] if not (v or "").strip()]
    if rtype != "Filter" and not (rname or "").strip():
        missing.append("Rule Name")
    if missing: return False,"",True,f"Required: {', '.join(missing)}"
    ok, msg = insert_rule(rid.strip(), rtype, rname or "", rdesc or "", rsql.strip(),
                          rlogic or "", rmsglevel or "LOAN", rby.strip())
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-rule-delete", "n_clicks"),
    State("rules-table", "selected_rows"),
    State("rules-table", "data"),
    prevent_initial_call=True,
)
def delete_rule_cb(n, selected, data):
    if not n: return no_update, no_update, no_update, no_update
    if not selected or not data: return False,"",True,"Select a row to delete."
    row = data[selected[0]]
    ok, msg = delete_rule(row["rule_id"], row["version"])
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("r-id",        "value"),
    Output("r-type",      "value"),
    Output("r-name",      "value"),
    Output("r-desc",      "value"),
    Output("r-logic",     "value"),
    Output("r-msglevel",  "value"),
    Output("r-sql",       "value"),
    Output("r-createdby", "value"),
    Input("btn-rule-clear", "n_clicks"),
    Input("rules-table",    "selected_rows"),
    State("rules-table",    "data"),
    prevent_initial_call=True,
)
def rule_form_sync(clear_n, selected, data):
    if ctx.triggered_id == "btn-rule-clear" or not selected or not data:
        return "", None, "", "", "", "LOAN", "", get_current_user()
    row = data[selected[0]]
    df = fetch_rules()
    match = df[(df["rule_id"]==row["rule_id"]) & (df["version"]==row["version"])]
    full_sql  = match["rulesql"].iloc[0]      if not match.empty else ""
    rlogic    = match["rule_logic"].iloc[0]   if not match.empty else ""
    rmsglevel = match["message_level"].iloc[0] if not match.empty else "LOAN"
    return (row["rule_id"], row["ruletype"], row.get("rulename",""),
            row.get("ruledescription",""), rlogic or "", rmsglevel or "LOAN",
            full_sql, row["createdby"])


# ─────────────────────────────────────────────────────────────────────────────
# Callbacks — Executions
# ─────────────────────────────────────────────────────────────────────────────

@callback(
    Output("exec-table", "data"),
    Output("exec-table", "columns"),
    Input("btn-exec-refresh", "n_clicks"),
    Input("toast-success",    "is_open"),
    Input("e-testid",         "value"),
)
def load_executions(_, __, selected_test_id):
    df = fetch_executions()
    if df.empty:
        cols = ["test_id","rule_id","rulename","ruleexecutionorder","createdby","createdtime"]
        return [], [{"name":c,"id":c} for c in cols]
    # Always show test_id = 1; also show the selected test_id if different
    if selected_test_id is not None:
        ids_to_show = {1, int(selected_test_id)}
        df = df[df["test_id"].isin(ids_to_show)]
    else:
        df = df[df["test_id"] == 1]
    df["createdtime"] = pd.to_datetime(df["createdtime"], utc=True, errors="coerce") \
                          .dt.strftime("%Y-%m-%d %H:%M:%S")
    columns = [
        {"name":"Test ID",            "id":"test_id"},
        {"name":"Rule ID",            "id":"rule_id"},
        {"name":"Rule Name",          "id":"rulename"},
        {"name":"Execution Sequence", "id":"ruleexecutionorder"},
        {"name":"Created By",         "id":"createdby"},
        {"name":"Created Time",       "id":"createdtime"},
    ]
    return df.to_dict("records"), columns


@callback(
    Output("e-testid", "options"),
    Input("tabs",          "active_tab"),
    Input("toast-success", "is_open"),
)
def refresh_test_dropdown(tab, _):
    return fetch_test_ids()


@callback(
    Output("e-order", "value", allow_duplicate=True),
    Input("e-testid", "value"),
    prevent_initial_call=True,
)
def auto_exec_sequence(test_id):
    if test_id is None:
        return no_update
    return get_next_exec_order(int(test_id))


@callback(
    Output("e-ruleid", "options"),
    Input("tabs",              "active_tab"),
    Input("toast-success",    "is_open"),
)
def refresh_rule_dropdown(tab, _):
    return fetch_rule_ids()


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-exec-add", "n_clicks"),
    State("e-testid",    "value"),
    State("e-ruleid",    "value"),
    State("e-order",     "value"),
    State("e-createdby", "value"),
    prevent_initial_call=True,
)
def add_execution(n, test_id, rule_id, order, created_by):
    if not n: return no_update, no_update, no_update, no_update
    missing = [f for f,v in [("Test ID",test_id),("Rule ID",rule_id),
                               ("Order",order),("Created By",created_by)] if not str(v or "").strip()]
    if missing: return False,"",True,f"Required: {', '.join(missing)}"
    ok, msg = insert_execution(test_id, rule_id, order, str(created_by).strip())
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-exec-delete", "n_clicks"),
    State("exec-table", "selected_rows"),
    State("exec-table", "data"),
    prevent_initial_call=True,
)
def delete_exec_cb(n, selected, data):
    if not n: return no_update, no_update, no_update, no_update
    if not selected or not data: return False,"",True,"Select a row to delete."
    row = data[selected[0]]
    ok, msg = delete_execution(row["test_id"], row["rule_id"])
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("e-testid",    "value"),
    Output("e-ruleid",    "value"),
    Output("e-order",     "value"),
    Output("e-createdby", "value"),
    Input("btn-exec-clear",  "n_clicks"),
    Input("exec-table",      "selected_rows"),
    State("exec-table",      "data"),
    prevent_initial_call=True,
)
def exec_form_sync(clear_n, selected, data):
    if ctx.triggered_id == "btn-exec-clear" or not selected or not data:
        return 1, None, None, get_current_user()
    row = data[selected[0]]
    return row["test_id"], row["rule_id"], row["ruleexecutionorder"], row["createdby"]


# ─────────────────────────────────────────────────────────────────────────────
# Callbacks — Tests Metadata
# ─────────────────────────────────────────────────────────────────────────────

@callback(
    Output("tests-table", "data"),
    Output("tests-table", "columns"),
    Input("btn-tests-refresh", "n_clicks"),
    Input("toast-success",     "is_open"),
)
def load_tests(_, __):
    df = fetch_tests()
    if df.empty:
        cols = ["test_id","version","test_title","business_line","createdby","createdtime"]
        return [], [{"name":c,"id":c} for c in cols]
    df["createdtime"] = pd.to_datetime(df["createdtime"], utc=True, errors="coerce") \
                          .dt.strftime("%Y-%m-%d %H:%M:%S")
    columns = [
        {"name":"Test ID",       "id":"test_id"},
        {"name":"Ver",           "id":"version"},
        {"name":"Test Title",    "id":"test_title"},
        {"name":"Business Line", "id":"business_line"},
        {"name":"Created By",    "id":"createdby"},
        {"name":"Created Time",  "id":"createdtime"},
    ]
    return df.to_dict("records"), columns


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-test-add", "n_clicks"),
    State("t-id",        "value"),
    State("t-title",     "value"),
    State("t-bizline",   "value"),
    State("t-createdby", "value"),
    prevent_initial_call=True,
)
def add_test(n, tid, title, bizline, createdby):
    if not n: return no_update, no_update, no_update, no_update
    missing = [f for f,v in [("Test ID",tid),("Test Title",title),("Business Line",bizline)] if not str(v or "").strip()]
    if missing: return False,"",True,f"Required: {', '.join(missing)}"
    ok, msg = insert_test(tid, str(title).strip(), str(bizline).strip(), str(createdby or "").strip())
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-test-update", "n_clicks"),
    State("t-id",        "value"),
    State("t-title",     "value"),
    State("t-bizline",   "value"),
    State("t-createdby", "value"),
    prevent_initial_call=True,
)
def update_test(n, tid, title, bizline, createdby):
    if not n: return no_update, no_update, no_update, no_update
    missing = [f for f,v in [("Test ID",tid),("Test Title",title),("Business Line",bizline)] if not str(v or "").strip()]
    if missing: return False,"",True,f"Required: {', '.join(missing)}"
    ok, msg = insert_test(tid, str(title).strip(), str(bizline).strip(), str(createdby or "").strip())
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("toast-success", "is_open",  allow_duplicate=True),
    Output("toast-success", "children", allow_duplicate=True),
    Output("toast-error",   "is_open",  allow_duplicate=True),
    Output("toast-error",   "children", allow_duplicate=True),
    Input("btn-test-delete", "n_clicks"),
    State("tests-table", "selected_rows"),
    State("tests-table", "data"),
    prevent_initial_call=True,
)
def delete_test_cb(n, selected, data):
    if not n: return no_update, no_update, no_update, no_update
    if not selected or not data: return False,"",True,"Select a row to delete."
    row = data[selected[0]]
    ok, msg = delete_test(row["test_id"], row["version"])
    return (True,msg,False,"") if ok else (False,"",True,msg)


@callback(
    Output("t-id",        "value"),
    Output("t-title",     "value"),
    Output("t-bizline",   "value"),
    Output("t-createdby", "value"),
    Input("btn-test-clear", "n_clicks"),
    Input("tests-table",    "selected_rows"),
    State("tests-table",    "data"),
    prevent_initial_call=True,
)
def test_form_sync(clear_n, selected, data):
    if ctx.triggered_id == "btn-test-clear" or not selected or not data:
        return None, "", "", get_current_user()
    row = data[selected[0]]
    return row["test_id"], row["test_title"], row["business_line"], row["createdby"]


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)