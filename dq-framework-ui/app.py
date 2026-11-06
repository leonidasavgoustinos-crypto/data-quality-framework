"""
DQ Framework UI — Databricks App (Streamlit)

One page per configuration notebook (01_rule_type … 08_rule_assignment), plus a start page.

The app never writes to the configuration tables itself: all writes go through
the existing notebooks, so the SCD2 logic stays in one place.
Queries run as the signed-in user (user authorization scope "sql").
"""

import json
import os
import re

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
ENV = os.getenv("DQ_ENV", "dev")
# Catalog of the DQ framework. Default: analytics_dq_<env>; can be overridden.
CATALOG = os.getenv("DQ_CATALOG") or f"analytics_dq_{ENV}"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
# One Job ("config runner") runs run_config_notebook.py, which calls the configuration
# notebook 01–08 named in its parameters. Empty -> the app is read-only.
CONFIG_JOB_ID = os.getenv("CONFIG_JOB_ID")

# Note: table, project, rule_type, rule_dimension live in the *metadata* schema.
Q_TABLES = f"SELECT DISTINCT table_nk FROM {CATALOG}.metadata.table WHERE is_active ORDER BY 1"
Q_TEMPLATES = f"""
    SELECT name, template_nk, description, engine_type, scope, statement
    FROM {CATALOG}.configuration.rule_template WHERE is_active ORDER BY name
"""
Q_POLICIES = f"SELECT DISTINCT policy_nk FROM {CATALOG}.configuration.run_policy WHERE is_active ORDER BY 1"
Q_APPLY_AT = f"SELECT DISTINCT apply_at_nk FROM {CATALOG}.configuration.apply_at WHERE is_active ORDER BY 1"
Q_PROJECTS = f"SELECT DISTINCT project_nk FROM {CATALOG}.metadata.project WHERE is_active ORDER BY 1"

# Active checks with readable names. Same joins as reporting.v_consolidated_configuration_metadata,
# but that view also returns switched-off / old versions (it has no filter on rule_assignment.is_active).
Q_ACTIVE_CHECKS = f"""
SELECT p.project, m.table_nk, rt.name AS rule_name, ra.parameters_identifiers, ra.parameters_values,
       rp.criticality, rp.category, rp.quarantine AS quarantine_on_fail, ap.apply_at, ap.is_project_specific,
       rty.rule_type, rd.rule_dimension, rt.engine_type, rt.scope, rt.description AS rule_description,
       m.layer, m.filter_field, m.filter_field_type, ra.added_by, ra.valid_from AS active_since
FROM {CATALOG}.configuration.rule_assignment ra
LEFT JOIN {CATALOG}.metadata.`table` m             ON ra.table_id = m.table_id
LEFT JOIN {CATALOG}.metadata.project p             ON ra.project_id = p.project_id
LEFT JOIN {CATALOG}.configuration.run_policy rp    ON ra.policy_id = rp.policy_id
LEFT JOIN {CATALOG}.configuration.rule_template rt ON ra.rule_template_id = rt.rule_template_id
LEFT JOIN {CATALOG}.metadata.rule_type rty         ON rt.rule_type_id = rty.rule_type_id
LEFT JOIN {CATALOG}.metadata.rule_dimension rd     ON rt.rule_dimension_id = rd.rule_dimension_id
LEFT JOIN {CATALOG}.configuration.apply_at ap      ON ra.apply_at_id = ap.apply_at_id
WHERE ra.is_active
ORDER BY m.table_nk, rt.name
"""

st.set_page_config(page_title="DQ Framework", layout="wide")

# Full-width tables on both older and newer Streamlit versions
# (Databricks Apps ships its own Streamlit; newer ones use width="stretch").
def _ver(v):
    return tuple(int(x) for x in re.findall(r"\d+", v)[:2])
WIDE = {"width": "stretch"} if _ver(st.__version__) >= (1, 50) else {"use_container_width": True}
cfg = Config()  # picks up the app's credentials automatically inside Databricks Apps


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------
def current_user() -> str:
    # Databricks Apps forwards the signed-in user's email in the request headers
    try:
        return st.context.headers.get("X-Forwarded-Email", "dq_app")
    except Exception:
        return "dq_app"


def _user_token():
    """Token of the signed-in user (needs the 'sql' user-authorization scope on the app)."""
    try:
        return st.context.headers.get("X-Forwarded-Access-Token")
    except Exception:
        return None


@st.cache_resource(ttl=3600, max_entries=20, show_spinner=False)
def _connection(_token, user: str):
    """One open connection per user, reused across queries (opening one takes 1–2 s)."""
    auth = {"access_token": _token} if _token else {"credentials_provider": lambda: cfg.authenticate}
    return sql.connect(server_hostname=cfg.host,
                       http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}", **auth)


@st.cache_data(ttl=300, show_spinner=False)
def _query(q: str, user: str, _token) -> pd.DataFrame:
    # Query as the signed-in user when possible, otherwise as the app's service principal.
    for attempt in (1, 2):
        try:
            with _connection(_token, f"{user}|{hash(_token)}").cursor() as cur:
                cur.execute(q)
                return cur.fetchall_arrow().to_pandas()
        except Exception:
            if attempt == 2:
                raise
            _connection.clear()   # connection expired/closed -> open a new one and retry


def query(q: str) -> pd.DataFrame:
    return _query(q, current_user(), _user_token())


def options(q: str) -> list:
    try:
        return query(q).iloc[:, 0].tolist()
    except Exception as e:  # show the problem instead of crashing the page
        st.error(f"Could not load options: {e}")
        return []


def first_present(df: pd.DataFrame, candidates: list):
    return next((c for c in candidates if c in df.columns), None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def placeholders(statement: str):
    """${i:column} -> identifier, ${v:min_val} -> value. Keeps the order they appear in."""
    idents = list(dict.fromkeys(re.findall(r"\$\{i:(\w+)\}", statement or "")))
    values = list(dict.fromkeys(re.findall(r"\$\{v:(\w+)\}", statement or "")))
    return idents, values


def table_columns(table_nk: str) -> list:
    # much faster than information_schema: returns no rows, only the column names
    try:
        return list(query(f"SELECT * FROM {table_nk} LIMIT 0").columns)
    except Exception as e:
        st.error(f"Could not read the columns of {table_nk}: {e}")
        return []



# ---------------------------------------------------------------------------
# Plain-language help (for non-technical users)
# ---------------------------------------------------------------------------
GUIDE = {
    "01_rule_type": ("What kind of check is it?",
        "A simple label that groups checks, e.g. **technical** (format, empty values) or **business** "
        "(rules that come from the business).",
        "You rarely need to change this. Add one only if your team agrees on a new category."),
    "02_rule_dimension": ("What does the check measure?",
        "The quality aspect a check looks at, e.g. **completeness** (nothing missing), **validity** "
        "(values make sense), **uniqueness** (no duplicates).",
        "You rarely need to change this."),
    "03_project": ("Which project is it for?",
        "Projects that use the data quality checks. A check can belong to a project, or to none.",
        "Add a project when a new team or use case starts using the framework."),
    "04_run_policy": ("What happens when a check fails?",
        "**Error** stops/flags the data as bad, **warning** only informs. **Quarantine** moves the bad rows "
        "to a separate table so they don't reach the reports.",
        "The name is built for you, e.g. error.high.quarantine."),
    "05_apply_at": ("At which stage is the data checked?",
        "The data layer where the check runs, e.g. **gold** (final, clean data).",
        "You rarely need to change this."),
    "06_rule_template": ("The check itself",
        "A reusable check, written once, e.g. *“value must not be empty”* or *“value must be between a "
        "minimum and a maximum”*. It is not linked to a table yet.",
        "Create one only if none of the existing checks fits. Then use **08_rule_assignment → ➕ Add a check** to apply it to a table."),
    "07_table": ("Which tables can be checked?",
        "Tables that the framework knows about. The table must already exist; this only registers it.",
        "Register a table once, before adding checks to it."),
    "08_rule_assignment": ("Which checks run on which tables?",
        "Here you **see** every active check, **add** a check to a table, or **switch off** a check.",
        "Adding: pick a table, pick a check, choose the column and what happens if it fails. "
        "It runs from the next data quality run."),
}

HELP = {
    "rule_type": "Short name, lower case, no spaces. Example: technical",
    "rule_dimension": "Short name, lower case, no spaces. Example: timeliness",
    "project": "Short name, lower case, no spaces. Example: sales_reporting",
    "description": "One sentence in plain words, so others understand what this is.",
    "project_description": "One sentence: what the project is.",
    "category": "Type error (serious) or warning (just inform).",
    "criticality": "How important: high, medium or low.",
    "quarantine": "ON = rows that fail are moved aside so they don't reach reports.",
    "apply_at": "Data layer, e.g. bronze, silver, gold.",
    "is_project_specific": "ON = only for one project. OFF = for all projects (most common).",
}

GLOSSARY = {
    "Check (template)": "A rule written once, e.g. “must not be empty”.",
    "Assignment": "A check applied to a specific table and column.",
    "Policy": "What happens when a check fails (error/warning, quarantine or not).",
    "Quarantine": "A side table where failing rows are kept, so they don't reach reports.",
    "Dimension": "What a check measures: completeness, validity, uniqueness…",
    "History": "Nothing is ever erased: every change keeps the old version.",
    "Delete": "Switches something off. It stays in the history and can be added again.",
}

POLICY_LABEL = {"error": "❌ Error", "warning": "⚠️ Warning"}


def friendly_policy(nk: str) -> str:
    """error.high.quarantine -> '❌ Error · high · bad rows go to quarantine'"""
    parts = str(nk).split(".")
    if len(parts) != 3:
        return str(nk)
    cat, crit, q = parts
    qtxt = "bad rows go to quarantine" if q == "quarantine" else "no quarantine"
    return f"{POLICY_LABEL.get(cat, cat)} · {crit} · {qtxt}   ({nk})"


def page_intro(key: str, technical: str = ""):
    title, what, tip = GUIDE.get(key, (key, "", ""))
    st.title(title)
    st.caption(key)
    if what:
        st.info(f"{what}\n\n💡 {tip}")
    if technical:
        with st.expander("Technical details"):
            st.caption(technical)


def show_flash():
    msg = st.session_state.pop("flash", None)
    if msg:
        st.success(msg)



def confirm_breaking_change(n_checks: int, key: str) -> bool:
    """Editing something that active checks use creates a new version with a new ID; the framework
    joins assignments by ID, so those checks would stop running. Warn and ask for confirmation.
    Returns True when it is OK to save."""
    if not n_checks:
        return True
    st.error(f"⚠️ **{n_checks} active check(s) use this.** If you save this change, those checks will "
             "**stop running** (they stay linked to the old version). To keep them running, they have to be "
             "added again in 08_rule_assignment after this change.")
    return st.checkbox("I understand — save anyway", key=key)


def run_config_notebook(notebook: str, params: dict) -> bool:
    """Run a configuration notebook (01–08) through the single 'config runner' Job.

    The Job runs run_config_notebook.py, which calls ./<notebook> with these params,
    exactly like filling in the widgets and pressing Run. Returns True on success.
    """
    with st.spinner(f"Running {notebook}…"):
        run = WorkspaceClient().jobs.run_now(
            job_id=int(CONFIG_JOB_ID),
            notebook_params={"notebook": notebook, "params": json.dumps(params)},
        ).result()
    state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
    if state == "SUCCESS":
        st.cache_data.clear()
        return True
    st.error(f"Job finished with state {state}. Open the run: {run.run_page_url}")
    return False


def notebook_call(notebook: str, params: dict) -> str:
    """The equivalent dbutils.notebook.run(...) line, so it can also be run by hand."""
    return f'dbutils.notebook.run("./{notebook}", 60, {json.dumps(params, ensure_ascii=False)})'


def save_button(notebook: str, params: dict, label: str, disabled: bool = False, key: str = None):
    """Shows the exact call, and a Save/Delete button when the config Job is set up."""
    with st.expander("Technical details: exactly what will be sent"):
        st.json(params)
        st.code(notebook_call(notebook, params), language="python")
    if not CONFIG_JOB_ID:
        st.info("Saving is not switched on yet in this app. Ask the DQ team to save it for you.")
        return
    if st.button(label, type="primary", disabled=disabled, key=key):
        if run_config_notebook(notebook, params):
            verb = "deleted" if params.get("action") == "delete" else "saved"
            st.session_state["flash"] = f"✅ Done — your change was {verb}. You can see it in the list above."
            st.rerun()


# ---------------------------------------------------------------------------
# Configuration entities: one generic page for notebooks 01–05
# ---------------------------------------------------------------------------
# key fields   = the natural key (what identifies the row)
# attr fields  = the other fields; changing them closes the old version (SCD2)
# cascade      = what the notebook also deactivates on delete
ENTITIES = {
    "Rule types": dict(
        notebook="01_rule_type", table="metadata.rule_type", id="rule_type_id", nk="rule_type_nk",
        key=[("rule_type", "Rule type", "text", "e.g. technical")],
        attrs=[("description", "Description", "area", "")],
        cascade=("templates", "rule_type_id"),
        about="High-level category of a rule (e.g. technical, business).",
    ),
    "Rule dimensions": dict(
        notebook="02_rule_dimension", table="metadata.rule_dimension", id="rule_dimension_id",
        nk="rule_dimension_nk",
        key=[("rule_dimension", "Dimension", "text", "e.g. timeliness")],
        attrs=[("description", "Description", "area", "")],
        cascade=("templates", "rule_dimension_id"),
        about="Which quality aspect a rule measures (completeness, validity, …).",
    ),
    "Projects": dict(
        notebook="03_project", table="metadata.project", id="project_id", nk="project_nk",
        key=[("project", "Project", "text", "e.g. demo_project")],
        attrs=[("project_description", "Description", "area", "")],
        cascade=("assignments", "project_id"),
        about="Projects that use the framework.",
    ),
    "Run policies": dict(
        notebook="04_run_policy", table="configuration.run_policy", id="policy_id", nk="policy_nk",
        key=[("category", "Category", "text", "error or warning"),
             ("criticality", "Criticality", "text", "e.g. high, low"),
             ("quarantine", "Quarantine failing rows", "bool", "")],
        attrs=[],
        cascade=("assignments", "policy_id"),
        about="Severity and quarantine behaviour. The name is built as category.criticality.quarantine.",
        nk_of=lambda v: f"{v['category']}.{v['criticality']}.{'quarantine' if v['quarantine'] else 'no_quarantine'}",
    ),
    "Apply at": dict(
        notebook="05_apply_at", table="configuration.apply_at", id="apply_at_id", nk="apply_at_nk",
        key=[("apply_at", "Layer / transition", "text", "e.g. gold, bronze_to_gold")],
        attrs=[("is_project_specific", "Project specific", "bool", "")],
        cascade=("assignments", "apply_at_id"),
        about="Where a rule is applied. The name is built as apply_at.project_specific / project_agnostic.",
    ),
}


def _as_param(v) -> str:
    """Widget values are strings: booleans become 'true' / 'false'."""
    if isinstance(v, (bool,)) or str(v) in ("True", "False"):
        return "true" if str(v) == "True" else "false"
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)


def _usage(rows: pd.DataFrame, id_col: str, nk_col: str, cascade):
    """How many active templates / assignments use each entity, counted by natural key
    (so references to older versions of the same entity are counted too)."""
    kind, fk = cascade
    ids = rows[[id_col, nk_col]]
    ra = query(f"SELECT * FROM {CATALOG}.configuration.rule_assignment")
    # keep only the columns we need: rule_assignment also has *_nk columns
    # (project_nk, apply_at_nk, …) that would clash with the entity's own nk column
    cols = list(dict.fromkeys(["rule_template_id", fk] if kind == "assignments" else ["rule_template_id"]))
    ra = ra[ra["is_active"] == True][cols].copy()
    n_tpl = pd.Series(dtype=int)
    if kind == "templates":
        tpl = query(f"SELECT * FROM {CATALOG}.configuration.rule_template")
        tpl = tpl[tpl["is_active"] == True][["rule_template_id", "name", fk]]
        tpl = tpl.merge(ids, left_on=fk, right_on=id_col, how="inner")
        n_tpl = tpl.groupby(nk_col).size()
        n_ra = ra.merge(tpl[["rule_template_id", nk_col]], on="rule_template_id").groupby(nk_col).size()
        names = tpl.groupby(nk_col)["name"].apply(lambda s: ", ".join(sorted(s.astype(str).unique())))
    else:
        n_ra = ra.merge(ids, left_on=fk, right_on=id_col, how="inner").groupby(nk_col).size()
        names = pd.Series(dtype=str)
    return n_tpl, n_ra, names


def page_entity(title: str):
    e = ENTITIES[title]
    page_intro(e["notebook"], f"{e['about']} · table {CATALOG}.{e['table']} · "
                               f"saved by notebook configuration/{e['notebook']}")
    show_flash()

    rows = query(f"SELECT * FROM {CATALOG}.{e['table']}")
    if "enrich" in e:                     # e.g. ids -> names, arrays -> text
        rows = e["enrich"](rows)
    nk = e["nk"]
    active = rows[rows["is_active"] == True].sort_values(nk).copy()
    n_tpl, n_ra, tpl_names = _usage(rows, e["id"], nk, e["cascade"])
    if e["cascade"][0] == "templates":
        active["Templates"] = active[nk].map(n_tpl).fillna(0).astype(int)
    active["Assignments"] = active[nk].map(n_ra).fillna(0).astype(int)

    # --- Current + history ----------------------------------------------------
    fields = [f[0] for f in e["key"] + e["attrs"]]
    # single-field keys are the same as the name -> show only the name
    key_cols = [f[0] for f in e["key"]] if len(e["key"]) > 1 else []
    shown = [nk] + [f for f in key_cols + [a[0] for a in e["attrs"]] if f in active.columns]
    shown += [c for c in ["Templates", "Assignments", "added_by", "valid_from"] if c in active.columns]
    labels = {nk: "Name", "added_by": "Added by", "valid_from": "Valid from",
              **{f: lbl for f, lbl, *_ in e["key"] + e["attrs"]}}
    st.subheader(f"What exists today ({len(active)})")
    st.dataframe(active[shown].rename(columns=labels), **WIDE, hide_index=True)
    with st.expander("History — every change ever made (nothing is erased)"):
        hist = rows.sort_values([nk, "valid_from"])
        st.dataframe(hist[[c for c in [nk] + fields + ["is_active", "added_by", "valid_from", "valid_to",
                                                      e["id"]] if c in hist.columns]],
                     **WIDE, hide_index=True)

    # --- Change ---------------------------------------------------------------
    st.subheader("Make a change")
    tab_add, tab_del = st.tabs(["➕ Add or edit", "🗑️ Switch off (delete)"])
    by_nk = {r[nk]: r for _, r in active.iterrows()}
    key_name = e["key"][0][0]                       # field the notebook MERGEs on
    by_name = {r[key_name]: r for _, r in active.iterrows()}

    with tab_add:
      if "form" in e:                     # templates / tables have their own form
        e["form"](e, title, active, by_nk, n_ra)
      else:
        can_edit = bool(e["attrs"]) and bool(by_name)
        mode = st.radio("What do you want to do?",
                        ["Add new", "Edit existing"] if can_edit else ["Add new"],
                        horizontal=True, key=f"mode_{title}",
                        help="Add new = create something that does not exist yet. "
                             "Edit existing = change the description of something in the list above.")
        values, current = {}, None
        if mode == "Edit existing":
            pick = st.selectbox(e["key"][0][1], list(by_name), key=f"pick_{title}")
            current = by_name[pick]
            values[key_name] = pick
        for fname, label, kind, hint in (e["key"] if mode == "Add new" else []) + e["attrs"]:
            default = current[fname] if current is not None and fname in current else None
            k = f"{title}_{fname}_{mode}_{values.get(key_name, '')}"
            h = HELP.get(fname)
            if kind == "bool":
                values[fname] = st.toggle(label, value=bool(default) if default is not None else True, key=k, help=h)
            elif kind == "area":
                values[fname] = st.text_area(label, value="" if default is None or pd.isna(default) else str(default),
                                             placeholder=hint, key=k, help=h).strip()
            else:
                values[fname] = st.text_input(label, placeholder=hint, key=k, help=h).strip()

        missing = [lbl for f, lbl, kind, _ in e["key"] if kind != "bool" and not values.get(f)]
        if missing:
            st.info(f"👉 Please fill in: **{', '.join(missing)}**.")
        else:
            for f, lbl, kind, _ in e["key"]:
                v = values[f]
                if kind == "text" and (v != v.lower() or " " in v):
                    st.warning(f"{lbl}: please use lower case and no spaces, "
                               f"e.g. `{v.lower().replace(' ', '_')}` — otherwise it may not match later.")
            same = False
            lookup = by_nk if "nk_of" in e else by_name
            name = e["nk_of"](values) if "nk_of" in e else values[key_name]
            if name not in lookup:
                st.success(f"🆕 This will **add** `{name}`.")
            else:
                cur = lookup[name]
                changed = [lbl for f, lbl, _, _ in e["attrs"]
                           if _as_param(cur.get(f)) != _as_param(values.get(f))]
                if not changed and e["attrs"]:
                    same = True
                    st.info(f"`{name}` already exists, exactly like this — nothing to save. "
                            "You can see it in the list above.")
                elif not e["attrs"]:
                    same = True
                    st.info(f"`{cur[nk]}` already exists — nothing to add.")
                else:
                    st.success(f"✏️ This will **update** `{name}` ({', '.join(changed) or 'updated'}). "
                               "The old version is kept in the history.")
                    if title == "Apply at":
                        st.warning("The name changes with *Project specific* (e.g. gold.project_agnostic → "
                                   "gold.project_specific).")
                    if e["cascade"][0] == "assignments" and not confirm_breaking_change(
                            int(n_ra.get(cur[nk], 0)), key=f"brk_{title}"):
                        same = True
            params = {"target_environment": ENV, **{f: _as_param(v) for f, v in values.items()},
                      "added_by": current_user(), "action": "insert/update"}
            save_button(e["notebook"], params, "💾 Save", disabled=same, key=f"save_{title}")

    with tab_del:
        if not by_nk:
            st.info("Nothing active to delete.")
        else:
            pick = st.selectbox("Which one do you want to switch off?", list(by_nk), key=f"del_{title}")
            row = by_nk[pick]
            n_t, n_a = int(n_tpl.get(pick, 0)), int(n_ra.get(pick, 0))
            what = (f"**{n_t} template(s)** and **{n_a} rule assignment(s)**"
                    if e["cascade"][0] == "templates" else f"**{n_a} rule assignment(s)**")
            if n_t or n_a:
                st.error(f"⚠️ Switching off **{pick}** will also switch off {what} that use it. "
                         "Those checks will **stop running**. Nothing is erased — it all stays in the history.")
            else:
                st.warning(f"**{pick}** is not used by any check, so switching it off affects nothing else. "
                           "It stays in the history.")
            if n_t and pick in tpl_names:
                st.caption(f"Checks that will be switched off: {tpl_names[pick]}")
            confirm = st.text_input(f"To be safe, type **{pick}** to confirm", key=f"confirm_{title}")
            params = {"target_environment": ENV,
                      **{f: _as_param(row.get(f)) for f, *_ in e["key"] + e["attrs"]},
                      "added_by": current_user(), "action": "delete"}
            save_button(e["notebook"], params, "Switch off", disabled=(confirm != pick), key=f"delbtn_{title}")



def tab_assignment_list():
    df = query(Q_ACTIVE_CHECKS)

    # Filters
    tcol = first_present(df, ["table_nk", "table_name", "table"])
    pcol = first_present(df, ["project"])
    c1, c2, c3 = st.columns([1, 1, 2])
    if pcol:
        p = c1.selectbox("Project", ["All"] + sorted(df[pcol].dropna().unique().tolist()))
        if p != "All":
            df = df[df[pcol] == p]
    if tcol:
        t = c2.selectbox("Table", ["All"] + sorted(df[tcol].dropna().unique().tolist()))
        if t != "All":
            df = df[df[tcol] == t]
    search = c3.text_input("Search", placeholder="Type any word: table, column, check…")
    if search:
        mask = df.astype(str).apply(lambda col: col.str.contains(search, case=False)).any(axis=1)
        df = df[mask]

    # Hide technical columns, put the useful ones first
    noisy = {"catalog", "schema", "project_description", "quarantine", "valid_from", "valid_to"}
    noisy |= {c for c in df.columns if c.endswith("_id")}
    preferred = [c for c in [tcol, "rule_name", "template", "parameters_identifiers", "parameters_values",
                             "policy", "criticality", "category", "apply_at", "rule_type",
                             "rule_dimension", "engine_type", "project"] if c and c in df.columns]
    rest = [c for c in df.columns if c not in preferred and c not in noisy]
    view = df[preferred + rest].reset_index(drop=True).rename(columns={
        "table_nk": "Table", "rule_name": "Check", "parameters_identifiers": "Column(s)",
        "parameters_values": "Values", "criticality": "Criticality", "apply_at": "Stage",
        "rule_type": "Type", "rule_dimension": "Measures", "engine_type": "Engine", "project": "Project",
        "policy": "If it fails", "category": "Error / warning",
        "added_by": "Added by", "active_since": "Active since", "rule_description": "Description"})

    st.caption(f"{len(view)} active checks · tick a row on the left to see all its details")
    event = st.dataframe(view, **WIDE, hide_index=True,
                         on_select="rerun", selection_mode="single-row")

    rows = getattr(getattr(event, "selection", None), "rows", []) if event is not None else []
    if rows:
        st.subheader("Details")
        st.json({k: (None if pd.isna(v) else str(v)) for k, v in df.reset_index(drop=True).iloc[rows[0]].items()})


def _pretty(name: str) -> str:
    return str(name).replace("_", " ").strip().capitalize()


def tab_assignment_add():

    # Step 1 — table
    st.subheader("1️⃣  Which table do you want to check?")
    tables = options(Q_TABLES)
    if not tables:
        st.warning("There are no registered tables yet. Register one first in **07_table**.")
        return
    table = st.selectbox("Table", tables, help="Only registered tables are listed. "
                         "Missing one? Register it first in 07_table.")

    # Step 2 — check
    st.subheader("2️⃣  What should be checked?")
    templates = query(Q_TEMPLATES)
    if templates.empty:
        st.warning("There are no checks yet. Create one first in **06_rule_template**.")
        return
    desc = dict(zip(templates["name"], templates["description"].fillna("")))
    tpl_name = st.selectbox(
        "Check", templates["name"].tolist(),
        format_func=lambda n: f"{n} — {desc.get(n, '')}" if desc.get(n) else n,
        help="Choose the rule. Missing one? Create it first in 06_rule_template.")
    tpl = templates[templates["name"] == tpl_name].iloc[0]
    if tpl.get("description"):
        st.markdown(f"> {tpl['description']}")
    with st.expander("Show how this check works (SQL)"):
        st.code(tpl["statement"], language="sql")

    # Step 3 — details generated from the check
    idents, values = placeholders(tpl["statement"])
    if tpl["engine_type"] == "dqx" and not idents:
        idents = ["column"]
    cols = table_columns(table)
    p_ident, p_value = {}, {}
    if idents or values:
        st.subheader("3️⃣  Fill in the details")
        for name in idents:
            p_ident[name] = st.selectbox(f"{_pretty(name)} to check", cols, key=f"i_{name}",
                                         help="Pick the column of the table this check should look at.")
        vc = st.columns(len(values)) if values else []
        for i, name in enumerate(values):
            p_value[name] = vc[i].text_input(_pretty(name), key=f"v_{name}",
                                             help=f"The value to use for “{_pretty(name)}”, e.g. a number.").strip()

    # Step 4 — what happens on failure
    st.subheader("4️⃣  What happens if the check fails?")
    c1, c2, c3 = st.columns(3)
    policy = c1.selectbox("If it fails", options(Q_POLICIES), format_func=friendly_policy,
                          help="Error = serious, warning = just inform. Quarantine = bad rows are moved aside.")
    apply_at = c2.selectbox("Stage", options(Q_APPLY_AT),
                            help="Data layer where the check runs. Usually leave the default.")
    projects = options(Q_PROJECTS)
    project = c3.selectbox("Project (optional)", [""] + projects, index=1 if projects else 0,
                           format_func=lambda p: str(p) if p not in ("", None) else "— none —",
                           help="The project this check belongs to. Leave as is if unsure.")

    # Step 5 — summary & save
    st.subheader("5️⃣  Check and save")
    missing = [_pretty(n) for n, v in p_value.items() if v == ""]
    what = f"**{tpl_name}**"
    if p_ident:
        what += " on " + ", ".join(f"`{v}`" for v in p_ident.values())
    if p_value:
        what += " with " + ", ".join(f"{_pretty(k)} = `{v or '?'}`" for k, v in p_value.items())
    st.markdown(f"You are adding: {what} in table `{table}`.  \nIf it fails: {friendly_policy(policy)}.")
    if missing:
        st.info(f"👉 Please fill in: **{', '.join(missing)}**.")

    existing = _find_assignment(table, tpl_name, apply_at, project, p_ident)
    if existing is not None:
        st.warning("This check already exists on this column. Saving will **update** it "
                   f"(current values: `{existing.get('parameters_values') or '—'}`). The old version is kept in the history.")

    params = {
        "target_environment": ENV,
        "table": table,
        "template": tpl_name,
        "policy": policy,
        "apply_at": apply_at,
        "parameters_identifiers": json.dumps(p_ident) if p_ident else "",
        "parameters_values": json.dumps(p_value) if p_value else "",
        "project": project,
        "added_by": current_user(),
        "action": "insert/update",
    }
    save_button("08_rule_assignment", params, "💾 Add this check", disabled=bool(missing))


def _active_assignments() -> pd.DataFrame:
    ra = query(f"SELECT * FROM {CATALOG}.configuration.rule_assignment")
    return ra[ra["is_active"] == True].copy()


def _ident_key(s) -> str:
    """Normalise a parameters_identifiers JSON string, so {"column":"a"} == { "column": "a" }."""
    try:
        return json.dumps(json.loads(s), sort_keys=True) if s not in (None, "") and not pd.isna(s) else ""
    except Exception:
        return str(s)


def _find_assignment(table, template, apply_at, project, p_ident):
    ra = _active_assignments()
    want = _ident_key(json.dumps(p_ident)) if p_ident else ""
    for _, r in ra.iterrows():
        if (r.get("table_nk") == table and r.get("template_nk") == template
                and r.get("apply_at_nk") == apply_at
                and (r.get("project_nk") or "") == (project or "")
                and _ident_key(r.get("parameters_identifiers")) == want):
            return r
    return None


def _sentence(r) -> str:
    cols = ""
    try:
        cols = ", ".join(str(v) if not isinstance(v, list) else ", ".join(v)
                         for v in json.loads(r.get("parameters_identifiers") or "{}").values())
    except Exception:
        cols = str(r.get("parameters_identifiers") or "")
    vals = ""
    if r.get("parameters_values") and not pd.isna(r.get("parameters_values")):
        try:
            vals = " (" + ", ".join(f"{k} = {v}" for k, v in json.loads(r["parameters_values"]).items()) + ")"
        except Exception:
            vals = f" ({r['parameters_values']})"
    return f"{r.get('template_nk')} on {cols or '—'}{vals} · in {r.get('table_nk')}"


def tab_assignment_switch_off():
    ra = _active_assignments()
    if ra.empty:
        st.info("There are no active checks.")
        return
    tables = sorted(ra["table_nk"].dropna().unique().tolist())
    tbl = st.selectbox("Table", tables, key="off_table", help="First pick the table, then the check.")
    sub = ra[ra["table_nk"] == tbl].reset_index(drop=True)
    labels = {i: _sentence(r) for i, r in sub.iterrows()}
    i = st.selectbox("Which check do you want to switch off?", list(labels), format_func=labels.get, key="off_check")
    r = sub.iloc[i]
    st.warning(f"This check will **stop running**: {labels[i]}. Nothing is erased — it stays in the history "
               "and can be added again later.")
    ok = st.checkbox("Yes, switch off this check", key="off_confirm")
    params = {
        "target_environment": ENV,
        "table": r.get("table_nk") or "",
        "template": r.get("template_nk") or "",
        "policy": "",
        "apply_at": r.get("apply_at_nk") or "",
        "parameters_identifiers": "" if pd.isna(r.get("parameters_identifiers")) else str(r.get("parameters_identifiers") or ""),
        "parameters_values": "" if pd.isna(r.get("parameters_values")) else str(r.get("parameters_values") or ""),
        "project": "" if pd.isna(r.get("project_nk")) else str(r.get("project_nk") or ""),
        "added_by": current_user(),
        "action": "delete",
    }
    save_button("08_rule_assignment", params, "Switch off", disabled=not ok, key="off_btn")


def page_rule_assignment():
    page_intro("08_rule_assignment", "Saved by notebook configuration/08_rule_assignment · "
                                     "list of active checks from configuration.rule_assignment")
    show_flash()
    t1, t2, t3 = st.tabs(["📋 All checks", "➕ Add a check", "🗑️ Switch off a check"])
    with t1:
        tab_assignment_list()
    with t2:
        tab_assignment_add()
    with t3:
        tab_assignment_switch_off()


# ---------------------------------------------------------------------------
# 06 Rule templates and 07 Tables: same list / history / delete as the other
# pages, but with their own Add-or-update form.
# ---------------------------------------------------------------------------
def _norm(v) -> str:
    return _as_param(v).strip()


def _enrich_templates(rows: pd.DataFrame) -> pd.DataFrame:
    """rule_template stores rule_type_id / rule_dimension_id: add their names."""
    rt = query(f"SELECT rule_type_id, rule_type FROM {CATALOG}.metadata.rule_type")
    rd = query(f"SELECT rule_dimension_id, rule_dimension FROM {CATALOG}.metadata.rule_dimension")
    rows = rows.drop(columns=[c for c in ["rule_type", "rule_dimension"] if c in rows.columns])
    rows = rows.merge(rt.drop_duplicates("rule_type_id"), on="rule_type_id", how="left")
    return rows.merge(rd.drop_duplicates("rule_dimension_id"), on="rule_dimension_id", how="left")


def _pk_text(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, str):
        return v
    return ", ".join(str(x) for x in list(v))


def _enrich_tables(rows: pd.DataFrame) -> pd.DataFrame:
    """metadata.table stores primary_key as an array: show it as 'a, b'."""
    rows = rows.copy()
    src = "primary_key" if "primary_key" in rows.columns else "primary_keys"
    rows["primary_keys"] = rows[src].apply(_pk_text) if src in rows.columns else ""
    return rows


def form_template(e, title, active, by_nk, n_ra):
    types = sorted(query(f"SELECT rule_type FROM {CATALOG}.metadata.rule_type WHERE is_active")
                   .iloc[:, 0].dropna().unique().tolist())
    dims = sorted(query(f"SELECT rule_dimension FROM {CATALOG}.metadata.rule_dimension WHERE is_active")
                  .iloc[:, 0].dropna().unique().tolist())
    mode = st.radio("What do you want to do?", ["Add new", "Edit existing"] if by_nk else ["Add new"],
                    horizontal=True, key=f"mode_{title}")
    cur = None
    if mode == "Edit existing":
        name = st.selectbox("Template", list(by_nk), key=f"pick_{title}")
        cur = by_nk[name]
    else:
        name = st.text_input("Name of the check", placeholder="e.g. check_not_negative", key=f"name_{title}",
                             help="Short name, lower case, no spaces. People will pick it from a list.").strip()

    def d(field, fallback=""):
        return fallback if cur is None or pd.isna(cur.get(field)) else cur.get(field)

    def idx(options, value):
        return options.index(value) if value in options else 0

    k = f"{title}_{mode}_{name}"
    desc = st.text_area("Description — what does it check, in plain words?", value=str(d("description")),
                        key=f"desc_{k}", placeholder="e.g. The amount must not be negative",
                        help="This is what other people will read when choosing the check.")
    c1, c2, c3 = st.columns(3)
    rule_type = c1.selectbox("Type", types, index=idx(types, d("rule_type")), key=f"rt_{k}",
                             help="technical = format/empty values · business = rules from the business.")
    rule_dim = c2.selectbox("What it measures", dims, index=idx(dims, d("rule_dimension")), key=f"rd_{k}",
                            help="completeness = nothing missing · validity = values make sense · "
                                 "uniqueness = no duplicates.")
    scopes = sorted(set(["column", "table"]) | set(active.get("scope", pd.Series()).dropna()))
    scope = c3.selectbox("Looks at", scopes, index=idx(scopes, d("scope", "column")), key=f"sc_{k}",
                         help="column = one column at a time (most common) · table = the whole table.")
    c4, c5 = st.columns(2)
    engine = c4.selectbox("Written as", ["sql", "dqx"], index=idx(["sql", "dqx"], d("engine_type", "sql")),
                          help="sql = a SQL query (use the examples) · dqx = a ready-made DQX function.",
                          key=f"en_{k}")
    reusable = c5.toggle("Reusable (placeholders are filled per assignment)",
                         value=bool(d("is_reusable", True)), key=f"ru_{k}")
    if engine == "sql":
        examples = {
            "— write my own —": "",
            "Value must not be empty": "SELECT * FROM ${table} WHERE ${i:column} IS NULL",
            "Value must not be negative": "SELECT * FROM ${table} WHERE ${i:column} < 0",
            "Value must be between a minimum and a maximum":
                "SELECT * FROM ${table} WHERE ${i:column} < ${v:min_val} OR ${i:column} > ${v:max_val}",
            "Value must be one of a list": "SELECT * FROM ${table} WHERE ${i:column} NOT IN (${v:allowed_values})",
            "Text must not be longer than N characters":
                "SELECT * FROM ${table} WHERE length(${i:column}) > ${v:max_length}",
            "Date must not be in the future": "SELECT * FROM ${table} WHERE ${i:column} > current_date()",
        }

        def _use_example():
            pick = st.session_state.get(f"ex_{k}")
            if examples.get(pick):
                st.session_state[f"st_{k}"] = examples[pick]

        if cur is None:
            st.selectbox("Start from a ready-made example (recommended)", list(examples), key=f"ex_{k}",
                         on_change=_use_example,
                         help="Pick the closest example. The SQL below is filled in for you; "
                              "you can still change it.")
        if f"st_{k}" not in st.session_state:
            st.session_state[f"st_{k}"] = str(d("statement"))
        statement = st.text_area(
            "SQL statement — it must return the rows that are WRONG", height=120,
            placeholder="SELECT * FROM ${table} WHERE ${i:column} < ${v:min_val}", key=f"st_{k}")
        idents, vals = placeholders(statement)
        st.caption("In the SQL: `${table}` = the table being checked · `${i:name}` = a column, chosen when the "
                   "check is added to a table · `${v:name}` = a value (e.g. a number), entered at the same time.")
        if statement:
            st.markdown(f"When adding this check to a table, people will choose → column(s): "
                        f"**{', '.join(idents) or '—'}** · and enter value(s): **{', '.join(vals) or '—'}**")
            if "${table}" not in statement:
                st.warning("The SQL does not use `${table}`, so it will not read the data being checked.")
    else:
        statement = st.text_input("DQX function name", value=str(d("statement")),
                                  placeholder="e.g. is_not_null", key=f"st_{k}").strip()

    values = {"name": name, "description": desc, "rule_type": rule_type, "rule_dimension": rule_dim,
              "scope": scope, "is_reusable": reusable, "engine_type": engine, "statement": statement}
    if not name or not statement:
        st.info("👉 Please fill in: **the name** and **the SQL** (or pick an example).")
        return
    if not types or not dims:
        st.error("There are no active rule types or dimensions. Create them first.")
        return
    same = False
    if name not in by_nk:
        st.success(f"🆕 This will **add** the check `{name}`.")
    else:
        changed = [f for f in ["description", "rule_type", "rule_dimension", "scope", "is_reusable",
                               "engine_type", "statement"] if _norm(by_nk[name].get(f)) != _norm(values[f])]
        if not changed:
            same = True
            st.info("Nothing to save — this is exactly the same as what already exists.")
        else:
            st.success(f"✏️ This will **update** `{name}` (changed: {', '.join(changed)}). "
                       "The old version is kept in the history.")
            if not confirm_breaking_change(int(n_ra.get(name, 0)), key=f"brk_{title}_{name}"):
                same = True
    params = {"target_environment": ENV, **{f: _as_param(v) for f, v in values.items()},
              "added_by": current_user(), "action": "insert/update"}
    save_button(e["notebook"], params, "Save", disabled=same, key=f"save_{title}")


def form_table(e, title, active, by_nk, n_ra):
    mode = st.radio("What do you want to do?",
                    ["Register a new table", "Edit a registered table"] if by_nk else ["Register a new table"],
                    horizontal=True, key=f"mode_{title}")
    cur = None
    if mode == "Edit a registered table":
        pick = st.selectbox("Table", list(by_nk), key=f"pick_{title}")
        cur = by_nk[pick]
        cat, sch, tbl = str(cur["catalog"]), str(cur["schema"]), str(cur["table"])
    else:
        st.caption("The table must already exist. Registering it does not create it and does not add checks: "
                   "after this, add rule assignments for it.")
        c1, c2, c3 = st.columns(3)
        cat = c1.text_input("Catalog", placeholder="e.g. analytics_dq_dev", key=f"cat_{title}",
                            help="First part of the full table name catalog.schema.table").strip()
        sch = c2.text_input("Schema", placeholder="e.g. metadata", key=f"sch_{title}").strip()
        tbl = c3.text_input("Table", placeholder="e.g. test_trips", key=f"tbl_{title}").strip()
    if not (cat and sch and tbl):
        st.info("👉 Please fill in: **catalog**, **schema** and **table**.")
        return
    full = f"{cat}.{sch}.{tbl}"
    try:
        cols = list(query(f"SELECT * FROM {full} LIMIT 0").columns)
    except Exception:
        cols = []
        st.warning(f"Could not read `{full}` (it does not exist, or you have no access). "
                   "The notebook does not check this — please verify the name.")

    def d(field, fallback=""):
        return fallback if cur is None or pd.isna(cur.get(field)) else cur.get(field)

    k = f"{title}_{full}"
    layers = sorted(set(["bronze", "silver", "gold"]) | set(active.get("layer", pd.Series()).dropna()))
    layer = st.selectbox("Layer", layers, index=layers.index(d("layer", "gold")) if d("layer", "gold") in layers else 0,
                         key=f"ly_{k}")
    cur_pks = [p.strip() for p in str(d("primary_keys")).split(",") if p.strip()]
    if cols:
        pks = st.multiselect("Unique ID column(s)", cols, default=[p for p in cur_pks if p in cols], key=f"pk_{k}",
                             help="The column(s) that identify each row, e.g. an order id.")
        ff_opts = [""] + cols
        ff = st.selectbox("Date column (to check only the latest data)", ff_opts,
                          help="Usually a date/time column. The checks look at the most recent data.",
                          index=ff_opts.index(d("filter_field")) if d("filter_field") in ff_opts else 0, key=f"ff_{k}")
    else:
        pks = [p.strip() for p in st.text_input("Primary key column(s), comma-separated",
                                                value=", ".join(cur_pks), key=f"pk_{k}").split(",") if p.strip()]
        ff = st.text_input("Filter field", value=str(d("filter_field")), key=f"ff_{k}").strip()
    known = sorted(active.get("filter_field_type", pd.Series()).dropna().astype(str).unique())
    fft = st.text_input("Filter field type (lookup key for filter_values at run time)",
                        value=str(d("filter_field_type")),
                        placeholder="e.g. " + (", ".join(known) if known else "timestamp, date"), key=f"ft_{k}").strip()

    values = {"catalog": cat, "schema": sch, "table": tbl, "layer": layer,
              "primary_keys": ", ".join(pks), "filter_field": ff, "filter_field_type": fft}
    same = False
    if full not in by_nk:
        st.success(f"🆕 This will **register** `{full}`. Next step: add checks to it in 08_rule_assignment → ➕ Add a check.")
    else:
        c = by_nk[full]
        versioned = [f for f in ["primary_keys", "filter_field", "filter_field_type"]
                     if _norm(c.get(f)).replace(" ", "") != _norm(values[f]).replace(" ", "")]
        layer_changed = _norm(c.get("layer")) != _norm(layer)
        if versioned:
            st.success(f"✏️ This will **update** `{full}` (changed: {', '.join(versioned)}). "
                       "The old version is kept in the history.")
            if not confirm_breaking_change(int(n_ra.get(full, 0)), key=f"brk_{title}_{full}"):
                same = True
        else:
            same = True
            st.info("Nothing to save — this is exactly the same as what already exists.")
        if layer_changed:
            st.warning("The notebook only creates a new version when primary keys, filter field or filter "
                       "field type change. A change of **layer alone is not saved**.")
    params = {"target_environment": ENV, **values, "added_by": current_user(), "action": "insert/update"}
    save_button(e["notebook"], params, "Save", disabled=same, key=f"save_{title}")


ENTITIES["Rule templates"] = dict(
    notebook="06_rule_template", table="configuration.rule_template", id="rule_template_id",
    nk="template_nk",
    key=[("name", "Template", "text", "")],
    attrs=[("description", "Description", "area", ""), ("rule_type", "Rule type", "text", ""),
           ("rule_dimension", "Dimension", "text", ""), ("scope", "Scope", "text", ""),
           ("engine_type", "Engine", "text", ""), ("is_reusable", "Reusable", "bool", ""),
           ("statement", "Statement", "area", "")],
    cascade=("assignments", "rule_template_id"),
    about="Reusable checks (SQL or DQX). Assignments apply them to a table and column.",
    enrich=_enrich_templates, form=form_template,
)
ENTITIES["Tables"] = dict(
    notebook="07_table", table="metadata.table", id="table_id", nk="table_nk",
    key=[("catalog", "Catalog", "text", ""), ("schema", "Schema", "text", ""), ("table", "Table", "text", "")],
    attrs=[("layer", "Layer", "text", ""), ("primary_keys", "Primary keys", "text", ""),
           ("filter_field", "Filter field", "text", ""), ("filter_field_type", "Filter field type", "text", "")],
    cascade=("assignments", "table_id"),
    about="Tables registered in the framework (the table itself must already exist).",
    enrich=_enrich_tables, form=form_table,
)

# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------
def page_start():
    st.title("Data quality checks")
    st.markdown("This app lets you **see and change the data quality checks** — the automatic rules that "
                "make sure our data is correct (for example: *“the amount must not be empty”*). "
                "You don't need to write code.")
    show_flash()
    st.subheader("What do you want to do?")
    tasks = [
        ("🔎 See which checks exist", "08_rule_assignment",
         "A list of every active check, with filters per table."),
        ("➕ Add a check to a table", "08_rule_assignment",
         "Pick a table, pick a check, choose the column. Tab “➕ Add a check”."),
        ("🧩 Create a new kind of check", "06_rule_template",
         "Only if none of the existing checks fits what you need."),
        ("📋 Register a new table", "07_table",
         "Needed once, before you can add checks to a table."),
    ]
    for i in range(0, len(tasks), 2):
        cols = st.columns(2)
        for j, (col, (label, page, text)) in enumerate(zip(cols, tasks[i:i + 2])):
            with col.container(border=True):
                st.markdown(f"**{label}**  \n{text}")
                st.button("Go", key=f"go_{i + j}", on_click=lambda p=page: st.session_state.update(nav=p))
    st.subheader("Good to know")
    st.markdown("- **Nothing is ever lost.** Every change keeps the old version in the history.\n"
                "- **“Delete” only switches things off.** You can add them again later.\n"
                "- Before saving, every page tells you **in plain words what will happen**.\n"
                "- Not sure? Hover over the **ⓘ** next to a field for an explanation.")
    with st.expander("📖 What do the words mean?"):
        for k, v in GLOSSARY.items():
            st.markdown(f"**{k}** — {v}")


# Menu = the configuration notebooks, same names and order (01_… → 08_…)
_by_notebook = sorted(ENTITIES, key=lambda n: ENTITIES[n]["notebook"])
PAGES = {
    "00_start_here": page_start,
    **{ENTITIES[n]["notebook"]: (lambda n=n: page_entity(n)) for n in _by_notebook},   # 01–07
    "08_rule_assignment": page_rule_assignment,                                       # 08 list / add / switch off
}
with st.sidebar:
    st.markdown("### DQ Framework")
    if st.session_state.get("nav") not in PAGES:
        st.session_state["nav"] = "00_start_here"
    choice = st.radio("Go to", list(PAGES), label_visibility="collapsed", key="nav")
    with st.expander("📖 Glossary"):
        for k, v in GLOSSARY.items():
            st.markdown(f"**{k}** — {v}")
    st.caption(f"env: {ENV} · {CATALOG}")
    st.caption(f"signed in as {current_user()}")

PAGES[choice]()
