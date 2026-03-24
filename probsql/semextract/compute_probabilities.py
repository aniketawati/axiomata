"""
Compute empirical conditional probabilities for the Bayesian resolver chain.

Uses WikiSQL ground truth to compute:
1. P(column_keyword | value_type) — when value is person_name, how often is col "Player" vs "Name"?
2. P(is_WHERE | column_mentioned_in_question) — when col name appears in Q, is it WHERE or SELECT?
3. P(column_keyword | trigger_phrase) — when "played for" appears, which col keyword?
4. P(is_SELECT | question_word) — when Q starts with "Who", how often is SELECT a person column?

These are computed mechanically from labeled data. No LLM needed for this step.
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

WIKISQL_DIR = Path(__file__).parent.parent.parent / "wikisql_data" / "data"
OUTPUT_DIR = Path(__file__).parent / "knowledge"

AGG_OPS = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
COND_OPS = ['=', '>', '<', '>=', '<=']


def classify_value_type(value, column_name):
    """Classify value type based on content and column context."""
    val_str = str(value).strip()
    col_lower = column_name.lower()

    # Number check
    try:
        float(val_str.replace(",", ""))
        if re.match(r'^\d{4}$', val_str):
            return "year_string"
        if re.match(r'^\d{4}[-–]\d{2,4}$', val_str):
            return "season_string"
        return "number"
    except ValueError:
        pass

    if re.match(r'^\d{4}$', val_str):
        return "year_string"
    if re.match(r'^\d{4}[-–]\d{2,4}$', val_str):
        return "season_string"

    name_words = {"player", "name", "winner", "candidate", "person", "commander",
                  "director", "artist", "author", "manager", "coach", "captain",
                  "rider", "driver", "incumbent", "representative", "minister"}
    if any(w in col_lower for w in name_words):
        return "person_name"

    loc_words = {"country", "city", "location", "venue", "capital", "state",
                 "headquarters", "base", "region", "district", "county", "ground"}
    if any(w in col_lower for w in loc_words):
        return "location"

    inst_words = {"school", "club", "team", "university", "college", "company",
                  "party", "network", "organization", "carrier", "airline"}
    if any(w in col_lower for w in inst_words):
        return "institution"

    cat_words = {"position", "type", "category", "genre", "status", "result",
                 "class", "division", "conference", "league", "branch", "rating"}
    if any(w in col_lower for w in cat_words):
        return "category"

    if re.match(r'\w+ \d{1,2},? \d{4}', val_str):
        return "date_string"

    return "string"


def extract_column_keywords(col_name):
    """Extract meaningful keywords from a column name."""
    words = set(re.findall(r'\b\w+\b', col_name.lower()))
    stop = {"the", "a", "an", "of", "in", "for", "and", "or", "by", "to", "at", "on"}
    return words - stop


def compute_all_probabilities(split="train"):
    """Compute all conditional probability tables from WikiSQL."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load data
    tables = {}
    with open(WIKISQL_DIR / f"{split}.tables.jsonl") as f:
        for line in f:
            t = json.loads(line)
            tables[t["id"]] = t

    examples = []
    with open(WIKISQL_DIR / f"{split}.jsonl") as f:
        for line in f:
            examples.append(json.loads(line))

    print(f"Computing from {len(examples)} {split} examples...")

    # Accumulators
    # 1. P(column_keyword | value_type)
    vtype_colkw = defaultdict(Counter)  # vtype → {col_keyword: count}
    vtype_total = Counter()

    # 2. P(is_WHERE | column_mentioned_in_question)
    mention_where = 0  # col mentioned in Q AND is WHERE col
    mention_select = 0  # col mentioned in Q AND is SELECT col
    mention_neither = 0  # col mentioned in Q AND is neither

    # 3. P(column_keyword | trigger_phrase)
    trigger_colkw = defaultdict(Counter)
    trigger_total = Counter()
    triggers_to_check = [
        (r'\bplayed?\s+for\b', "played for"),
        (r'\bplays?\s+for\b', "plays for"),
        (r'\bfrom\s+(?=[A-Z])', "from [Entity]"),
        (r'\bat\s+(?=[A-Z])', "at [Entity]"),
        (r'\bin\s+\d{4}\b', "in [Year]"),
        (r'\bin\s+(?=[A-Z])', "in [Entity]"),
        (r'\bagainst\s+', "against"),
        (r'\bwears?\s+(?:number|no)', "wears number"),
        (r'\bnumber\s+\d', "number [N]"),
        (r'\bdirected\s+by\b', "directed by"),
        (r'\bwritten\s+by\b', "written by"),
        (r'\bwon\s+(?:in|at|by)\b', "won in/at/by"),
        (r'\belected\s+in\b', "elected in"),
        (r'\bborn\s+in\b', "born in"),
        (r'\baired\b', "aired"),
        (r'\bscored?\b', "scored"),
        (r'\brepresent', "represent"),
        (r'\blocated\s+in\b', "located in"),
    ]

    # 4. P(is_SELECT | question_word_pattern)
    qword_select_colkw = defaultdict(Counter)
    qword_total = Counter()

    # 5. P(operator | value_type)
    vtype_op = defaultdict(Counter)

    for ex in examples:
        table = tables.get(ex["table_id"])
        if not table:
            continue
        headers = table["header"]
        q = ex["question"]
        q_lower = q.lower()
        sql = ex["sql"]
        conds = sql["conds"]
        sel_idx = sql["sel"]
        sel_col = headers[sel_idx]

        # Prob 4: question word → SELECT column keywords
        first_words = q_lower.split()[:3]
        qword = first_words[0] if first_words else ""
        if qword == "how" and len(first_words) > 1:
            qword = f"how {first_words[1]}"
        sel_kws = extract_column_keywords(sel_col)
        for kw in sel_kws:
            qword_select_colkw[qword][kw] += 1
        qword_total[qword] += 1

        for col_idx, op_idx, value in conds:
            where_col = headers[col_idx]
            where_op = COND_OPS[op_idx]
            vtype = classify_value_type(value, where_col)
            where_kws = extract_column_keywords(where_col)

            # Prob 1: P(column_keyword | value_type)
            for kw in where_kws:
                vtype_colkw[vtype][kw] += 1
            vtype_total[vtype] += 1

            # Prob 5: P(operator | value_type)
            vtype_op[vtype][where_op] += 1

            # Prob 2: P(is_WHERE | mentioned)
            for i, h in enumerate(headers):
                if h.lower() in q_lower or any(
                    w in q_lower for w in re.findall(r'\b\w{4,}\b', h.lower())
                ):
                    if i == col_idx:
                        mention_where += 1
                    elif i == sel_idx:
                        mention_select += 1
                    else:
                        mention_neither += 1

            # Prob 3: P(column_keyword | trigger_phrase)
            for pattern, trigger_name in triggers_to_check:
                if re.search(pattern, q_lower):
                    for kw in where_kws:
                        trigger_colkw[trigger_name][kw] += 1
                    trigger_total[trigger_name] += 1

    # Compute normalized probabilities
    prob_tables = {}

    # Prob 1: P(col_keyword | value_type) — top 15 keywords per type
    p1 = {}
    for vtype, kw_counts in vtype_colkw.items():
        total = sum(kw_counts.values())
        p1[vtype] = {
            "distribution": {kw: round(c / total, 4) for kw, c in kw_counts.most_common(15)},
            "total_examples": vtype_total[vtype],
        }
    prob_tables["P_colkw_given_valuetype"] = p1

    # Prob 2: P(is_WHERE | mentioned)
    total_mentions = mention_where + mention_select + mention_neither
    prob_tables["P_role_given_mentioned"] = {
        "P_WHERE": round(mention_where / max(total_mentions, 1), 4),
        "P_SELECT": round(mention_select / max(total_mentions, 1), 4),
        "P_NEITHER": round(mention_neither / max(total_mentions, 1), 4),
        "total": total_mentions,
    }

    # Prob 3: P(col_keyword | trigger_phrase) — top 10 per trigger
    p3 = {}
    for trigger, kw_counts in trigger_colkw.items():
        total = sum(kw_counts.values())
        if total >= 5:
            p3[trigger] = {
                "distribution": {kw: round(c / total, 4) for kw, c in kw_counts.most_common(10)},
                "total_examples": trigger_total[trigger],
            }
    prob_tables["P_colkw_given_trigger"] = p3

    # Prob 4: P(select_col_keyword | question_word) — top 10 per qword
    p4 = {}
    for qword, kw_counts in qword_select_colkw.items():
        total = sum(kw_counts.values())
        if total >= 10:
            p4[qword] = {
                "distribution": {kw: round(c / total, 4) for kw, c in kw_counts.most_common(10)},
                "total_examples": qword_total[qword],
            }
    prob_tables["P_select_colkw_given_qword"] = p4

    # Prob 5: P(operator | value_type)
    p5 = {}
    for vtype, op_counts in vtype_op.items():
        total = sum(op_counts.values())
        p5[vtype] = {op: round(c / total, 4) for op, c in op_counts.most_common()}
    prob_tables["P_operator_given_valuetype"] = p5

    # Save
    with open(OUTPUT_DIR / "bayesian_tables.json", "w") as f:
        json.dump(prob_tables, f, indent=2)

    # Print summary
    print(f"\n{'='*60}")
    print("EMPIRICAL CONDITIONAL PROBABILITIES")
    print(f"{'='*60}")

    print(f"\n1. P(col_keyword | value_type):")
    for vtype in ["person_name", "institution", "location", "category", "number", "year_string"]:
        if vtype in p1:
            top3 = list(p1[vtype]["distribution"].items())[:3]
            n = p1[vtype]["total_examples"]
            print(f"   {vtype} (n={n}): {', '.join(f'{k}={v:.0%}' for k,v in top3)}")

    print(f"\n2. P(role | column_mentioned_in_question):")
    p2 = prob_tables["P_role_given_mentioned"]
    print(f"   WHERE: {p2['P_WHERE']:.0%}, SELECT: {p2['P_SELECT']:.0%}, NEITHER: {p2['P_NEITHER']:.0%}")

    print(f"\n3. P(col_keyword | trigger_phrase):")
    for trigger in ["played for", "from [Entity]", "wears number", "in [Year]", "against"]:
        if trigger in p3:
            top3 = list(p3[trigger]["distribution"].items())[:3]
            n = p3[trigger]["total_examples"]
            print(f"   '{trigger}' (n={n}): {', '.join(f'{k}={v:.0%}' for k,v in top3)}")

    print(f"\n4. P(select_col_keyword | question_word):")
    for qword in ["who", "what", "where", "when", "how many", "which"]:
        if qword in p4:
            top3 = list(p4[qword]["distribution"].items())[:3]
            n = p4[qword]["total_examples"]
            print(f"   '{qword}' (n={n}): {', '.join(f'{k}={v:.0%}' for k,v in top3)}")

    print(f"\n5. P(operator | value_type):")
    for vtype in ["person_name", "number", "year_string", "category"]:
        if vtype in p5:
            print(f"   {vtype}: {', '.join(f'{k}={v:.0%}' for k,v in p5[vtype].items())}")

    return prob_tables


if __name__ == "__main__":
    compute_all_probabilities("train")
