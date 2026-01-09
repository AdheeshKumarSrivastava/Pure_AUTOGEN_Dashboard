import re

FORBIDDEN = [r"\bINSERT\b",r"\bUPDATE\b",r"\bDELETE\b",r"\bMERGE\b",r"\bDROP\b",r"\bALTER\b",
             r"\bTRUNCATE\b",r"\bCREATE\b",r"\bGRANT\b",r"\bREVOKE\b",r"\bEXEC\b",r"\bEXECUTE\b"]

def is_select_only(sql:str)->bool:
    s = sql.strip().strip(";")
    if not re.match(r"^(WITH\s+[\s\S]+?\)\s*)?SELECT\b",s,flags=re.IGNORECASE):
        return False
    for pat in FORBIDDEN:
        if re.search(pat,s,flags=re.IGNORECASE):
            return False
    return True

def enforce_select_only(sql:str)->None:
    if not is_select_only(sql):
        raise ValueError("BLOCKED non-SELECT SQL. Only read-only SELECT queries are allowed.")