import config
from utils.utils import _build_serial_filter, _run_query
from datetime import datetime


def format_ans_cnt(ans_cnt):
    """Format ans_cnt into a 4-element array of digit counts."""
    ans_cnt_str = str(ans_cnt) if ans_cnt else "0"

    if len(ans_cnt_str) > 4:
        ans_cnt_str = ans_cnt_str[:4]

    uint_array = [0, 0, 0, 0]

    for i, digit in enumerate(ans_cnt_str):
        uint_array[4 - len(ans_cnt_str) + i] = int(digit)

    return uint_array


# Count functions for pagination


def count_user_polls(wii_numbers, db_url=None):
    """Count total polls for given Wii numbers."""
    if db_url is None:
        db_url = getattr(config, "evc_db_url", None)
    if not db_url or not wii_numbers:
        return 0

    placeholders = ",".join(["%s"] * len(wii_numbers))
    query = f"SELECT COUNT(DISTINCT question_id) AS count FROM votes WHERE wii_no IN ({placeholders}) AND type_cd = 0"
    result = _run_query(query, wii_numbers, db_url)
    return result[0].get("count", 0) if result else 0


def count_user_suggestions(wii_numbers, db_url=None):
    """Count total suggestions for given Wii numbers."""
    if db_url is None:
        db_url = getattr(config, "evc_db_url", None)
    if not db_url or not wii_numbers:
        return 0

    placeholders = ",".join(["%s"] * len(wii_numbers))
    query = (
        f"SELECT COUNT(*) AS count FROM suggestions WHERE wii_no IN ({placeholders})"
    )
    result = _run_query(query, wii_numbers, db_url)
    return result[0].get("count", 0) if result else 0


def fetch_user_polls(wii_numbers, limit=30, offset=0, db_url=None):
    """Fetch user's poll votes with question details from evc database."""
    if db_url is None:
        db_url = getattr(config, "evc_db_url", None)
    if not db_url:
        return []

    if not wii_numbers:
        return []

    # Build WHERE clause for Wii numbers
    placeholders = ",".join(["%s"] * len(wii_numbers))
    where_clause = f"v.wii_no IN ({placeholders}) AND v.type_cd = 0"

    query = (
        "SELECT "
        "v.id, v.wii_no, v.question_id, v.type_cd, v.ans_cnt, "
        "q.question_id, q.content_english, q.choice1_english, q.choice2_english, "
        "q.type, q.category, q.date "
        "FROM votes v "
        "LEFT JOIN questions q ON v.question_id = q.question_id "
        f"WHERE {where_clause} "
        "ORDER BY q.date DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    polls = _run_query(query, wii_numbers, db_url)

    # Enrich polls with computed fields
    for poll in polls:
        # FIrst, we join the polls with type_cd 0 and 1 that have the same question_id, and then we format the ans_cnt for each type_cd
        question_id = poll.get("question_id")
        if question_id:
            # Fetch votes and predictions for the same question_id
            votes_query = (
                "SELECT ans_cnt FROM votes "
                "WHERE question_id = %s AND type_cd = 0 AND wii_no IN ("
                + ",".join(["%s"] * len(wii_numbers))
                + ")"
            )
            predictions_query = (
                "SELECT ans_cnt FROM votes "
                "WHERE question_id = %s AND type_cd = 1 AND wii_no IN ("
                + ",".join(["%s"] * len(wii_numbers))
                + ")"
            )
            votes_rows = _run_query(votes_query, [question_id] + wii_numbers, db_url)
            predictions_rows = _run_query(
                predictions_query, [question_id] + wii_numbers, db_url
            )

            # Format ans_cnt for votes and predictions
            poll["votes"] = [format_ans_cnt(v.get("ans_cnt", "0")) for v in votes_rows]
            poll["predictions"] = [
                format_ans_cnt(p.get("ans_cnt", "0")) for p in predictions_rows
            ]

            formatted_votes = []
            for vote_array in poll["votes"]:
                for _ in range(vote_array[0]):
                    formatted_votes.append({"gender": "male", "choice": 1})
                for _ in range(vote_array[1]):
                    formatted_votes.append({"gender": "female", "choice": 1})
                for _ in range(vote_array[2]):
                    formatted_votes.append({"gender": "male", "choice": 2})
                for _ in range(vote_array[3]):
                    formatted_votes.append({"gender": "female", "choice": 2})
            poll["votes"] = formatted_votes

            formatted_predictions = []
            for prediction_array in poll["predictions"]:
                for _ in range(prediction_array[0]):
                    formatted_predictions.append({"gender": "male", "choice": 1})
                for _ in range(prediction_array[1]):
                    formatted_predictions.append({"gender": "female", "choice": 1})
                for _ in range(prediction_array[2]):
                    formatted_predictions.append({"gender": "male", "choice": 2})
                for _ in range(prediction_array[3]):
                    formatted_predictions.append({"gender": "female", "choice": 2})
            poll["predictions"] = formatted_predictions
    return polls


def fetch_user_suggestions(wii_numbers, limit=30, offset=0, db_url=None):
    """Fetch user's suggestions from evc database."""
    if db_url is None:
        db_url = getattr(config, "evc_db_url", None)
    if not db_url:
        return []

    if not wii_numbers:
        return []

    # Build WHERE clause for Wii numbers
    placeholders = ",".join(["%s"] * len(wii_numbers))
    where_clause = f"wii_no IN ({placeholders})"

    query = (
        "SELECT "
        "id, country_code, region_code, language_code, content, "
        "choice1, choice2, wii_no "
        "FROM suggestions "
        f"WHERE {where_clause} "
        "ORDER BY id DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    suggestions = _run_query(query, wii_numbers, db_url)
    return suggestions
