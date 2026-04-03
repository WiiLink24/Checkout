import config
import math
import random
from collections import defaultdict
from utils import _build_serial_filter, _run_query


def _split_genres(genre_value):
    """Split a comma-separated genre string into clean tokens."""
    if not genre_value:
        return []
    return [g.strip() for g in genre_value.split(",") if g.strip()]


def _normalize_scores(score_map):
    """Normalize scores to 0..1 while preserving relative ordering."""
    if not score_map:
        return {}
    max_value = max(score_map.values()) or 1.0
    return {k: (v / max_value) for k, v in score_map.items()}


def _build_discover_profile(where_clause, params):
    """Build user taste profile from recommendation history."""
    query = (
        "SELECT "
        "r.game_id, r.recommendation_percent, "
        "t.genre, t.developer, t.publisher, t.game_type "
        "FROM recommendations r "
        "LEFT JOIN LATERAL ("
        "    SELECT * FROM titles t "
        "    WHERE t.game_id LIKE r.game_id || '%%' "
        "    ORDER BY t.game_id "
        "    LIMIT 1"
        ") t ON true "
        f"WHERE {where_clause}"
    )
    rows = _run_query(query, params, config.db_url)
    if not rows:
        return None

    genre_scores = defaultdict(float)
    developer_scores = defaultdict(float)
    publisher_scores = defaultdict(float)
    game_type_scores = defaultdict(float)
    reviewed_ids = set()

    for row in rows:
        reviewed_ids.add(row.get("game_id"))

        recommendation_percent = float(row.get("recommendation_percent") or 0)
        # Map recommendation score into a positive preference weight (0..1).
        preference_weight = max(0.0, min(1.0, (recommendation_percent - 40.0) / 60.0))
        if preference_weight <= 0:
            continue

        for genre in _split_genres(row.get("genre"))[:3]:
            genre_scores[genre] += preference_weight

        developer = (row.get("developer") or "").strip()
        if developer:
            developer_scores[developer] += preference_weight * 0.65

        publisher = (row.get("publisher") or "").strip()
        if publisher:
            publisher_scores[publisher] += preference_weight * 0.45

        game_type = (row.get("game_type") or "").strip()
        if game_type:
            game_type_scores[game_type] += preference_weight * 0.35

    top_genres = [
        g
        for g, _ in sorted(
            genre_scores.items(), key=lambda item: item[1], reverse=True
        )[:5]
    ]
    return {
        "reviewed_ids": reviewed_ids,
        "genre_scores": _normalize_scores(genre_scores),
        "developer_scores": _normalize_scores(developer_scores),
        "publisher_scores": _normalize_scores(publisher_scores),
        "game_type_scores": _normalize_scores(game_type_scores),
        "top_genres": top_genres,
        "total_rated": len(reviewed_ids),
    }


def _is_already_seen(candidate_game_id, seen_game_ids):
    """Exclude candidates already seen by the user (played or reviewed)."""
    if candidate_game_id in seen_game_ids:
        return True
    for seen_id in seen_game_ids:
        if not seen_id:
            continue
        if candidate_game_id.startswith(seen_id) or seen_id.startswith(
            candidate_game_id
        ):
            return True
    return False


def _score_discover_candidate(candidate, profile):
    """Score a candidate game using weighted user affinity and community signal."""
    genres = _split_genres(candidate.get("genre"))
    genre_scores = profile["genre_scores"]

    if genres:
        genre_match_best = max((genre_scores.get(g, 0.0) for g in genres), default=0.0)
        genre_match_sum = sum((genre_scores.get(g, 0.0) for g in genres))
        genre_score = genre_match_best + (0.30 * genre_match_sum)
    else:
        genre_match_best = 0.0
        genre_score = 0.0

    developer_score = profile["developer_scores"].get(
        (candidate.get("developer") or "").strip(), 0.0
    )
    publisher_score = profile["publisher_scores"].get(
        (candidate.get("publisher") or "").strip(), 0.0
    )
    game_type_score = profile["game_type_scores"].get(
        (candidate.get("game_type") or "").strip(), 0.0
    )

    avg_recommendation = float(candidate.get("avg_recommendation") or 50.0)
    rating_count = int(candidate.get("rating_count") or 0)
    confidence = (
        min(1.0, math.log1p(rating_count) / math.log(25)) if rating_count > 0 else 0.0
    )
    community_score = ((avg_recommendation - 50.0) / 50.0) * confidence

    # Small jitter keeps discover results from feeling static while preserving quality.
    exploration_jitter = random.uniform(0.0, 0.03)

    total_score = (
        (0.55 * genre_score)
        + (0.18 * developer_score)
        + (0.12 * publisher_score)
        + (0.05 * game_type_score)
        + (0.10 * community_score)
        + exploration_jitter
    )

    matched_genre = "Unknown"
    if genres:
        matched_genre = max(genres, key=lambda g: genre_scores.get(g, 0.0))

    return total_score, matched_genre


def find_game_recommendation(serial_prefixes):
    """Find a discover recommendation using weighted taste matching."""
    if not serial_prefixes:
        return None

    where_clause, params = _build_serial_filter("serial_number", serial_prefixes)

    profile = _build_discover_profile(where_clause, params)
    if not profile:
        return None

    played_rows = _run_query(
        f"SELECT DISTINCT game_id FROM time_played WHERE {where_clause}",
        params,
        config.db_url
    )
    played_ids = {row.get("game_id") for row in played_rows if row.get("game_id")}
    seen_game_ids = set(profile["reviewed_ids"]) | played_ids

    candidates = _run_query(
        (
            "SELECT "
            "t.game_id, t.display_name, t.title_en, t.synopsis_en, t.genre, t.developer, t.publisher, "
            "t.game_type, t.release_year, t.rating_type, t.rating_value, t.region, "
            "COALESCE(stats.avg_recommendation, 50) AS avg_recommendation, "
            "COALESCE(stats.rating_count, 0) AS rating_count "
            "FROM titles t "
            "LEFT JOIN LATERAL ("
            "    SELECT AVG(r.recommendation_percent) AS avg_recommendation, COUNT(*) AS rating_count "
            "    FROM recommendations r "
            "    WHERE t.game_id LIKE r.game_id || '%%'"
            ") stats ON true "
            "WHERE t.display_name IS NOT NULL "
            "AND t.genre IS NOT NULL "
            "ORDER BY COALESCE(stats.rating_count, 0) DESC, COALESCE(stats.avg_recommendation, 50) DESC "
            "LIMIT 1200"
        ),
        [],
        config.db_url
    )

    scored_candidates = []
    for candidate in candidates:
        candidate_game_id = candidate.get("game_id")
        if not candidate_game_id or _is_already_seen(candidate_game_id, seen_game_ids):
            continue

        score, matched_genre = _score_discover_candidate(candidate, profile)
        scored_candidates.append((score, matched_genre, candidate))

    if not scored_candidates:
        return None

    scored_candidates.sort(key=lambda item: item[0], reverse=True)
    best_score, matched_genre, best_game = scored_candidates[0]

    best_game["reason"] = {
        "genres": (
            ", ".join(profile["top_genres"][:2])
            if profile["top_genres"]
            else "your recent likes"
        ),
        "matched_genre": matched_genre,
        "total_rated": profile["total_rated"],
        "score": round(best_score, 3),
    }
    return best_game
