# Fetch a user's submitted miis and submitted miis to a given contest
import config
from utils import _run_query


def get_artisan_id_from_wii_number(wii_number, db_url=None):
    """Get artisan ID from a Wii number in the cmoc database."""
    if db_url is None:
        db_url = getattr(config, 'cmoc_db_url', None)
    if not db_url or not wii_number:
        return None
    
    query = "SELECT artisan_id FROM artisans WHERE wii_number = %s LIMIT 1"
    result = _run_query(query, [wii_number], db_url)
    
    if result:
        return result[0].get('artisan_id')
    return None