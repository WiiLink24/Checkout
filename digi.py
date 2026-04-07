import configparser
import io
import config
import base64
from digicam.render import render
from utils import _run_query


def fetch_orders_by_email(email, db_url=None):
    """Fetch all business cards for a given email from cam_server database"""
    if db_url is None:
        db_url = getattr(config, "cam_server_db_url", None)
    if not db_url or not email:
        return []

    query = """
        SELECT order_id, date_created, is_business_card, email, order_schema
        FROM orders
        WHERE email = %s AND is_business_card = true
        ORDER BY date_created DESC
    """
    result = _run_query(query, [email], db_url)
    return result if result else []


def render_card_to_image(order_data):
    try:
        order_id = order_data.get("order_id")
        order_schema = order_data.get("order_schema", "")

        if not order_id or not order_schema:
            return None

        result = render(order_schema, order_id)

        if not result or "pages" not in result or not result["pages"]:
            return None

        image_bytes = result["pages"][0]

        image_base64 = base64.b64encode(image_bytes).decode("utf-8")

        return image_base64
    except Exception as e:
        print(f"Error rendering card: {e}")
        return None


def get_card_name(order_schema):
    try:
        s_config = order_schema.encode().decode("utf-8-sig")
        buf = io.StringIO(s_config)
        config_obj = configparser.ConfigParser()
        config_obj.read_file(buf)

        # Strip quotes from all values
        for section in config_obj.sections():
            for key, value in config_obj.items(section):
                config_obj[section][key] = value.strip('"')

        # Extract only the name from card schema
        card_info = {
            "name": "",
        }

        # Parse page objects
        page_info_section = "Page01Info"
        if page_info_section in config_obj:
            page_objects_count = int(
                config_obj[page_info_section].get("PageObjectsCount", 0)
            )

            for obj_num in range(1, page_objects_count + 1):
                obj_section = f"Page01Object{obj_num:02d}"
                if obj_section not in config_obj:
                    continue

                obj_config = config_obj[obj_section]
                obj_type = obj_config.get("ObjectType", "")

                # Object type 2 is text
                if obj_type == "2":
                    text = obj_config.get("Text", "").strip()
                    if obj_num == 3 and text:
                        card_info["name"] = text
                        break

        return card_info

    except Exception as e:
        print(f"Error parsing card schema: {e}")
        return {"name": ""}
