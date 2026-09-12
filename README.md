# WiiLink Checkout

## Translation workflow

Translations use Flask-Babel and catalogs under `translations/`.

Run these commands from the repository root with the virtual environment activated:

```bash
# Extract translatable strings from Python and Jinja templates.
.venv/bin/python -m babel.messages.frontend extract -F babel.cfg -o messages.pot .

# Merge new strings into the existing catalogs without deleting translations.
.venv/bin/pybabel update -i messages.pot -d translations

# Update one locale only.
.venv/bin/pybabel update -i messages.pot -d translations -l es

# Initialize a new locale (only when its PO file does not exist yet).
.venv/bin/pybabel init -i messages.pot -d translations -l fr

# Compile PO catalogs into MO files used by the application.
.venv/bin/pybabel compile -d translations

# Compile one locale only.
.venv/bin/pybabel compile -d translations -l es
```

`pybabel update` preserves existing `msgstr` values by matching messages by
`msgid`. Review new or fuzzy entries in the relevant `.po` file, then run the
compile command before testing the application.

### Adding a locale

Use Babel's normalized locale code, such as `pt_PT` for European Portuguese
(`pt-pt` is not the catalog directory name used here).

1. Add the locale to `BABEL_SUPPORTED_LOCALES` in `app.py`:

```python
app.config["BABEL_SUPPORTED_LOCALES"] = ["en", "es", "pt_PT"]
```

2. Create its catalog:

```bash
.venv/bin/pybabel init -i messages.pot -d translations -l pt_PT
```

3. Translate the `msgstr` values in `translations/pt_PT/LC_MESSAGES/messages.po`.

4. Compile the catalog:

```bash
.venv/bin/pybabel compile -d translations -l pt_PT
```

5. Test it with `?lang=pt_PT`, or set the Authentik user's
	`attributes.settings.locale` value to `pt_PT`. Future catalog updates use
	the normal `pybabel update` command and preserve completed translations.

The full update cycle is:

```bash
.venv/bin/python -m babel.messages.frontend extract -F babel.cfg -o messages.pot .
.venv/bin/pybabel update -i messages.pot -d translations
.venv/bin/pybabel compile -d translations
```

Use the project virtual environment for these commands. If invoking the
`pybabel` executable directly cannot import the local `utils` package, run it
with `PYTHONPATH=.` or use the module command shown above.
