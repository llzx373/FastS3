# FastS3 user documentation

Install, deploy, operate, and protocol reference. Design / ADRs remain in the repo at `docs/DESIGN.md`.

**English is the default.** Chinese pages are built under `/zh/`.

```bash
pip install -r docs/site/requirements.txt
mkdocs serve -f docs/site/mkdocs.yml
# http://127.0.0.1:8000        English
# http://127.0.0.1:8000/zh/    中文
```

When you host the site publicly, set `site_url` and `repo_url` in `mkdocs.yml`.

New or updated user-facing pages need both `foo.md` (English) and `foo.zh.md` (Chinese).
