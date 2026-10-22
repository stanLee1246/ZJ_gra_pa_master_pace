from pathlib import Path

from streamlit.testing.v1 import AppTest


def test_unavailable_platform_does_not_fabricate_results(tmp_path, monkeypatch):
    monkeypatch.setenv("ECOMMERCE_ARTIFACTS", str(tmp_path))
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "app" / "main.py"))
    app.run(timeout=30)
    assert not app.exception
    assert "尚未接入" in app.info[0].value
    app.sidebar.selectbox[0].select("拼多多").run(timeout=30)
    assert not app.exception
    assert "拼多多" in app.info[0].value
    assert not app.metric
