from pathlib import Path

import pytest
import yaml

from ecommerce_research.config import config_hash, load_config, run_directory
from ecommerce_research.demo import generate_demo
from ecommerce_research.mining import pattern_matches
from ecommerce_research.quality import inspect_sources


def test_disabled_platform_is_not_silently_taobao():
    with pytest.raises(ValueError, match="尚未接入"):
        load_config(Path(__file__).resolve().parents[1] / "configs" / "pdd.template.yaml")


def test_run_id_cannot_escape_artifact_directory(cfg):
    with pytest.raises(ValueError, match="run-id"):
        run_directory(cfg, "../../external")


def test_time_split_rejects_future_training_labels(tmp_path, cfg):
    clean = {k: v for k, v in cfg.items() if not k.startswith("_")}
    clean["task"]["validation_dates"] = ["2017-11-29"]
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(clean), encoding="utf-8")
    with pytest.raises(ValueError, match="锚点不能重复|时间泄漏"):
        load_config(p)


@pytest.mark.parametrize("sequence,pattern,expected", [
    ([["browse", "cart_add"]], [["browse"], ["cart_add"]], False),
    ([["browse", "cart_add"]], [["browse", "cart_add"]], True),
    ([["browse"], ["favorite"], ["cart_add"]], [["browse"], ["cart_add"]], True),
    ([["cart_add"], ["browse"]], [["browse"], ["cart_add"]], False),
])
def test_sequence_respects_simultaneous_itemsets(sequence, pattern, expected):
    assert pattern_matches(sequence, pattern) is expected


def test_source_sidecar_prevents_demo_as_real(tmp_path, cfg):
    data = tmp_path / "UserBehavior.csv"
    generate_demo(data, users=30)
    cfg["data"]["input_glob"] = str(data)
    cfg["data"]["kind"] = "public"
    with pytest.raises(ValueError, match="演示文件不能声明"):
        inspect_sources(cfg, tmp_path / "run")


def test_duplicate_source_file_is_rejected(tmp_path, cfg):
    for filename in ("one.csv", "two.csv"):
        (tmp_path / filename).write_text("u,i,c,pv,1511539200\n", encoding="utf-8")
    cfg["data"]["input_glob"] = str(tmp_path / "*.csv")
    with pytest.raises(ValueError, match="完全相同"):
        inspect_sources(cfg, tmp_path / "run")


def test_demo_generator_preserves_existing_file(tmp_path):
    output = tmp_path / "demo.csv"
    output.write_text("user content")
    with pytest.raises(FileExistsError):
        generate_demo(output, users=30)
    assert output.read_text() == "user content"


def test_config_fingerprint_ignores_runtime_paths(cfg):
    before = config_hash(cfg)
    cfg["_config_file"] = "/another/location"
    assert config_hash(cfg) == before
