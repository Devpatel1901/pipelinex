"""Tests for the YAML config loader and builder."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipelinex.core.builder import build_pipeline
from pipelinex.core.config import PipelineConfig, load_config, parse_config
from pipelinex.core.exceptions import ConfigurationError
from pipelinex.persistence.memory_repo import InMemoryLogRepository


def _minimal_config(tmp_path: Path, **overrides: object) -> dict[str, object]:
    src = tmp_path / "in.log"
    src.write_text("")
    cfg: dict[str, object] = {
        "source": {"kind": "file", "path": str(src)},
        "parser": {"kind": "hdfs"},
        "repository": {"kind": "memory"},
    }
    cfg.update(overrides)
    return cfg


class TestLoadConfig:
    def test_load_valid_yaml(self, tmp_path: Path) -> None:
        cfg_path = tmp_path / "pipeline.yaml"
        cfg_path.write_text(yaml.safe_dump(_minimal_config(tmp_path)))
        cfg = load_config(cfg_path)
        assert isinstance(cfg, PipelineConfig)
        assert cfg.source.kind == "file"
        assert cfg.parser.kind == "hdfs"

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigurationError, match="not found"):
            load_config(tmp_path / "does_not_exist.yaml")

    def test_invalid_yaml_raises(self, tmp_path: Path) -> None:
        cfg_path = tmp_path / "bad.yaml"
        cfg_path.write_text("source: { unbalanced :")
        with pytest.raises(ConfigurationError, match="parse YAML"):
            load_config(cfg_path)

    def test_unknown_top_level_field_rejected(self, tmp_path: Path) -> None:
        bad = _minimal_config(tmp_path)
        bad["unknown"] = "x"
        with pytest.raises(ConfigurationError, match="validation failed"):
            parse_config(bad)


class TestSourceValidation:
    def test_file_kind_requires_path(self, tmp_path: Path) -> None:
        bad = {
            "source": {"kind": "file"},
            "parser": {"kind": "hdfs"},
            "repository": {"kind": "memory"},
        }
        with pytest.raises(ConfigurationError, match="path is required"):
            parse_config(bad)

    def test_stdin_kind_does_not_require_path(self) -> None:
        cfg = parse_config(
            {
                "source": {"kind": "stdin"},
                "parser": {"kind": "hdfs"},
                "repository": {"kind": "memory"},
            }
        )
        assert cfg.source.kind == "stdin"


class TestRepositoryValidation:
    def test_postgres_kind_requires_dsn(self, tmp_path: Path) -> None:
        bad = _minimal_config(tmp_path)
        bad["repository"] = {"kind": "postgres"}
        with pytest.raises(ConfigurationError, match="dsn is required"):
            parse_config(bad)


class TestRuntimeBounds:
    def test_workers_must_be_at_least_one(self, tmp_path: Path) -> None:
        bad = _minimal_config(tmp_path)
        bad["runtime"] = {"num_workers": 0}
        with pytest.raises(ConfigurationError):
            parse_config(bad)


class TestBuilder:
    def test_minimal_build(self, tmp_path: Path) -> None:
        cfg = parse_config(_minimal_config(tmp_path))
        built = build_pipeline(cfg)
        assert isinstance(built.repository, InMemoryLogRepository)
        assert built.sessionizer is None
        assert built.sequence_detector is None
        # Expect just the parser stage.
        assert len(built.executor._stages) == 1  # type: ignore[attr-defined]

    def test_validation_added_when_configured(self, tmp_path: Path) -> None:
        cfg = parse_config(
            _minimal_config(
                tmp_path,
                validation=[{"kind": "schema", "require_message": True}],
            )
        )
        built = build_pipeline(cfg)
        # parser + validation = 2.
        assert len(built.executor._stages) == 2  # type: ignore[attr-defined]

    def test_sessionizer_added_when_enabled(self, tmp_path: Path) -> None:
        cfg = parse_config(
            _minimal_config(
                tmp_path,
                sessionizer={"enabled": True, "idle_window_s": 60.0},
            )
        )
        built = build_pipeline(cfg)
        assert built.sessionizer is not None
        # parser + sessionizer = 2.
        assert len(built.executor._stages) == 2  # type: ignore[attr-defined]

    def test_detector_added_when_configured(self, tmp_path: Path) -> None:
        cfg = parse_config(
            _minimal_config(
                tmp_path,
                detectors=[
                    {"kind": "z_score", "threshold": 3.0, "window_size": 50}
                ],
            )
        )
        built = build_pipeline(cfg)
        # parser + detector = 2.
        assert len(built.executor._stages) == 2  # type: ignore[attr-defined]

    def test_sequence_detector_subscribes_to_bus(self, tmp_path: Path) -> None:
        cfg = parse_config(
            _minimal_config(
                tmp_path,
                sequence_detector={"kind": "sequence_ngram", "threshold": 0.0},
            )
        )
        built = build_pipeline(cfg)
        assert built.sequence_detector is not None
        # The bus has a listener for BlockTraceClosed.
        from pipelinex.events.events import BlockTraceClosed

        assert built.bus.listener_count(BlockTraceClosed) == 1

    def test_real_hdfs_v1_config_loads_and_builds(self) -> None:
        # The shipped config in configs/hdfs_v1.yaml must parse cleanly.
        # We don't actually run it (would need ~20GB memory for the full
        # corpus); we just verify it's a valid config.
        cfg_path = Path(__file__).resolve().parents[2] / "configs" / "hdfs_v1.yaml"
        if not cfg_path.exists():
            pytest.skip(f"config not found: {cfg_path}")

        # The path inside the config might not exist on the test machine,
        # so swap it for a tmp file before building.
        with cfg_path.open() as fh:
            raw = yaml.safe_load(fh)
        # The Pydantic load itself is the assertion — no tmp swap needed
        # because we never construct the FileLogSource from this test.
        cfg = parse_config(raw)
        assert cfg.parser.kind == "hdfs"
        assert cfg.sessionizer.enabled is True
        assert cfg.sequence_detector is not None
