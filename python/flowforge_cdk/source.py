"""
FlowForge Source connector base class and registration decorator.

Provides the abstract Source interface matching the Go CDK Source interface,
a decorator for registering source connectors, and a built-in CLI runner.
"""

from __future__ import annotations

import abc
import json
import sys
from typing import Any, Callable, Dict, Iterator, List, Optional, Type

from flowforge_cdk.protocol import (
    Catalog,
    CheckResult,
    ConfiguredCatalog,
    Log,
    LogLevel,
    Message,
    MessageType,
    Spec,
    State,
    StateType,
)

# ---------------------------------------------------------------------------
# Global registry
# ---------------------------------------------------------------------------

_source_registry: Dict[str, Type[Source]] = {}


def get_registered_sources() -> Dict[str, Type[Source]]:
    """Return a copy of the source registry."""
    return dict(_source_registry)


# ---------------------------------------------------------------------------
# Source base class
# ---------------------------------------------------------------------------


class Source(abc.ABC):
    """
    Abstract base class for FlowForge source connectors.

    Mirrors the Go ``cdk.Source`` interface:
      - Spec() -> Spec
      - Check(config) -> CheckResult
      - Discover(config) -> Catalog
      - Read(config, catalog, state) -> Iterator[Message]

    Subclasses must implement all four abstract methods.
    """

    @abc.abstractmethod
    def spec(self) -> Spec:
        """Return the JSON Schema describing this connector's configuration."""
        ...

    @abc.abstractmethod
    def check(self, config: Dict[str, Any]) -> CheckResult:
        """Validate that the connector can reach the external system."""
        ...

    @abc.abstractmethod
    def discover(self, config: Dict[str, Any]) -> Catalog:
        """Return a catalog of available streams and their schemas."""
        ...

    @abc.abstractmethod
    def read(
        self,
        config: Dict[str, Any],
        catalog: ConfiguredCatalog,
        state: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Message]:
        """
        Yield records from the selected streams.

        For incremental syncs, ``state`` contains the last checkpoint.
        Implementations yield ``Message`` objects (records, state checkpoints, logs).
        """
        ...

    # ------------------------------------------------------------------
    # Convenience helpers available to all source implementations
    # ------------------------------------------------------------------

    def emit_record(self, stream: str, data: Dict[str, Any], namespace: Optional[str] = None) -> Message:
        """Create a RECORD message."""
        return Message.record_message(stream=stream, data=data, namespace=namespace)

    def emit_state(self, data: Dict[str, Any], stream: Optional[str] = None) -> Message:
        """Create a STATE message for checkpointing."""
        state_type = StateType.STREAM if stream else StateType.GLOBAL
        return Message.state_message(state_type=state_type, data=data, stream=stream)

    def emit_log(self, level: LogLevel, message: str, data: Optional[Dict[str, Any]] = None) -> Message:
        """Create a LOG message."""
        return Message.log_message(level=level, message=message, data=data)


# ---------------------------------------------------------------------------
# Registration decorator
# ---------------------------------------------------------------------------


def source(name: str) -> Callable[[Type[Source]], Type[Source]]:
    """
    Decorator to register a Source connector class.

    Usage::

        @source("my-api")
        class MyAPISource(Source):
            ...
    """

    def decorator(cls: Type[Source]) -> Type[Source]:
        if not issubclass(cls, Source):
            raise TypeError(f"{cls.__name__} must be a subclass of Source")
        _source_registry[name] = cls
        cls._connector_name = name  # type: ignore[attr-defined]
        return cls

    return decorator


# ---------------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------------


def _read_json_arg(path: str) -> Dict[str, Any]:
    """Read a JSON file and return parsed contents."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_message(msg: Message) -> None:
    """Write a single protocol message as a JSON line to stdout."""
    payload = msg.model_dump(by_alias=True, exclude_none=True)
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def run_source(source_cls: Type[Source], args: Optional[List[str]] = None) -> None:
    """
    CLI entry point for running a source connector.

    Supports the standard connector protocol commands:
      - spec
      - check --config <path>
      - discover --config <path>
      - read --config <path> --catalog <path> [--state <path>]

    Usage in a connector module::

        if __name__ == "__main__":
            run_source(MySource)
    """
    argv = args if args is not None else sys.argv[1:]

    if not argv:
        print(
            "Usage: <connector> <command> [options]\n"
            "Commands: spec, check, discover, read",
            file=sys.stderr,
        )
        sys.exit(1)

    command = argv[0]
    source_instance = source_cls()

    if command == "spec":
        spec_result = source_instance.spec()
        msg = Message.spec_message(
            config_schema=spec_result.config_schema,
            documentation_url=spec_result.documentation_url,
        )
        _write_message(msg)

    elif command == "check":
        config_path = _extract_option(argv, "--config")
        if not config_path:
            print("Error: --config <path> is required for check", file=sys.stderr)
            sys.exit(1)
        config = _read_json_arg(config_path)
        result = source_instance.check(config)
        payload = {
            "type": "CHECK",
            "check": result.model_dump(exclude_none=True),
        }
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()

    elif command == "discover":
        config_path = _extract_option(argv, "--config")
        if not config_path:
            print("Error: --config <path> is required for discover", file=sys.stderr)
            sys.exit(1)
        config = _read_json_arg(config_path)
        catalog = source_instance.discover(config)
        payload = {
            "type": "DISCOVER",
            "catalog": catalog.model_dump(exclude_none=True),
        }
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()

    elif command == "read":
        config_path = _extract_option(argv, "--config")
        catalog_path = _extract_option(argv, "--catalog")
        state_path = _extract_option(argv, "--state")

        if not config_path or not catalog_path:
            print(
                "Error: --config <path> and --catalog <path> are required for read",
                file=sys.stderr,
            )
            sys.exit(1)

        config = _read_json_arg(config_path)
        catalog_data = _read_json_arg(catalog_path)
        catalog = ConfiguredCatalog.model_validate(catalog_data)

        state: Optional[Dict[str, Any]] = None
        if state_path:
            state = _read_json_arg(state_path)

        for message in source_instance.read(config, catalog, state):
            _write_message(message)

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)


def _extract_option(argv: List[str], flag: str) -> Optional[str]:
    """Extract the value following a CLI flag."""
    try:
        idx = argv.index(flag)
        if idx + 1 < len(argv):
            return argv[idx + 1]
    except ValueError:
        pass
    return None
