"""
FlowForge Destination connector base class and registration decorator.

Provides the abstract Destination interface matching the Go CDK Destination
interface, a decorator for registering destination connectors, and a CLI runner.
"""

from __future__ import annotations

import abc
import json
import sys
from typing import Any, Callable, Dict, Iterator, List, Optional, Type

from flowforge_cdk.protocol import (
    CheckResult,
    ConfiguredCatalog,
    DestinationSyncMode,
    Log,
    LogLevel,
    Message,
    MessageType,
    Spec,
    State,
    StateType,
    WriteError,
    WriteResult,
)

# ---------------------------------------------------------------------------
# Global registry
# ---------------------------------------------------------------------------

_destination_registry: Dict[str, Type[Destination]] = {}


def get_registered_destinations() -> Dict[str, Type[Destination]]:
    """Return a copy of the destination registry."""
    return dict(_destination_registry)


# ---------------------------------------------------------------------------
# Destination base class
# ---------------------------------------------------------------------------


class Destination(abc.ABC):
    """
    Abstract base class for FlowForge destination connectors.

    Mirrors the Go ``cdk.Destination`` interface:
      - Spec() -> Spec
      - Check(config) -> CheckResult
      - Write(config, catalog, messages) -> WriteResult
      - Capabilities() -> list of DestinationSyncMode

    Subclasses must implement all four abstract methods.
    """

    @abc.abstractmethod
    def spec(self) -> Spec:
        """Return the JSON Schema describing this destination's configuration."""
        ...

    @abc.abstractmethod
    def check(self, config: Dict[str, Any]) -> CheckResult:
        """Validate write access to the destination."""
        ...

    @abc.abstractmethod
    def write(
        self,
        config: Dict[str, Any],
        catalog: ConfiguredCatalog,
        messages: Iterator[Message],
    ) -> WriteResult:
        """
        Consume record messages and write them to the destination.

        ``messages`` is an iterator of ``Message`` objects (primarily RECORD type).
        Returns a ``WriteResult`` summarizing the operation.
        """
        ...

    @abc.abstractmethod
    def capabilities(self) -> List[DestinationSyncMode]:
        """Return the destination sync modes this connector supports."""
        ...

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def emit_log(self, level: LogLevel, message: str, data: Optional[Dict[str, Any]] = None) -> Message:
        """Create a LOG message."""
        return Message.log_message(level=level, message=message, data=data)

    def create_write_result(
        self,
        records_written: int,
        errors: Optional[List[WriteError]] = None,
        state_messages: Optional[List[State]] = None,
    ) -> WriteResult:
        """Create a WriteResult with the given counts."""
        return WriteResult(
            records_written=records_written,
            errors=errors or [],
            state_messages=state_messages or [],
        )


# ---------------------------------------------------------------------------
# Registration decorator
# ---------------------------------------------------------------------------


def destination(name: str) -> Callable[[Type[Destination]], Type[Destination]]:
    """
    Decorator to register a Destination connector class.

    Usage::

        @destination("my-warehouse")
        class MyWarehouseDestination(Destination):
            ...
    """

    def decorator(cls: Type[Destination]) -> Type[Destination]:
        if not issubclass(cls, Destination):
            raise TypeError(f"{cls.__name__} must be a subclass of Destination")
        _destination_registry[name] = cls
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


def _write_json_line(data: Dict[str, Any]) -> None:
    """Write a JSON line to stdout."""
    sys.stdout.write(json.dumps(data) + "\n")
    sys.stdout.flush()


def _stdin_message_iterator() -> Iterator[Message]:
    """Read protocol messages from stdin, one JSON object per line."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        raw = json.loads(line)
        yield Message.model_validate(raw)


def _extract_option(argv: List[str], flag: str) -> Optional[str]:
    """Extract the value following a CLI flag."""
    try:
        idx = argv.index(flag)
        if idx + 1 < len(argv):
            return argv[idx + 1]
    except ValueError:
        pass
    return None


def run_destination(dest_cls: Type[Destination], args: Optional[List[str]] = None) -> None:
    """
    CLI entry point for running a destination connector.

    Supports the standard connector protocol commands:
      - spec
      - check --config <path>
      - write --config <path> --catalog <path>
        (reads RECORD messages from stdin)

    Usage in a connector module::

        if __name__ == "__main__":
            run_destination(MyDestination)
    """
    argv = args if args is not None else sys.argv[1:]

    if not argv:
        print(
            "Usage: <connector> <command> [options]\n"
            "Commands: spec, check, write",
            file=sys.stderr,
        )
        sys.exit(1)

    command = argv[0]
    dest_instance = dest_cls()

    if command == "spec":
        spec_result = dest_instance.spec()
        msg = Message.spec_message(
            config_schema=spec_result.config_schema,
            documentation_url=spec_result.documentation_url,
        )
        payload = msg.model_dump(by_alias=True, exclude_none=True)
        _write_json_line(payload)

    elif command == "check":
        config_path = _extract_option(argv, "--config")
        if not config_path:
            print("Error: --config <path> is required for check", file=sys.stderr)
            sys.exit(1)
        config = _read_json_arg(config_path)
        result = dest_instance.check(config)
        payload = {
            "type": "CHECK",
            "check": result.model_dump(exclude_none=True),
        }
        _write_json_line(payload)

    elif command == "write":
        config_path = _extract_option(argv, "--config")
        catalog_path = _extract_option(argv, "--catalog")

        if not config_path or not catalog_path:
            print(
                "Error: --config <path> and --catalog <path> are required for write",
                file=sys.stderr,
            )
            sys.exit(1)

        config = _read_json_arg(config_path)
        catalog_data = _read_json_arg(catalog_path)
        catalog = ConfiguredCatalog.model_validate(catalog_data)

        messages = _stdin_message_iterator()
        write_result = dest_instance.write(config, catalog, messages)

        payload = {
            "type": "WRITE",
            "write_result": write_result.model_dump(exclude_none=True),
        }
        _write_json_line(payload)

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)
