"""
FlowForge protocol types.

Pydantic models that match the Go protocol types defined in
pkg/protocol/protocol.go exactly. These are the wire-format types
exchanged between the platform and connectors.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class MessageType(str, Enum):
    SPEC = "SPEC"
    CHECK = "CHECK"
    DISCOVER = "DISCOVER"
    READ = "READ"
    WRITE = "WRITE"
    RECORD = "RECORD"
    STATE = "STATE"
    SCHEMA = "SCHEMA"
    LOG = "LOG"
    CONTROL = "CONTROL"
    MCP_TOOL_CALL = "MCP_TOOL_CALL"
    MCP_TOOL_RESULT = "MCP_TOOL_RESULT"


class StateType(str, Enum):
    STREAM = "STREAM"
    GLOBAL = "GLOBAL"


class SchemaChange(str, Enum):
    NONE = ""
    NEW_COLUMN = "NEW_COLUMN"
    REMOVED_COLUMN = "REMOVED_COLUMN"
    TYPE_CHANGE = "TYPE_CHANGE"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class ControlType(str, Enum):
    RATE_LIMIT = "RATE_LIMIT"
    BACKPRESSURE = "BACKPRESSURE"
    PAUSE = "PAUSE"
    RESUME = "RESUME"


class SyncMode(str, Enum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class DestinationSyncMode(str, Enum):
    APPEND = "append"
    UPSERT = "upsert"
    MERGE = "merge"
    SOFT_DELETE = "soft_delete"


class CheckStatus(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Payload models
# ---------------------------------------------------------------------------


class Record(BaseModel):
    """A single data record from a stream."""

    stream: str
    namespace: Optional[str] = None
    data: Dict[str, Any]
    emitted_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() + "Z"}


class State(BaseModel):
    """A checkpoint for incremental sync resumption."""

    type: StateType
    stream: Optional[str] = None
    data: Dict[str, Any]


class SchemaMessage(BaseModel):
    """Signals a stream schema declaration or change."""

    stream: str
    schema_: Dict[str, Any] = Field(alias="schema")
    change: Optional[SchemaChange] = None

    class Config:
        populate_by_name = True


class Log(BaseModel):
    """A structured log message from a connector."""

    level: LogLevel
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Optional[Dict[str, Any]] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() + "Z"}


class Control(BaseModel):
    """A bidirectional control signal."""

    type: ControlType
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


class Spec(BaseModel):
    """A connector's configuration JSON Schema."""

    documentation_url: Optional[str] = None
    config_schema: Dict[str, Any]


class MCPToolCall(BaseModel):
    """A tool invocation from an AI agent."""

    id: str
    tool: str
    parameters: Dict[str, Any]
    agent_id: Optional[str] = None
    tenant_id: Optional[str] = None


class MCPToolResult(BaseModel):
    """The result of an MCP tool execution."""

    call_id: str
    content: Any
    is_error: bool = False


# ---------------------------------------------------------------------------
# Message envelope
# ---------------------------------------------------------------------------


class Message(BaseModel):
    """
    Envelope for all protocol communication.
    Only one of the payload fields will be non-None for any given message.
    """

    type: MessageType
    record: Optional[Record] = None
    state: Optional[State] = None
    schema_: Optional[SchemaMessage] = Field(default=None, alias="schema")
    log: Optional[Log] = None
    control: Optional[Control] = None
    spec: Optional[Spec] = None
    mcp_tool_call: Optional[MCPToolCall] = None
    mcp_tool_result: Optional[MCPToolResult] = None

    class Config:
        populate_by_name = True

    @classmethod
    def record_message(cls, stream: str, data: Dict[str, Any], namespace: Optional[str] = None) -> Message:
        """Create a RECORD message."""
        return cls(
            type=MessageType.RECORD,
            record=Record(stream=stream, namespace=namespace, data=data),
        )

    @classmethod
    def state_message(cls, state_type: StateType, data: Dict[str, Any], stream: Optional[str] = None) -> Message:
        """Create a STATE message."""
        return cls(
            type=MessageType.STATE,
            state=State(type=state_type, stream=stream, data=data),
        )

    @classmethod
    def log_message(cls, level: LogLevel, message: str, data: Optional[Dict[str, Any]] = None) -> Message:
        """Create a LOG message."""
        return cls(
            type=MessageType.LOG,
            log=Log(level=level, message=message, data=data),
        )

    @classmethod
    def spec_message(cls, config_schema: Dict[str, Any], documentation_url: Optional[str] = None) -> Message:
        """Create a SPEC message."""
        return cls(
            type=MessageType.SPEC,
            spec=Spec(config_schema=config_schema, documentation_url=documentation_url),
        )

    @classmethod
    def schema_change_message(
        cls,
        stream: str,
        schema: Dict[str, Any],
        change: Optional[SchemaChange] = None,
    ) -> Message:
        """Create a SCHEMA message."""
        return cls(
            type=MessageType.SCHEMA,
            schema=SchemaMessage(stream=stream, schema=schema, change=change),
        )


# ---------------------------------------------------------------------------
# Stream / Catalog models
# ---------------------------------------------------------------------------


class Stream(BaseModel):
    """A data stream exposed by a connector."""

    name: str
    namespace: Optional[str] = None
    display_name: Optional[str] = None
    json_schema: Dict[str, Any]
    supported_sync_modes: List[SyncMode]
    default_cursor_field: Optional[List[str]] = None
    source_defined_primary_key: bool = False
    primary_key: Optional[List[List[str]]] = None


class ConfiguredStream(BaseModel):
    """A user-selected stream with sync configuration."""

    stream: Stream
    sync_mode: SyncMode
    destination_sync_mode: Optional[DestinationSyncMode] = None
    cursor_field: Optional[List[str]] = None
    primary_key: Optional[List[List[str]]] = None


class Catalog(BaseModel):
    """The full set of streams a connector exposes."""

    streams: List[Stream]


class ConfiguredCatalog(BaseModel):
    """The user-selected streams and their configurations."""

    streams: List[ConfiguredStream]


# ---------------------------------------------------------------------------
# Check / Write results
# ---------------------------------------------------------------------------


class CheckResult(BaseModel):
    """The result of a connectivity check."""

    status: CheckStatus
    message: Optional[str] = None

    @classmethod
    def succeeded(cls, message: str = "Connection successful") -> CheckResult:
        return cls(status=CheckStatus.SUCCEEDED, message=message)

    @classmethod
    def failed(cls, message: str) -> CheckResult:
        return cls(status=CheckStatus.FAILED, message=message)


class WriteError(BaseModel):
    """A single write failure."""

    message: str
    record: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None


class WriteResult(BaseModel):
    """The result of a write operation."""

    records_written: int = 0
    errors: List[WriteError] = Field(default_factory=list)
    state_messages: List[State] = Field(default_factory=list)
