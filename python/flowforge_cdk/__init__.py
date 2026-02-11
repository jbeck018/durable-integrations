"""
FlowForge Connector Development Kit (CDK) for Python.

Provides base classes, protocol types, and utilities for building
FlowForge connectors in Python.

Usage::

    from flowforge_cdk import Source, source, protocol

    @source("my-source")
    class MySource(Source):
        ...
"""

from flowforge_cdk.protocol import (
    Catalog,
    CheckResult,
    CheckStatus,
    ConfiguredCatalog,
    ConfiguredStream,
    Control,
    ControlType,
    DestinationSyncMode,
    Log,
    LogLevel,
    MCPToolCall,
    MCPToolResult,
    Message,
    MessageType,
    Record,
    SchemaChange,
    SchemaMessage,
    Spec,
    State,
    StateType,
    Stream,
    SyncMode,
    WriteError,
    WriteResult,
)
from flowforge_cdk.source import Source, source
from flowforge_cdk.destination import Destination, destination
from flowforge_cdk.http_client import AsyncRESTClient

__version__ = "0.1.0"

__all__ = [
    # Protocol types
    "Catalog",
    "CheckResult",
    "CheckStatus",
    "ConfiguredCatalog",
    "ConfiguredStream",
    "Control",
    "ControlType",
    "DestinationSyncMode",
    "Log",
    "LogLevel",
    "MCPToolCall",
    "MCPToolResult",
    "Message",
    "MessageType",
    "Record",
    "SchemaChange",
    "SchemaMessage",
    "Spec",
    "State",
    "StateType",
    "Stream",
    "SyncMode",
    "WriteError",
    "WriteResult",
    # Base classes
    "Source",
    "Destination",
    # Decorators
    "source",
    "destination",
    # Utilities
    "AsyncRESTClient",
]
