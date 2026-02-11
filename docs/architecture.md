---
title: Architecture
layout: default
nav_order: 2
has_children: true
description: "FlowForge system architecture, data flows, and design patterns."
permalink: /architecture/
---

# Architecture

FlowForge is a 6-layer architecture where each layer is independently scalable. All data movement is orchestrated by Temporal.io durable workflows, ensuring no data loss even under infrastructure failures.

```
┌─────────────────────────────────────────────────────────────┐
│                     API Gateway Layer                        │
│     REST (gRPC-Gateway) · Connect-RPC · GraphQL · WebSocket │
├─────────────────────────────────────────────────────────────┤
│                     Control Plane Layer                      │
│     Scheduler · Config Manager · Tenant Manager · Schema    │
├─────────────────────────────────────────────────────────────┤
│                  Orchestration Engine Layer                  │
│              Temporal.io Server Cluster                      │
│       Workflows · Activities · Task Queues · Signals        │
├─────────────────────────────────────────────────────────────┤
│                     Worker Pool Layer                        │
│   Extract Workers · Transform Workers · Load Workers        │
│                  MCP Workers · Connector Workers             │
├─────────────────────────────────────────────────────────────┤
│                   Connector Runtime Layer                    │
│     Native Plugins · OCI Containers · WASM Sandboxes        │
│          CDK Runtime · OAuth Manager · Protocol             │
├─────────────────────────────────────────────────────────────┤
│                      Data Layer                             │
│    PostgreSQL · Redis · S3-Compatible · HashiCorp Vault     │
└─────────────────────────────────────────────────────────────┘
```

Explore the child pages below for in-depth documentation of each architectural component.
