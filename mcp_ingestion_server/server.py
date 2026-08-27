#!/usr/bin/env python3
"""
Affiliate Junction MCP Ingestion Server

Exposes Kafka → watsonx.data ingestion as MCP tools callable from
Claude Desktop or any MCP-compatible client.

Tools (to be implemented in Stories 5–9):
  - subscribe_and_ingest  : consume a batch from Kafka, return structured events
"""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("affiliate-junction-ingestion")


def run():
    mcp.run()


if __name__ == "__main__":
    run()
