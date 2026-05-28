"""
Tools package for the RCA Agent service.
Exports all tool functions, schemas, and the Tool base class so the Step 13d
Kafka consumer can register tools in a single import statement.
  1. Creating a new module in this package (tools/search_runbooks.py, etc.)
  2. Adding its exports to __all__ here
Existing tool modules are never modified. The agent's register_tool call
in the consumer receives the new tool — zero changes to agent.py or any
existing tool module.
the consumer depends on these abstractions
(function + schema pairs), not on the asyncpg pool or tenant_id directly.
functools.partial binds the infrastructure arguments at the call site.
"""

from tools.base import Tool, ToolSchema
from tools.build_timeline import BUILD_TIMELINE_SCHEMA, build_timeline
from tools.get_dependencies import GET_DEPENDENCIES_SCHEMA, get_dependencies
from tools.query_logs import QUERY_LOGS_SCHEMA, query_logs
from tools.search_knowledge_base import SEARCH_KNOWLEDGE_BASE_SCHEMA, search_knowledge_base

# __all__ controls what `from tools import *` exports and documents the public
# surface of this package explicitly. Anything not in __all__ is an internal
# implementation detail that callers should not depend on.
__all__ = [
    # Base class and TypedDict — used to type-check tool registration calls.
    "Tool",
    "ToolSchema",
    # QueryLogs: primary evidence-gathering tool.
    "query_logs",
    "QUERY_LOGS_SCHEMA",
    # GetDependencies: trace-based service topology discovery.
    "get_dependencies",
    "GET_DEPENDENCIES_SCHEMA",
    # BuildTimeline: chronological root-cause sequencing.
    "build_timeline",
    "BUILD_TIMELINE_SCHEMA",
    # SearchKnowledgeBase: hybrid RAG over past_incidents (Step 13c).
    # Uses BM25 + ChromaDB vector search + RRF + cross-encoder reranking.
    "search_knowledge_base",
    "SEARCH_KNOWLEDGE_BASE_SCHEMA",
]
