# tests/

Cross-module test infrastructure that belongs to no single library.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-system-tests` | `tests/system` | Unpublished end-to-end system tests exercising the toolkit across module boundaries, plus the generator behind `docs/generated/action-inventory.json` |
| none (Node) | `tests/mcp-inspector` | MCP compatibility lane: the pinned official MCP Inspector CLI against the stdio and streamable-HTTP servers; see [its README](mcp-inspector/README.md) |

Refresh the checked-in action inventory after changing action registration:

```shell
./gradlew :protomolt-system-tests:generateActionInventory --max-workers=2
```

The inventory check compares that document with the running catalogs, so
stale names or manual count claims fail the build. See
[building and testing](../docs/operations/building.md).
