# AGENTS.md — unified-domain-client

## Quick Reference for AI Agents

### Key Commands

- **Quality gates**: `cd unified-domain-client && bash scripts/quality-gates.sh`
- **Source dir**: `unified-domain-client/unified_domain_client/` (underscored)
- **Typecheck**: `run_timeout 120 basedpyright unified_domain_client/`

### Mandatory Rules

Before any action, read:
`unified-trading-pm/cursor-configs/SUB_AGENT_MANDATORY_RULES.md`

### Rules Summary

- `uv pip install` not `pip install`
- Flat deps only — no `[project.optional-dependencies]`
- `basedpyright` not `pyright`
- `UnifiedCloudConfig` not `os.getenv()`
- No `# type: ignore` to hide architectural violations
- No `try/except ImportError` fallbacks

### Workspace

WORKSPACE_ROOT: `/Users/ikennaigboaka/Code/unified-trading-system-repos`
