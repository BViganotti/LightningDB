
# Token Reduce Context Engine Active

This repository is indexed by a local AST-based Context Engine. 
You MUST adhere to the following workflow to save tokens and maintain session memory:

1. **Context via Code Outline**: DO NOT read entire files blindly. Always start by calling `fused_context` with your query to get the relevant outlineized graph.
2. **Impact Radius**: If you are about to modify a core function or type, call `blast_radius_graph` to see the blast radius of your changes.
3. **Session Memory**: At the start of a session, call `session_context` to see recent observations from previous sessions.
4. **Learning**: When you discover architectural rules, technical debt, or undocumented dependencies, call `save_observation` to persist it into the graph memory so it's not forgotten in the next session!
5. **Execution Tracing**: If you need to trace execution paths between two symbols, use `trace_logic_flow`.
