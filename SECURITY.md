# Security Policy

## Reporting a Vulnerability

The Lightning project takes security seriously. If you discover a security
vulnerability, please report it responsibly.

**Do not file a public issue or discuss the vulnerability in public
channels.**

### Responsible Disclosure Process

1. Send a detailed report to **[security@lightning-db.org](mailto:security@lightning-db.org)**.
   Include:
   - A description of the vulnerability and its impact.
   - Steps to reproduce, including environment details.
   - Any proof-of-concept code or exploit scenarios.
   - Suggested remediation if available.

2. You will receive an acknowledgment within **72 hours**.

3. We will investigate and provide an initial assessment within **10 business
   days**, including a timeline for a fix.

4. We aim to release a fix and publish an advisory within **90 days** of the
   initial report. If circumstances require an extension, we will communicate
   the revised timeline.

5. Once a fix is released, we will coordinate the public disclosure date
   with you. We will credit you in the advisory unless you request anonymity.

### Scope

The following areas are in scope for the security policy:

| Area | Description |
|------|-------------|
| **Cypher injection** | Query injection vulnerabilities in the Cypher query engine, including unauthorized data access or modification via crafted queries. |
| **WASM UDF sandboxing** | Sandbox escapes or privilege escalation in the WebAssembly user-defined function runtime. |
| **File I/O** | Path traversal, unauthorized file access, or data leakage through the storage layer. |
| **C FFI** | Memory safety violations, buffer overflows, or use-after-free in the C foreign function interface. |
| **MVCC isolation violations** | Bugs that allow reading uncommitted data or violating snapshot isolation guarantees. |
| **Serialization/deserialization** | Remote code execution, denial of service, or data corruption via crafted serialized payloads. |

### Out of Scope

- Vulnerabilities in third-party dependencies (unless Lightning's usage
  exacerbates them). Please report those to the upstream project.
- Issues requiring physical access to the host machine.
- Denial of service caused by unbounded resource consumption (unless it
  results from a clear logic bug).
- Social engineering attacks.

### Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Pre-alpha — security fixes provided on a best-effort basis |

### Security Advisories

Published advisories are available at
[GitHub Security Advisories](https://github.com/lightning-db/lightning/security/advisories).
