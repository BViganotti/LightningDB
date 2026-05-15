#!/bin/bash
# Pre-commit hook: scan for accidentally committed secrets, API keys, and tokens.
# Install: ln -s ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit

# Patterns that look like secrets (API keys, tokens, passwords)
SECRET_PATTERNS=(
    '(?i)(api[_-]?key|secret|token|password|passwd|credential|auth_token|access_key|private_key)\s*[:=]\s*['"'"'"][A-Za-z0-9_\-]{16,}['"'"'"]'
    'ghp_[A-Za-z0-9_]{36,}'        # GitHub PAT
    'gho_[A-Za-z0-9_]{36,}'        # GitHub OAuth
    'sk-[A-Za-z0-9_]{20,}'         # OpenAI key
    'xox[baprs]-[A-Za-z0-9_\-]{10,}'  # Slack token
    'AKIA[0-9A-Z]{16}'             # AWS access key
)

# Files to exclude (generated files, test fixtures, etc.)
EXCLUDE_PATTERNS=(
    '\.git/'
    'target/'
    '\.lbug$'
    '\.bin$'
    '\.wasm$'
)

EXCLUDE_JOINED=$(IFS='|'; echo "${EXCLUDE_PATTERNS[*]}")

for FILE in $(git diff --cached --name-only --diff-filter=ACM | grep -v -E "$EXCLUDE_JOINED"); do
    if [ -f "$FILE" ]; then
        for PATTERN in "${SECRET_PATTERNS[@]}"; do
            if grep -Pq "$PATTERN" "$FILE" 2>/dev/null; then
                # Check if it's in a test file or example — allow those
                if echo "$FILE" | grep -qE '(_test\.rs|test_|example|\.md$)'; then
                    continue
                fi
                echo "ERROR: Potential secret found in $FILE (matches: $PATTERN)"
                echo "If this is a false positive, add an exclusion pattern."
                exit 1
            fi
        done
    fi
done

exit 0
