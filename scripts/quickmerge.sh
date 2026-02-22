#!/bin/bash
# quickmerge: Push changes through a PR with auto-merge
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#   ./scripts/quickmerge.sh "commit message" --files "path1 path2 path3"
#
# When --files is provided: only stage and commit those paths (repo-relative).
# When --files is omitted: stage all changes (git add -A).
#
# Agents MUST use --files with the list of files they changed to avoid
# committing other agents' partial work in multi-agent sessions.
#
# What it does:
#   1. Runs quality gates FIRST (scripts/quality-gates.sh)
#      - If quality gates fail, script exits immediately (fail fast)
#   2. Stashes changes, creates new branch from origin/main (without checking out local main)
#   3. Restores stashed changes onto new branch
#   4. Stages changes (--files or -A), commits
#   5. Pushes branch, creates PR with auto-merge (squash)
#   6. Stays on PR branch (does NOT checkout main)
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Auto-merge enabled on the repo (Settings > General > Allow auto-merge)
#
# Notes:
#   - If quickmerge fails and you fix it: run quickmerge again directly. Do NOT
#     run quality gates first—quickmerge already runs quality gates and pre-commit fixes.

set -e

# Source Cursor Team Kit enforcement prompts (optional but recommended)
WORKSPACE_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$WORKSPACE_ROOT/.cursor/scripts/cursor-team-kit-enforcement.sh" ]; then
    source "$WORKSPACE_ROOT/.cursor/scripts/cursor-team-kit-enforcement.sh"
    CURSOR_TEAM_KIT_ENABLED=1
else
    CURSOR_TEAM_KIT_ENABLED=0
fi

COMMIT_MSG="chore: automated update"
FILES_ARG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)
            FILES_ARG="$2"
            shift 2
            ;;
        *)
            COMMIT_MSG="$1"
            shift
            ;;
    esac
done

REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
REPO_NAME=$(basename "$REPO_DIR")

cd "$REPO_DIR"

VENV_ACTIVATED=0
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    VENV_ACTIVATED=1
    echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
elif [ -f ".venv/Scripts/activate" ]; then
    source .venv/Scripts/activate
    VENV_ACTIVATED=1
    echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
else
    echo "[$REPO_NAME] ⚠️  No .venv found - using system Python"
fi

if [ -f "pyproject.toml" ]; then
    echo "[$REPO_NAME] Installing project dependencies..."
    command -v uv >/dev/null 2>&1 || pip install uv --quiet
    uv pip install -e ".[dev]" || uv pip install -e . || true
fi

if [ -z "$(git status --porcelain)" ]; then
    echo "No changes to commit in $REPO_NAME"
    exit 0
fi

git fetch origin main --quiet 2>/dev/null || true
if git rev-parse origin/main &>/dev/null && [ -z "$(git diff origin/main 2>/dev/null)" ]; then
    echo "[$REPO_NAME] No differences from main - nothing to merge"
    exit 0
fi

if [ -f "scripts/quality-gates.sh" ]; then
    echo "[$REPO_NAME] Phase 1: Running quality gates (auto-fix)..."
    bash scripts/quality-gates.sh
    echo "[$REPO_NAME] Phase 2: Verifying quality gates (--no-fix)..."
    if ! bash scripts/quality-gates.sh --no-fix; then
        echo "[$REPO_NAME] ❌ Quality gates FAILED"
        exit 1
    fi
    echo "[$REPO_NAME] ✅ Quality gates PASSED"
else
    echo "[$REPO_NAME] ⚠️  No quality-gates.sh found"
fi

RESTORE_STASH=0
if [ -n "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] Stashing changes..."
    git stash push -u -m "quickmerge-$$" --quiet
    RESTORE_STASH=1
fi

git fetch origin main --quiet
BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
echo "[$REPO_NAME] Creating branch $BRANCH from origin/main"
git checkout -b "$BRANCH" origin/main --quiet

if [ "$RESTORE_STASH" = 1 ] && git stash list | grep -q "quickmerge-$$"; then
    git stash pop --quiet
fi

if [ -n "$FILES_ARG" ]; then
    for f in $FILES_ARG; do
        [ -e "$f" ] && git add "$f"
    done
else
    git add -A
fi

git commit -m "$COMMIT_MSG" --quiet
git push -u origin "$BRANCH" --quiet 2>/dev/null

PR_BODY="Automated PR. Will auto-merge once quality gates pass."
PR_URL=$(gh pr create --title "$COMMIT_MSG" --body "$PR_BODY" --base main --head "$BRANCH" 2>/dev/null)
PR_NUM=$(echo "$PR_URL" | grep -o "[0-9]*$")
gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true

echo "[$REPO_NAME] PR created: $PR_URL (auto-merge enabled)"
echo "[$REPO_NAME] To sync: git checkout main && git pull"
