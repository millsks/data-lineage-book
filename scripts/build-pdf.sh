#!/usr/bin/env bash
#
# build-pdf.sh — Build a KDP-ready 7"×10" PDF of
# "Data Lineage: From Novice to Expert"
#
# Usage:
#   bash scripts/build-pdf.sh
#
# Prerequisites:
#   brew install pandoc
#   brew install --cask basictex   # or: brew install texlive
#   # After installing basictex, add TeX to PATH and install extras:
#   eval "$(/usr/libexec/path_helper)"
#   sudo tlmgr update --self
#   sudo tlmgr install collection-fontsrecommended fancyhdr titlesec \
#        enumitem booktabs longtable xcolor dejavu
#
# Optional (for Mermaid diagram rendering):
#   npm install -g @mermaid-js/mermaid-cli
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
OUTPUT="$BUILD_DIR/Data-Lineage-From-Novice-to-Expert.pdf"
METADATA="$SCRIPT_DIR/metadata.yaml"
FILTER="$SCRIPT_DIR/filter-nav.lua"
MERMAID_FILTER="$SCRIPT_DIR/mermaid-render.lua"
HEADER="$SCRIPT_DIR/header.tex"

# Chapter files in order
CHAPTERS=(
  "$ROOT_DIR/chapters/01-what-is-data-lineage.md"
  "$ROOT_DIR/chapters/02-metadata-fundamentals.md"
  "$ROOT_DIR/chapters/03-lineage-data-models.md"
  "$ROOT_DIR/chapters/04-your-first-lineage-graph.md"
  "$ROOT_DIR/chapters/05-openlineage-standard.md"
  "$ROOT_DIR/chapters/06-sql-lineage-parsing.md"
  "$ROOT_DIR/chapters/07-airflow-and-marquez.md"
  "$ROOT_DIR/chapters/08-spark-lineage.md"
  "$ROOT_DIR/chapters/09-dbt-lineage.md"
  "$ROOT_DIR/chapters/10-column-level-lineage.md"
  "$ROOT_DIR/chapters/11-graph-databases-lineage.md"
  "$ROOT_DIR/chapters/12-lineage-api-fastapi.md"
  "$ROOT_DIR/chapters/13-data-quality-lineage.md"
  "$ROOT_DIR/chapters/14-data-observability.md"
  "$ROOT_DIR/chapters/15-streaming-lineage.md"
  "$ROOT_DIR/chapters/16-compliance-governance-privacy.md"
  "$ROOT_DIR/chapters/17-data-mesh-federated-lineage.md"
  "$ROOT_DIR/chapters/18-ml-lineage.md"
  "$ROOT_DIR/chapters/19-genai-llm-lineage.md"
  "$ROOT_DIR/chapters/20-lineage-at-scale.md"
  "$ROOT_DIR/chapters/21-capstone-project.md"
)

# Verify prerequisites
for cmd in pandoc xelatex; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' not found. Please install it first." >&2
    echo "  pandoc:  brew install pandoc" >&2
    echo "  xelatex: brew install --cask basictex" >&2
    exit 1
  fi
done

# Verify all chapter files exist
for ch in "${CHAPTERS[@]}"; do
  if [[ ! -f "$ch" ]]; then
    echo "ERROR: Chapter file not found: $ch" >&2
    exit 1
  fi
done

# Create build directory
mkdir -p "$BUILD_DIR"

echo "=== Building PDF ==="
echo "  Output: $OUTPUT"
echo "  Chapters: ${#CHAPTERS[@]}"
echo ""

# Check for Mermaid rendering support (optional)
MERMAID_OPTS=()
if [[ "${SKIP_MERMAID:-}" == "1" ]]; then
  echo "  Mermaid rendering skipped (SKIP_MERMAID=1)"
elif command -v mmdc &>/dev/null; then
  echo "  Mermaid CLI (mmdc) detected — diagrams will be rendered as images"
  export MERMAID_IMG_DIR="$BUILD_DIR/mermaid-images"
  mkdir -p "$MERMAID_IMG_DIR"
  MERMAID_OPTS=(--lua-filter="$MERMAID_FILTER")
else
  echo "  WARNING: mmdc not found. Mermaid diagrams will appear as code blocks."
  echo "  Install with: npm install -g @mermaid-js/mermaid-cli"
fi
echo ""

# Run pandoc
# - index.md is included first as the front matter
# - All 21 chapter files follow in order
# - The Lua filter strips navigation links (← Back to Index, etc.)
# - metadata.yaml provides KDP formatting (7"×10", fonts, headers)
pandoc \
  --defaults="$METADATA" \
  "${MERMAID_OPTS[@]+"${MERMAID_OPTS[@]}"}" \
  --lua-filter="$FILTER" \
  --include-in-header="$HEADER" \
  --resource-path="$ROOT_DIR" \
  --from=markdown+pipe_tables+backtick_code_blocks+fenced_code_blocks \
  -o "$OUTPUT" \
  "$ROOT_DIR/index.md" \
  "${CHAPTERS[@]}"

echo ""
echo "=== Build complete ==="
echo "  PDF: $OUTPUT"
echo "  Size: $(du -h "$OUTPUT" | cut -f1)"
echo ""
echo "Next steps:"
echo "  1. Review the PDF in Preview or Acrobat"
echo "  2. Upload to Amazon KDP: https://kdp.amazon.com/"
echo "  3. Run KDP's automated print previewer to check margins"
