"""Chapter 19 Exercise — GenAI & LLM Lineage.

Track lineage for RAG pipelines: documents → chunks → embeddings
→ retrieval → prompt → generation.

Usage:
    pixi run -e exercises python docs/exercises/ch19_genai_lineage.py
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Document:
    id: str
    source: str
    content: str
    ingested_at: datetime = field(default_factory=datetime.now)


@dataclass
class Chunk:
    id: str
    doc_id: str
    text: str
    index: int


@dataclass
class RAGEvent:
    """A single RAG pipeline execution event."""
    query: str
    retrieved_chunks: list[str]  # chunk IDs
    prompt_template: str
    response: str
    model: str
    latency_ms: float
    timestamp: datetime = field(default_factory=datetime.now)


class RAGLineageTracker:
    """Track lineage through a RAG pipeline."""

    def __init__(self):
        self.documents: dict[str, Document] = {}
        self.chunks: dict[str, Chunk] = {}
        self.events: list[RAGEvent] = []

    def ingest_document(self, doc: Document, chunk_size: int = 200) -> list[Chunk]:
        """Ingest a document and split into chunks."""
        self.documents[doc.id] = doc
        chunks = []
        words = doc.content.split()
        for i in range(0, len(words), chunk_size):
            text = " ".join(words[i : i + chunk_size])
            chunk_id = hashlib.md5(
                f"{doc.id}:{i}".encode()
            ).hexdigest()[:12]
            chunk = Chunk(chunk_id, doc.id, text, i // chunk_size)
            self.chunks[chunk_id] = chunk
            chunks.append(chunk)
        return chunks

    def record_event(self, event: RAGEvent):
        self.events.append(event)

    def source_documents_for_response(self, event_idx: int) -> list[Document]:
        """Trace a response back to its source documents."""
        if event_idx >= len(self.events):
            return []
        event = self.events[event_idx]
        doc_ids = set()
        for chunk_id in event.retrieved_chunks:
            chunk = self.chunks.get(chunk_id)
            if chunk:
                doc_ids.add(chunk.doc_id)
        return [self.documents[did] for did in doc_ids if did in self.documents]

    def document_usage_stats(self) -> dict[str, int]:
        """Count how often each document contributed to responses."""
        usage: dict[str, int] = {}
        for event in self.events:
            seen_docs: set[str] = set()
            for chunk_id in event.retrieved_chunks:
                chunk = self.chunks.get(chunk_id)
                if chunk and chunk.doc_id not in seen_docs:
                    seen_docs.add(chunk.doc_id)
                    usage[chunk.doc_id] = usage.get(chunk.doc_id, 0) + 1
        return usage

    def cost_estimate(self) -> dict:
        """Estimate token cost across all events."""
        total_latency = sum(e.latency_ms for e in self.events)
        return {
            "total_events": len(self.events),
            "total_latency_ms": total_latency,
            "avg_latency_ms": total_latency / max(len(self.events), 1),
        }


def main():
    print("=== Chapter 19: GenAI & LLM Lineage ===\n")

    tracker = RAGLineageTracker()

    # Ingest documents
    docs = [
        Document("doc-001", "confluence", "Data lineage tracks the flow of data "
                 "from source systems through transformations to analytics outputs. "
                 "Understanding lineage helps with compliance audit and impact analysis. " * 5),
        Document("doc-002", "wiki", "OpenLineage is an open standard for lineage metadata. "
                 "It defines events with inputs outputs and facets. "
                 "Jobs emit START COMPLETE and FAIL events. " * 5),
    ]

    all_chunks = []
    for doc in docs:
        chunks = tracker.ingest_document(doc, chunk_size=20)
        all_chunks.extend(chunks)
        print(f"Ingested {doc.id}: {len(chunks)} chunks")
    print()

    # Simulate RAG events
    events = [
        RAGEvent(
            query="What is data lineage?",
            retrieved_chunks=[all_chunks[0].id, all_chunks[1].id],
            prompt_template="Answer based on context: {context}\n\nQ: {query}",
            response="Data lineage tracks data flow from sources to outputs.",
            model="gpt-4",
            latency_ms=450.0,
        ),
        RAGEvent(
            query="What is OpenLineage?",
            retrieved_chunks=[all_chunks[-2].id, all_chunks[-1].id],
            prompt_template="Answer based on context: {context}\n\nQ: {query}",
            response="OpenLineage is an open standard for lineage metadata.",
            model="gpt-4",
            latency_ms=380.0,
        ),
    ]
    for event in events:
        tracker.record_event(event)

    # Trace sources
    print("Source documents for response 0:")
    sources = tracker.source_documents_for_response(0)
    for doc in sources:
        print(f"  ← {doc.id} ({doc.source})")
    print()

    # Document usage
    print("Document usage across responses:")
    usage = tracker.document_usage_stats()
    for doc_id, count in usage.items():
        print(f"  {doc_id}: {count} response(s)")
    print()

    # Cost estimate
    print("Cost estimate:")
    costs = tracker.cost_estimate()
    for k, v in costs.items():
        print(f"  {k}: {v}")
    print()

    print("✓ Exercises complete!")


if __name__ == "__main__":
    main()
