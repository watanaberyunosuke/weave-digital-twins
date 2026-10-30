"""Choose the part of the pipe graph that a capped hydraulic model can solve.

Kept free of database and solver imports so the selection can be tested on
its own (model/test_subgraph.py) without a running stack.

    select_supplied_subgraph(pipes, sources, cap)
    element_name(prefix, source_id)
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field

# EPANET 2.2 ids are at most 31 characters and are split on whitespace, with
# ';' starting a comment and '"' quoting, so any of those would corrupt the
# .inp file.
EPANET_MAX_ID = 31
_UNSAFE_ID = re.compile(r'[\s;"]')


def element_name(prefix: str, source_id: str) -> str:
    """Model element id that maps back to its source asset id.

    The source id is used as-is wherever EPANET accepts it, so results join
    straight back to asset.pipe / asset.facility. Ids that are too long or
    contain separators fall back to a hash of the source id: still stable
    across builds, and recomputable from the source id by any consumer.
    """
    name = f"{prefix}{source_id}"
    if len(name) <= EPANET_MAX_ID and not _UNSAFE_ID.search(name):
        return name
    digest = hashlib.sha1(str(source_id).encode()).hexdigest()[:16]
    return f"{prefix}#{digest}"


@dataclass(frozen=True)
class Pipe:
    pipe_id: str
    start_node: int
    end_node: int
    length_m: float
    diameter_m: float


@dataclass
class Selection:
    pipes: list[Pipe]                       # in selection (walk) order
    nodes: set[int]                         # every node the selected pipes touch
    unsupplied_pipes: int = 0               # in components no source reaches
    capped_pipes: int = 0                   # reachable, but beyond the cap
    skipped_pipes: int = 0                  # self-loops after snapping
    sources_without_pipes: list[int] = field(default_factory=list)


def select_supplied_subgraph(pipes: list[Pipe], sources: list[int],
                             cap: int) -> Selection:
    """Walk outward from the sources and keep pipes until the cap is reached.

    A plain LIMIT keeps an arbitrary subset whose pieces may have no path to
    a reservoir, and EPANET cannot solve a fragment with no fixed head. Every
    pipe kept here touches a node already reached from a source, so the
    selection is connected to supply by construction. Pipes between two
    reached nodes are kept too, because loops carry flow.

    Determinism: sources are walked in node-id order and each node's pipes
    in pipe-id order, so the same graph and cap always give the same model.
    """
    adjacency, skipped = _adjacency(pipes)
    start = sorted(set(sources))
    selected = _walk(adjacency, start, cap)

    # Account for everything left out, so the build can say why the model is
    # smaller than the network.
    reachable = _reachable(adjacency, start)
    usable = [p for p in pipes if p.start_node != p.end_node]
    reachable_pipes = sum(1 for p in usable if p.start_node in reachable)

    nodes = {n for p in selected for n in (p.start_node, p.end_node)}
    return Selection(
        pipes=selected,
        nodes=nodes,
        unsupplied_pipes=len(usable) - reachable_pipes,
        capped_pipes=reachable_pipes - len(selected),
        skipped_pipes=skipped,
        sources_without_pipes=[s for s in start if not adjacency.get(s)],
    )


def _other_end(pipe: Pipe, node: int) -> int:
    return pipe.end_node if pipe.start_node == node else pipe.start_node


def _adjacency(pipes: list[Pipe]) -> tuple[dict[int, list[Pipe]], int]:
    """Each node's pipes in pipe-id order, and how many self-loops were dropped."""
    adjacency: dict[int, list[Pipe]] = defaultdict(list)
    skipped = 0
    for pipe in sorted(pipes, key=lambda p: p.pipe_id):
        if pipe.start_node == pipe.end_node:
            skipped += 1
            continue
        adjacency[pipe.start_node].append(pipe)
        adjacency[pipe.end_node].append(pipe)
    return adjacency, skipped


def _walk(adjacency: dict[int, list[Pipe]], start: list[int],
          cap: int) -> list[Pipe]:
    """Breadth-first from the sources, taking each pipe once, up to the cap."""
    reached: set[int] = set(start)
    queue = deque(start)
    taken: set[str] = set()
    selected: list[Pipe] = []
    while queue:
        node = queue.popleft()
        for pipe in adjacency.get(node, ()):
            if pipe.pipe_id in taken:
                continue
            if len(selected) >= cap:
                return selected
            taken.add(pipe.pipe_id)
            selected.append(pipe)
            other = _other_end(pipe, node)
            if other not in reached:
                reached.add(other)
                queue.append(other)
    return selected


def _reachable(adjacency: dict[int, list[Pipe]], start: list[int]) -> set[int]:
    """Every node connected to a source, ignoring the cap."""
    reachable: set[int] = set(start)
    walk = deque(start)
    while walk:
        node = walk.popleft()
        for pipe in adjacency.get(node, ()):
            other = _other_end(pipe, node)
            if other not in reachable:
                reachable.add(other)
                walk.append(other)
    return reachable
