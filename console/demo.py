"""Sample output for actions a demo user can only simulate.

Nothing here runs. The command shown is the exact one the admin console would
run, so the page still teaches the CLI; the lines below it are representative,
not read from the live stack, and say so on their first line. No place names:
the samples describe shapes of work, not a region.
"""

from __future__ import annotations

BANNER = "[demo] Simulated: nothing ran on the live stack. Output below is a sample."

_SAMPLES: dict[str, list[str]] = {
    "create_topics": [
        "Creating topics ...",
        "  water.hydrometry.raw.v1",
        "  electricity.market-dispatch.raw.v1",
        "  transport.vehicle-positions.raw.v1",
        "  water.meter-readings.raw.v1",
        "  water.pressure-readings.raw.v1",
        "  electricity.meter-interval.raw.v1",
    ],
    "load_gis": [
        "layer water_main: fetching features (limit {limit})",
        "layer water_main: {limit} features -> asset.gis_raw, asset.pipe",
        "layer reservoir: 12 features -> asset.facility",
        "build_topology: snapped pipe ends to shared nodes",
    ],
    "seed": [
        "Clustering pipes into {dmas} DMAs (ST_ClusterKMeans)",
        "Placing {meters_per_dma} meters per DMA on real pipe geometry",
        "Writing schemas/water-meter.avsc, pressure-logger.avsc, electricity-meter.avsc",
        "Re-register the connectors so Datagen picks up the new ids.",
    ],
    "register_connectors": [
        "Waiting for Kafka Connect ...",
        "datagen-water-meters: deleted, created (RUNNING)",
        "datagen-pressure-loggers: deleted, created (RUNNING)",
        "datagen-electricity-meters: deleted, created (RUNNING)",
    ],
    "build_model": [
        "Reservoirs snapped: 3 (0 beyond SIM_MAX_SNAP_M)",
        "Supplied subgraph: pipes kept up to SIM_MAX_PIPES, unsupplied fragments left out",
        "Wrote /app/out/network.inp",
    ],
    "start_flink": [
        "[INFO] Executing SQL from file.",
        "[INFO] Execute statement succeeded.",
        "[INFO] Submitting SQL update statement to the cluster...",
        "[INFO] SQL update statement has been successfully submitted to the cluster.",
    ],
    "reset_baseline": [
        "Residual baselines reset for {scope} from now.",
        "DMAs read UNKNOWN until SIM_RESIDUAL_MIN_SAMPLES new residuals exist.",
    ],
    "load_elevation": [
        "Sampling the region DEM onto asset.node ({scope})",
        "Nodes outside the DEM keep the 50 m default.",
    ],
    "discover": [
        "Searching the region catalogue ...",
        "No live search in a simulation.",
    ],
    "run_sim_once": [
        "Applied meter demands at junctions, then solved.",
        "Residuals written; DMA status updated.",
    ],
}


class _Fields(dict):
    def __missing__(self, key: str) -> str:
        return f"<{key}>"


def simulated_lines(action: str, params: dict, command: list[str]) -> list[str]:
    scope = "every DMA" if action == "reset_baseline" and not params.get("dma") else (
        f"DMA {params['dma']}" if action == "reset_baseline" else
        "every node" if params.get("all") else "nodes without an elevation")
    fields = {**{k: v for k, v in params.items()}, "scope": scope}
    if not fields.get("limit"):
        fields["limit"] = "none"
    # Validated params always carry every field; the fallback only keeps a
    # sample readable if one is ever missing.
    safe_fields = _Fields(fields)
    body = [line.format_map(safe_fields)
            for line in _SAMPLES.get(action, ["(no sample for this action)"])]
    return [BANNER, "$ " + " ".join(command), *body, "exit 0 (simulated)"]
