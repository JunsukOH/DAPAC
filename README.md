# DAPAC
"DAPAC: Deadline-Aware Prefetch-Aided Caching with Online Content and Request-Time Prediction," submitted to International Conference on Information Networking (ICOIN), Nha Trang, Vietnam, 2027

## Implementation snapshot

This repository contains a snapshot of the on-time caching algorithm used in the KOREN proof of concept. It preserves the `ack-committed-v3` processing order: **validate the delivery acknowledgment, commit the user's per-video playback position, predict the next request, and schedule prefetching**.

## Architecture and source files

```text
Campus content origin → KOREN intermediate cache → KOREN synthetic-user receiver
```

- [`cache/poc.py`](cache/poc.py): location- and current-stream-conditioned Markov prediction, per-user request-interval EMA, LRU/TTL cache, and origin download worker.
- [`cache/experiment.py`](cache/experiment.py): hourly rotation through no caching, demand-only LRU, global Top-1 stream-popularity prefetching, and on-time caching.
- [`cache/cache_api.py`](cache/cache_api.py): deterministic segment generation and HTTPS responses, synthetic users, per-video resume state, receiver-side integrity checks, and duplicate-delivery acknowledgment handling.
- [`cache/cache_transport.py`](cache/cache_transport.py): mutually authenticated HTTPS origin downloads and delivery to the receiver.
- [`cache/image_https.py`](cache/image_https.py) and [`cache/relay.py`](cache/relay.py): shared HTTPS and image-exchange support modules retained from the running implementation.
- [`cache/experiment_report.py`](cache/experiment_report.py) and [`cache/experiment-status.py`](cache/experiment-status.py): experiment exports and freshness checks for current metrics.
- [`cache/ontime-cache-route.py`](cache/ontime-cache-route.py): source-policy routing validation and repair using example addresses. Review the configuration before use because this helper can modify local routing.
- [`tools/generate-tls.sh`](tools/generate-tls.sh): generation of new test or deployment certificates. Existing deployment keys and certificates are not included.

## Experiment profile

| Parameter | Value |
|---|---|
| Synthetic users / video streams | 8 / 8 |
| Segment size | 2 MiB |
| Shared cache capacity / TTL | 128 MiB / 30 seconds |
| Next-stream model | Shared transition observations, conditioned on current zone and stream |
| Next-request timing | Per-user EMA with coefficient 0.2 |
| Origin download duration | Shared EMA with coefficient 0.2; measured from fetch initiation through payload SHA-256 validation |
| Prediction-score threshold | 0.55 |
| Safety margin | 1 second in the deployment profile; 0.5 seconds in the CLI default and original baseline |
| Origin payload rate limit | 100 Mbps |
| Configured workload | Aggregate target of 110 Mbps; achieved throughput may differ |

Streams consist of deterministic synthetic segments rather than decoded video files. No stream length or end-of-video event is configured. A user's first visit starts at segment zero; returning to a stream resumes that user's saved next segment. All users share the same segment keys `(stream, seq)` and cache payloads.

## Tests

Requirements: Linux, Python 3.10 or later, Bash, and OpenSSL. No additional Python packages are required. Integration tests create temporary mTLS certificates and loopback servers to verify actual payload transfers.

```bash
bash tools/check.sh
```

Tests do not connect to deployment hosts or modify systemd, firewall settings, or actual routing. Routing tests exercise only the planning function.

## Running the implementation

Addresses in this repository are **documentation examples**, not the actual deployment IP addresses.

| Role | Example endpoint |
|---|---|
| Origin | `198.51.100.10:443` |
| Edge cache / source bind | `192.0.2.20` |
| User receiver | `192.0.2.21:8443` |

Adapt the CLI endpoints and TLS paths, server JSON files, source-address validation constants, and routing helper's addresses and interface to your testbed. This snapshot preserves the original deployment-default structure: changing one JSON file does not update every validation constant. Keep example configurations separate from a running KOREN deployment.

After preparing the mTLS origin and receiver, use [`config/controller.example.sh`](config/controller.example.sh) as a reference for explicit endpoints and certificate paths. The receiver's workload configuration must match the controller. The experiment controller calls `/client/reset`, which resets the receiver's synthetic-user state.

```bash
python3 cache/experiment.py run --help
python3 cache/cache_api.py --config /path/to/clients.json
python3 cache/experiment_report.py --state /path/to/state --experiment-id margin-1s
```

[`systemd/ontime-cache-experiment.service.example`](systemd/ontime-cache-experiment.service.example) is a service configuration example; it is not installed automatically. The 0.5-second and 1-second margin studies use separate databases. Reusing an experiment ID with a different margin is rejected to avoid mixing configurations.

## Implementation and metric semantics

- Requests are processed serially across all users, while one origin download worker runs separately.
- A hit requires a complete, unexpired segment in the cache. Queued or active downloads remain misses.
- `prefetch_late_requests` combines requests encountering queued and active speculative jobs. It does not isolate requests arriving during an active download.
- Stale speculative jobs are checked at entry to `Fetcher.request()`. Cancellation can therefore occur before demand handling and when a new prefetch request is submitted after an ACK.
- `Cache.get()` reclaims expired entries, but this snapshot's `Cache.put()` does not call `expire()` before checking capacity. This behavior is retained to preserve consistency with the existing experiments.
- Delivery or ACK-validation failure skips the failed request's predictor update and marks the interval as an error. Cleanup can finish an active transfer and persist final metrics.
- Total origin traffic depends on completed request count. Compare origin bytes per completed request alongside latency.
- Local traffic between the KOREN-attached edge and receiver is distinct from backbone link-utilization measurements.

## Excluded deployment data

Deployment certificates, private keys, SSH configuration, experiment databases, raw CSV exports, logs, and user image data are excluded. Actual deployment IP addresses and personal paths are replaced with examples. [`SOURCE_MANIFEST.json`](SOURCE_MANIFEST.json) records source and published-file hashes and the transformations applied. The manuscript and experimental performance tables are outside the scope of this code repository.

The existing paper description is preserved above. No license file has been added without the repository owner's choice.
