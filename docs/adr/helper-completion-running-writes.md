# Helper completion running-update writes

Status: accepted. Extends [Helper completion current projection](helper-completion-current-projection.md).

## Decision

A live running update rewrites the whole journal only when it changes the helper's current-state marker: the marker is absent, or it still names a terminal. A running update that finds a marker already naming no terminal advances the projection revision and the marker revision in memory and writes nothing. Repeated live snapshot observations of an already-projected terminal follow the same rule. Durable writes per helper are bounded by marker transitions, not by update count.

Recovery watermark comparison, the running marker, terminal replacement and eviction reporting are unchanged.

## Limits

The projection revision is read only in-process, so an in-memory-only advance does not need to survive a restart. A restart reloads the last written revision, and the persisted null marker still stops history from presenting an older terminal as current. The next write of any kind persists the current revisions. A running update that does not write cannot record failed-write coverage uncertainty on its own; the next write does.
