"""
net.codestorm.spacedirsync
==========================
Maubot plugin that mirrors a remote Matrix server's public room directory
into a local space.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from mautrix.types import EventType
from mautrix.errors import MatrixRequestError, MNotFound
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

from maubot import Plugin, MessageEvent
from maubot.handlers import command


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("space_room_id")
        helper.copy("directory.server")
        helper.copy("directory.page_limit")
        helper.copy("directory.search_term")
        helper.copy("directory.page_delay")
        helper.copy("via_servers")
        helper.copy("schedule_interval_seconds")
        helper.copy("run_on_start")
        helper.copy("scheduled_dry_run")
        helper.copy("api_call_delay")
        helper.copy("admins")
        helper.copy("control_room")
        helper.copy("notify_on")
        helper.copy("max_changes_per_sync")
        helper.copy("abort_on_empty_directory")


class SyncResult:
    """Plain container for what a sync did (or would have done)."""

    def __init__(self) -> None:
        self.fetched: int = 0
        self.current: int = 0
        self.added: list[str] = []
        self.removed: list[str] = []
        self.add_failures: list[tuple[str, str]] = []
        self.remove_failures: list[tuple[str, str]] = []
        self.aborted_reason: str | None = None
        self.dry_run: bool = False
        self.duration_seconds: float = 0.0

    @property
    def changed(self) -> int:
        return len(self.added) + len(self.removed)

    def short_summary(self) -> str:
        if self.aborted_reason:
            return f"aborted: {self.aborted_reason}"
        prefix = "[dry-run] " if self.dry_run else ""
        return (
            f"{prefix}fetched {self.fetched} upstream / "
            f"{self.current} in space → "
            f"+{len(self.added)} -{len(self.removed)} "
            f"({len(self.add_failures)} add fails, "
            f"{len(self.remove_failures)} remove fails) "
            f"in {self.duration_seconds:.1f}s"
        )


class SpaceDirSyncBot(Plugin):
    config: Config
    _sync_task: asyncio.Task | None
    _sync_lock: asyncio.Lock
    _last_result: SyncResult | None
    _last_run_at: float | None

    @classmethod
    def get_config_class(cls) -> type[BaseProxyConfig]:
        return Config

    async def start(self) -> None:
        await super().start()
        self.config.load_and_update()
        self._sync_task = None
        self._sync_lock = asyncio.Lock()
        self._last_result = None
        self._last_run_at = None

        interval = int(self.config["schedule_interval_seconds"] or 0)
        if interval > 0:
            self._sync_task = asyncio.create_task(self._scheduler_loop())
            self.log.info(
                "Scheduled sync enabled: every %d seconds (run_on_start=%s)",
                interval, self.config["run_on_start"],
            )
        else:
            self.log.info("Scheduled sync disabled (interval=0); use commands only.")

    async def stop(self) -> None:
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        await super().stop()

    # ------------------------------------------------------------------
    # Scheduler
    # ------------------------------------------------------------------
    async def _scheduler_loop(self) -> None:
        interval = int(self.config["schedule_interval_seconds"])
        if self.config["run_on_start"]:
            # Small delay so we don't fire during plugin startup races.
            await asyncio.sleep(15)
            await self._safe_scheduled_sync()
        while True:
            try:
                await asyncio.sleep(interval)
                await self._safe_scheduled_sync()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Scheduler loop iteration failed")

    async def _safe_scheduled_sync(self) -> None:
        try:
            result = await self.run_sync(dry_run=bool(self.config["scheduled_dry_run"]))
        except Exception as e:
            self.log.exception("Scheduled sync failed")
            if self._should_notify(outcome="exception"):
                await self._post_to_control_room(f"Scheduled sync FAILED: {e!r}")
            return

        if result.aborted_reason:
            outcome = "aborted"
        elif result.changed > 0:
            outcome = "changed"
        else:
            outcome = "noop"

        if self._should_notify(outcome=outcome):
            await self._post_to_control_room(
                f"Scheduled sync: {result.short_summary()}"
            )

    def _should_notify(self, *, outcome: str) -> bool:
        """Decide whether a scheduled-run outcome warrants a control-room post.

        outcome is one of: "noop", "changed", "aborted", "exception".
        Policy from config["notify_on"]:
          always   - all four
          changes  - changed + aborted + exception
          failures - aborted + exception
          never    - none
        Unknown values fall back to "changes" (and we log it once per call).
        """
        policy = (self.config["notify_on"] or "changes").strip().lower()
        if policy not in ("always", "changes", "failures", "never"):
            self.log.warning(
                "Unknown notify_on=%r in config; falling back to 'changes'", policy
            )
            policy = "changes"

        if policy == "never":
            return False
        if policy == "always":
            return True
        if policy == "failures":
            return outcome in ("aborted", "exception")
        # changes
        return outcome in ("changed", "aborted", "exception")

    async def _post_to_control_room(self, text: str) -> None:
        room = self.config["control_room"]
        if not room:
            return
        try:
            await self.client.send_notice(room, text)
        except Exception:
            self.log.exception("Failed to post to control room %s", room)

    # ------------------------------------------------------------------
    # Core sync
    # ------------------------------------------------------------------
    async def run_sync(self, *, dry_run: bool) -> SyncResult:
        """Fetch the upstream directory and reconcile the configured space.

        Serialised via `_sync_lock` so concurrent triggers (scheduled +
        command) can't fight each other.
        """
        async with self._sync_lock:
            start = time.monotonic()
            result = SyncResult()
            result.dry_run = dry_run

            space_id = self.config["space_room_id"]
            via_servers = list(self.config["via_servers"] or [])

            # 1. Pull upstream directory.
            try:
                upstream_room_ids = await self._fetch_directory()
            except Exception as e:
                self.log.exception("Directory fetch failed")
                result.aborted_reason = f"directory fetch failed: {e!r}"
                result.duration_seconds = time.monotonic() - start
                self._last_result = result
                self._last_run_at = time.time()
                return result

            result.fetched = len(upstream_room_ids)

            if (
                result.fetched == 0
                and self.config["abort_on_empty_directory"]
            ):
                result.aborted_reason = (
                    "upstream directory returned 0 rooms; refusing to wipe space"
                )
                result.duration_seconds = time.monotonic() - start
                self._last_result = result
                self._last_run_at = time.time()
                self.log.warning(result.aborted_reason)
                return result

            # 2. Pull current space children from the homeserver state.
            try:
                current_room_ids = await self._fetch_space_children(space_id)
            except Exception as e:
                self.log.exception("Space state fetch failed")
                result.aborted_reason = f"space state fetch failed: {e!r}"
                result.duration_seconds = time.monotonic() - start
                self._last_result = result
                self._last_run_at = time.time()
                return result

            result.current = len(current_room_ids)

            desired = set(upstream_room_ids)
            current = set(current_room_ids)

            # Never try to add the space to itself.
            desired.discard(space_id)

            to_add = sorted(desired - current)
            to_remove = sorted(current - desired)

            # 3. Safety cap.
            cap = int(self.config["max_changes_per_sync"] or 0)
            if cap > 0 and (len(to_add) + len(to_remove)) > cap:
                result.aborted_reason = (
                    f"would change {len(to_add) + len(to_remove)} rooms, "
                    f"exceeds max_changes_per_sync={cap}"
                )
                result.duration_seconds = time.monotonic() - start
                self._last_result = result
                self._last_run_at = time.time()
                self.log.warning(result.aborted_reason)
                return result

            self.log.info(
                "Sync plan: +%d -%d (upstream=%d current=%d) dry_run=%s",
                len(to_add), len(to_remove),
                result.fetched, result.current, dry_run,
            )

            api_delay = float(self.config["api_call_delay"] or 0)

            # 4. Apply additions.
            for room_id in to_add:
                if dry_run:
                    result.added.append(room_id)
                    continue
                try:
                    await self._space_child_set(space_id, room_id, via_servers)
                    result.added.append(room_id)
                except Exception as e:
                    self.log.warning("Failed to add %s: %r", room_id, e)
                    result.add_failures.append((room_id, repr(e)))
                if api_delay:
                    await asyncio.sleep(api_delay)

            # 5. Apply removals.
            for room_id in to_remove:
                if dry_run:
                    result.removed.append(room_id)
                    continue
                try:
                    await self._space_child_unset(space_id, room_id)
                    result.removed.append(room_id)
                except Exception as e:
                    self.log.warning("Failed to remove %s: %r", room_id, e)
                    result.remove_failures.append((room_id, repr(e)))
                if api_delay:
                    await asyncio.sleep(api_delay)

            result.duration_seconds = time.monotonic() - start
            self._last_result = result
            self._last_run_at = time.time()
            self.log.info("Sync done: %s", result.short_summary())
            return result

    # ------------------------------------------------------------------
    # Matrix API helpers
    # ------------------------------------------------------------------
    async def _fetch_directory(self) -> list[str]:
        """Page through the public rooms directory and return room IDs.

        """
        server = (self.config["directory.server"] or "").strip()
        limit = int(self.config["directory.page_limit"] or 50)
        search_term = self.config["directory.search_term"] or ""
        page_delay = float(self.config["directory.page_delay"] or 0)

        room_ids: list[str] = []
        seen: set[str] = set()
        next_batch: str | None = None
        page = 0

        while True:
            page += 1
            body: dict[str, Any] = {
                "limit": limit,
                "filter": {
                    "generic_search_term": search_term,
                    "room_types": [None],
                },
            }
            if next_batch:
                body["since"] = next_batch

            query = {"server": server} if server else None

            try:
                resp = await self.client.api.request(
                    "POST",
                    "/_matrix/client/v3/publicRooms",
                    content=body,
                    query_params=query,
                )
            except MatrixRequestError:
                self.log.exception("publicRooms page %d failed", page)
                raise

            chunk = resp.get("chunk") or []
            for entry in chunk:
                rid = entry.get("room_id")
                if rid and rid not in seen:
                    seen.add(rid)
                    room_ids.append(rid)

            self.log.debug(
                "directory page %d: got %d rooms (total %d)",
                page, len(chunk), len(room_ids),
            )

            next_batch = resp.get("next_batch")
            if not next_batch:
                break

            if page_delay:
                await asyncio.sleep(page_delay)

        return room_ids

    async def _fetch_space_children(self, space_id: str) -> list[str]:
        """Return room IDs currently linked from the space via m.space.child.

        """
        try:
            state = await self.client.get_state(space_id)
        except MNotFound:
            return []

        children: list[str] = []
        for evt in state:
            if evt.type != EventType.SPACE_CHILD:
                continue
            content = evt.content
            via = None
            if hasattr(content, "via"):
                via = getattr(content, "via", None)
            elif isinstance(content, dict):
                via = content.get("via")
            if via:
                children.append(evt.state_key)
        return children

    async def _space_child_set(
        self, space_id: str, room_id: str, via: list[str]
    ) -> None:
        await self.client.send_state_event(
            space_id,
            EventType.SPACE_CHILD,
            {"via": via},
            state_key=room_id,
        )

    async def _space_child_unset(self, space_id: str, room_id: str) -> None:
        # Per the spec (client-server-api §m.space.child): "Child rooms can be
        # removed from a space by omitting the via key of content on the
        # relevant state event, such as through redaction or otherwise
        # clearing the content." Sending {} (rather than {"via": []}) is the
        # spec-canonical form and matches what Element Web emits. Notably,
        # element-web#29606 documents an Element bug where {"via": []} leaves
        # the room visible in the sidebar — another reason to prefer {}.
        await self.client.send_state_event(
            space_id,
            EventType.SPACE_CHILD,
            {},
            state_key=room_id,
        )

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------
    def _is_admin(self, mxid: str) -> bool:
        admins = self.config["admins"] or []
        return mxid in admins

    async def _gate(self, evt: MessageEvent) -> bool:
        if not self._is_admin(evt.sender):
            await evt.reply("Not authorised.")
            return False
        return True

    @command.new(name="spacedirsync", aliases=["sds"], help="Mirror a public room directory into a space.")
    async def spacedirsync(self, evt: MessageEvent) -> None:
        # Help when called bare.
        await evt.reply(
            "Subcommands: `sync`, `dryrun`, `status`, `config`. "
            "Use `!spacedirsync <subcommand>`."
        )

    @spacedirsync.subcommand("sync", help="Run a full sync now.")
    async def cmd_sync(self, evt: MessageEvent) -> None:
        if not await self._gate(evt):
            return
        await evt.reply("Starting sync…")
        try:
            result = await self.run_sync(dry_run=False)
        except Exception as e:
            await evt.reply(f"Sync failed: {e!r}")
            return
        await evt.reply(self._format_result(result))

    @spacedirsync.subcommand("dryrun", help="Show what a sync would change, without applying.")
    async def cmd_dryrun(self, evt: MessageEvent) -> None:
        if not await self._gate(evt):
            return
        await evt.reply("Starting dry run…")
        try:
            result = await self.run_sync(dry_run=True)
        except Exception as e:
            await evt.reply(f"Dry run failed: {e!r}")
            return
        await evt.reply(self._format_result(result))

    @spacedirsync.subcommand("status", help="Show last run summary and config snapshot.")
    async def cmd_status(self, evt: MessageEvent) -> None:
        if not await self._gate(evt):
            return
        interval = int(self.config["schedule_interval_seconds"] or 0)
        last = self._last_result
        last_run = (
            time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(self._last_run_at))
            if self._last_run_at else "never"
        )
        lines = [
            "**spacedirsync status**",
            f"- space: `{self.config['space_room_id']}`",
            f"- upstream server: `{self.config['directory.server'] or '(local)'}`",
            f"- via: `{', '.join(self.config['via_servers'] or [])}`",
            f"- schedule: {'every ' + str(interval) + 's' if interval else 'disabled'}",
            f"- last run: {last_run}",
            f"- last result: {last.short_summary() if last else 'none yet'}",
        ]
        await evt.reply("\n".join(lines))

    @spacedirsync.subcommand("config", help="Echo the current effective config (sensitive fields excluded).")
    async def cmd_config(self, evt: MessageEvent) -> None:
        if not await self._gate(evt):
            return
        cfg = {
            "space_room_id": self.config["space_room_id"],
            "directory": {
                "server": self.config["directory.server"],
                "page_limit": self.config["directory.page_limit"],
                "search_term": self.config["directory.search_term"],
                "page_delay": self.config["directory.page_delay"],
            },
            "via_servers": self.config["via_servers"],
            "schedule_interval_seconds": self.config["schedule_interval_seconds"],
            "run_on_start": self.config["run_on_start"],
            "scheduled_dry_run": self.config["scheduled_dry_run"],
            "api_call_delay": self.config["api_call_delay"],
            "max_changes_per_sync": self.config["max_changes_per_sync"],
            "abort_on_empty_directory": self.config["abort_on_empty_directory"],
            "control_room": self.config["control_room"] or "(disabled)",
            "notify_on": self.config["notify_on"],
            "admins": self.config["admins"],
        }
        await evt.reply("```\n" + json.dumps(cfg, indent=2) + "\n```")

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------
    @staticmethod
    def _format_result(result: SyncResult) -> str:
        if result.aborted_reason:
            return f"❌ Aborted: {result.aborted_reason}"

        prefix = "🔍 Dry run" if result.dry_run else "✅ Sync"
        lines = [
            f"{prefix} complete in {result.duration_seconds:.1f}s",
            f"- upstream rooms: **{result.fetched}**",
            f"- current children: **{result.current}**",
            f"- {'would add' if result.dry_run else 'added'}: **{len(result.added)}**",
            f"- {'would remove' if result.dry_run else 'removed'}: **{len(result.removed)}**",
        ]
        if result.add_failures:
            lines.append(f"- add failures: **{len(result.add_failures)}**")
        if result.remove_failures:
            lines.append(f"- remove failures: **{len(result.remove_failures)}**")

        # Show small diff samples to make the command output useful.
        def sample(items: list, n: int = 5) -> str:
            head = items[:n]
            tail = f" … (+{len(items) - n} more)" if len(items) > n else ""
            return ", ".join(f"`{x}`" for x in head) + tail

        if result.added:
            lines.append(f"  - {'+' if not result.dry_run else '~'} {sample(result.added)}")
        if result.removed:
            lines.append(f"  - {'-' if not result.dry_run else '~'} {sample(result.removed)}")
        return "\n".join(lines)
