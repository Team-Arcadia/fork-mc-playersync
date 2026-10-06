package vip.fubuki.playersync.sync;

import vip.fubuki.playersync.util.SyncLogger;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import net.minecraft.ChatFormatting;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Holder;
import net.minecraft.core.HolderLookup;
import net.minecraft.core.component.DataComponents;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtUtils;
import net.minecraft.nbt.Tag;
import net.minecraft.nbt.TagParser;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.Style;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.PlayerAdvancements;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.effect.MobEffect;
import net.minecraft.world.effect.MobEffectInstance;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.item.Items;
import net.minecraft.world.item.component.CustomData;
import net.minecraft.world.item.component.ItemLore;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.storage.WorldData;
import net.neoforged.bus.api.SubscribeEvent;
import net.neoforged.fml.ModList;
import net.neoforged.fml.common.EventBusSubscriber;
import net.neoforged.neoforge.event.entity.living.LivingDeathEvent;
import net.neoforged.neoforge.event.entity.player.PlayerEvent;
import net.neoforged.neoforge.event.entity.player.PlayerNegotiationEvent;
import net.neoforged.neoforge.event.server.ServerStoppingEvent;
import net.neoforged.neoforge.event.tick.ServerTickEvent;
import net.neoforged.neoforge.server.ServerLifecycleHooks;
import vip.fubuki.playersync.PlayerSync;
import vip.fubuki.playersync.config.JdbcConfig;
import vip.fubuki.playersync.sync.addons.ModCompatSync;
import vip.fubuki.playersync.sync.addons.ModsSupport;
import vip.fubuki.playersync.util.JDBCsetUp;
import vip.fubuki.playersync.util.LocalJsonUtil;
import vip.fubuki.playersync.util.PSThreadPoolFactory;
import vip.fubuki.playersync.util.Tables;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

@EventBusSubscriber(modid = PlayerSync.MODID)
public class VanillaSync {

    public static void register() {}

    // FIX: Replace unbounded CachedThreadPool with a bounded ThreadPoolExecutor.
    // CachedThreadPool creates unlimited threads — with many players and slow DB queries,
    // thread count can explode to 25000+ causing memory leaks and server crashes.
    //
    // AUDIT FIX (pool sizing): standard ThreadPoolExecutor semantics only create threads
    // beyond corePoolSize when the work queue is FULL. The previous 4..16 config was
    // therefore permanently pinned at 4 threads (the queue would need 512 backlogged
    // tasks before thread #5 appeared) — every save path funneled through 4 workers
    // and the "35 parallel shutdown saves" assumption was wrong (only 4 ran at once).
    // core == max == 16 + allowCoreThreadTimeOut(true) gives real 16-wide parallelism
    // while still reaping idle threads after 30s.
    //
    // AUDIT FIX (rejection policy): CallerRunsPolicy ran the rejected task inline on
    // the SUBMITTING thread. For tasks submitted from the server main thread
    // (onServerTick, snapshotAndQueueSave, onPlayerJoin) that meant full DB
    // transactions — or doPlayerJoin's up-to-60s poll loop — executing ON the main
    // thread under queue overflow: a multi-second to multi-minute freeze.
    // MainThreadSafeBackpressurePolicy keeps CallerRuns backpressure for background
    // submitters but hands main-thread overflow to a dedicated single-thread overflow
    // executor so the tick thread NEVER blocks and no save is ever dropped.
    static ExecutorService executorService = new ThreadPoolExecutor(
            16,                         // core pool size (== max: see AUDIT FIX above)
            16,                         // maximum pool size
            30L, TimeUnit.SECONDS,      // idle thread keepalive (applies to core threads too)
            new LinkedBlockingQueue<>(512),  // bounded work queue
            new PSThreadPoolFactory("PlayerSync"),
            new MainThreadSafeBackpressurePolicy()
    );

    static {
        ((ThreadPoolExecutor) executorService).allowCoreThreadTimeOut(true);
    }

    /**
     * 2.2.0: join tasks get their own threads. A join spends most of its life waiting: on
     * the previous session's logout save (up to 15 s) and on the peer server's poll (up to a
     * minute). Sharing the save pool meant a wave of server transfers could put all 16 save
     * workers to sleep, and the logout saves those joins were waiting for then sat in the
     * queue behind them until the 15 s wait gave up and the join read stale data. Nothing
     * here ever runs on the main thread, so the queue is unbounded: joins are bounded by
     * the number of players connecting.
     */
    private static final ThreadPoolExecutor joinExecutor = new ThreadPoolExecutor(
            16, 16, 30L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new PSThreadPoolFactory("PlayerSync-join"));

    static {
        joinExecutor.allowCoreThreadTimeOut(true);
    }

    /**
     * Overflow lane for tasks rejected while being submitted FROM the server main
     * thread. Unbounded queue, single thread: absorbs rare overflow bursts without
     * ever blocking a tick. Never used by background submitters (they get classic
     * CallerRuns backpressure instead).
     */
    private static final ExecutorService overflowExecutor =
            Executors.newSingleThreadExecutor(new PSThreadPoolFactory("PlayerSync-overflow"));

    /**
     * AUDIT FIX: rejection policy that never executes blocking work on the server
     * main thread.
     * <ul>
     *   <li>Executor shut down → silently drop (same semantics as CallerRunsPolicy
     *       during shutdown; the shutdown-save path has already flushed players).</li>
     *   <li>Submitted from the server main thread → run on {@link #overflowExecutor}
     *       (never inline — a queued save or a join poll must not stall ticks).</li>
     *   <li>Submitted from any other thread → run inline (classic CallerRuns
     *       backpressure; dropping saves would mean data loss).</li>
     * </ul>
     */
    private static final class MainThreadSafeBackpressurePolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (executor.isShutdown()) return;
            MinecraftServer srv = ServerLifecycleHooks.getCurrentServer();
            if (srv != null && Thread.currentThread() == srv.getRunningThread()) {
                PlayerSync.LOGGER.warn("[executor] queue full ({} tasks) — diverting main-thread submission to overflow lane",
                        executor.getQueue().size());
                overflowExecutor.execute(r);
            } else {
                r.run();
            }
        }
    }

    // Per-player locks to prevent concurrent save/restore operations (anti-duplication)
    private static final ConcurrentHashMap<String, ReentrantLock> playerLocks = new ConcurrentHashMap<>();

    // FIX: Track in-progress logout saves so doPlayerJoin can wait for them.
    // Without this, a fast disconnect+reconnect can read stale DB data while the
    // previous session's save is still in flight.
    private static final ConcurrentHashMap<String, CompletableFuture<Void>> pendingLogoutSaves = new ConcurrentHashMap<>();

    private static ReentrantLock getPlayerLock(String uuid) {
        return playerLocks.computeIfAbsent(uuid, k -> new ReentrantLock());
    }

    /**
     * FIX P1-3: returns true if the given peer server's heartbeat is missing or
     * older than {@code staleAfterMs}. Used by doPlayerJoin's last_server poll to
     * short-circuit when the peer is a zombie (crashed without clearing online flag,
     * or legacy server_id=0 from pre-fix DB rows).
     */
    private static boolean isPeerServerStale(int peerServerId, long staleAfterMs) {
        if (peerServerId == 0) return true; // 0 is never a legitimate SERVER_ID
        try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(
                "SELECT last_update FROM " + Tables.serverInfo() + " WHERE id=?", peerServerId)) {
            ResultSet rs = qr.resultSet();
            if (!rs.next()) return true; // no heartbeat row => dead
            long lastUpdate = rs.getLong("last_update");
            long age = System.currentTimeMillis() - lastUpdate;
            return age > staleAfterMs;
        } catch (Exception e) {
            PlayerSync.LOGGER.warn("isPeerServerStale query failed for server {}: {}", peerServerId, e.getMessage());
            return false; // err on the side of waiting
        }
    }

    /** Admin-command accessor for the shared executor — read-only usage. */
    public static ThreadPoolExecutor getExecutor() {
        return (ThreadPoolExecutor) executorService;
    }

    public static void removePlayerLock(String uuid) {
        playerLocks.remove(uuid);
        lastWrittenSnapshotHash.remove(uuid);
        // AUDIT FIX (memory): evict the player's advancements cache entry. The cache
        // key is the absolute file path which always contains the player UUID
        // ("/advancements/<uuid>.json"), so contains() matches exactly one entry.
        // Without this the map retained the FULL advancements JSON (100KB-1MB+ on
        // large modpacks) for every player that ever joined — unbounded growth on
        // long-running hub servers. Cost of eviction: one disk re-read on the
        // player's next-session first snapshot (mtime check guarantees freshness).
        advancementsFileCache.keySet().removeIf(k -> k.contains(uuid));
        lastWrittenAdvancements.remove(uuid);
        // 2.1.6: release the container-storage handles established for this session
        // (see StorageOwnership) together with their write-skip hashes. Keeping them
        // would both leak memory on a hub server and let a stale hash suppress a write
        // after another server changed the same blob.
        ModsSupport.releaseSession(uuid);
    }

    /**
     * FIX DUP-REVIVE: returns true if the player is currently in the Revive Me mod's
     * "fallen" (downed) state — the "dead but still alive" phase shown after lethal
     * damage when Revive Me intercepts the death.
     *
     * <p>Queried reflectively against Revive Me's own state
     * ({@code invoker54.reviveme.common.capability.FallenData.get(player).isFallen()})
     * so PlayerSync keeps a soft (optional) dependency on the mod. The FallenData is a
     * NeoForge AttachmentType ({@code revive_me:fallen_data}); it is serialized into
     * the player's .dat file, so on a SAME-server reconnect the entity is already
     * carrying the fallen flag by the time {@link #doPlayerJoin} runs.
     *
     * <p>This is an EXACT signal — no heuristics, no false positives. A player is
     * fallen if and only if Revive Me says so.
     *
     * @return true only if the {@code revive_me} mod is loaded AND the player is
     *         currently fallen; false on any error or if the mod is absent.
     */
    // AUDIT FIX (perf): lazily-cached reflection handles for ReviveMe's FallenData.
    // Previously every call resolved Class.forName + 2× getMethod (each getMethod
    // returns a fresh Method copy — allocation + security check) and the function
    // runs on every logout, every join apply, and was re-invoked inside log
    // statements. Resolve once, publish via volatile (benign race), reuse forever.
    private static volatile java.lang.reflect.Method REVIVEME_FALLEN_GET;
    private static volatile java.lang.reflect.Method REVIVEME_IS_FALLEN;
    private static volatile boolean reviveMeReflectionFailed = false;

    public static boolean isReviveMeFallen(net.minecraft.world.entity.player.Player player) {
        if (player == null) return false;
        if (reviveMeReflectionFailed) return false;
        if (!ModList.get().isLoaded("revive_me")) return false;
        try {
            java.lang.reflect.Method getMethod = REVIVEME_FALLEN_GET;
            java.lang.reflect.Method isFallenMethod = REVIVEME_IS_FALLEN;
            if (getMethod == null || isFallenMethod == null) {
                Class<?> fallenDataClass = Class.forName("invoker54.reviveme.common.capability.FallenData");
                getMethod = fallenDataClass.getMethod("get", net.minecraft.world.entity.LivingEntity.class);
                isFallenMethod = fallenDataClass.getMethod("isFallen");
                REVIVEME_FALLEN_GET = getMethod;
                REVIVEME_IS_FALLEN = isFallenMethod;
            }
            Object fallenData = getMethod.invoke(null, player);
            if (fallenData == null) return false;
            Object result = isFallenMethod.invoke(fallenData);
            return result instanceof Boolean b && b;
        } catch (Throwable t) {
            // Resolution failure is permanent (class/method shape mismatch) — stop
            // retrying the reflection on every event. Invocation failures on a
            // resolved handle are unexpected; treat identically (fail-safe false).
            if (REVIVEME_FALLEN_GET == null || REVIVEME_IS_FALLEN == null) {
                reviveMeReflectionFailed = true;
                PlayerSync.LOGGER.warn("[revive-detect] ReviveMe FallenData reflection unavailable — fallen detection disabled: {}", t.toString());
            } else {
                PlayerSync.LOGGER.debug("[revive-detect] could not query ReviveMe FallenData for {}: {}",
                        player.getUUID(), t.toString());
            }
            return false;
        }
    }

    /**
     * PHASE 17 PERF: advancements JSON cache keyed by absolute file path.
     * Keeps the mtime along with the content — a mismatch on either forces a
     * fresh disk read. The cache is process-wide (not per-player) because the
     * path already includes the player UUID.
     */
    private static final ConcurrentHashMap<String, AdvancementsCacheEntry> advancementsFileCache = new ConcurrentHashMap<>();

    private static final class AdvancementsCacheEntry {
        final long mtime;
        final String content;
        AdvancementsCacheEntry(long mtime, String content) {
            this.mtime = mtime;
            this.content = content;
        }
    }

    /**
     * PHASE 7 PERF: per-player hash of the last successfully-written snapshot.
     * Auto-save / periodic / dimension-change BG tasks skip the DB write when
     * the new snapshot hashes identical to the last-written one — on an idle
     * server with 35 players this cuts 95%+ of redundant UPDATE traffic.
     *
     * <p>Never used by logout/shutdown/death paths: those MUST always write
     * to guarantee online=0 atomicity and capture the final state.
     */
    private static final ConcurrentHashMap<String, Integer> lastWrittenSnapshotHash = new ConcurrentHashMap<>();

    /** Cheap hash over the serialized snapshot. */
    private static int computeSnapshotHash(PlayerDataSnapshot s) {
        int h = 17;
        h = 31 * h + java.util.Objects.hashCode(s.inventory());
        h = 31 * h + java.util.Objects.hashCode(s.equipment());
        h = 31 * h + java.util.Objects.hashCode(s.enderChest());
        h = 31 * h + java.util.Objects.hashCode(s.effects());
        h = 31 * h + java.util.Objects.hashCode(s.leftHand());
        h = 31 * h + java.util.Objects.hashCode(s.cursors());
        h = 31 * h + java.util.Objects.hashCode(s.advancements());
        h = 31 * h + java.util.Objects.hashCode(s.curiosData());
        h = 31 * h + java.util.Objects.hashCode(s.accessoriesData());
        h = 31 * h + java.util.Objects.hashCode(s.cosmeticArmorData());
        h = 31 * h + java.util.Objects.hashCode(s.attachmentsData());
        h = 31 * h + java.util.Objects.hashCode(s.persistentData());
        h = 31 * h + s.xp();
        h = 31 * h + s.foodLevel();
        h = 31 * h + s.health();
        h = 31 * h + s.score();
        return h;
    }

    /**
     * Writes the stored advancements to the player's file and reloads them. MAIN THREAD:
     * snapshotPlayerData reads the same file on the main thread and reload() mutates
     * PlayerAdvancements.
     *
     * <p>2.2.0: called from the join restore, after the claim (see doPlayerJoin), instead of
     * from OnDatapackSyncEvent at login. Also skips the write and the reload (5-50 ms on a
     * large datapack) when the stored blob is byte-identical to the local file, the usual
     * case when a player rejoins the server they just left.
     */
    private static void applyAdvancementsFromData(MinecraftServer server, ServerPlayer serverPlayer,
                                                  String player_uuid, String advancementsData) {
        if (!JdbcConfig.SYNC_ADVANCEMENTS.get()) return;
        if (advancementsData == null || advancementsData.length() < 2) {
            PlayerSync.LOGGER.debug("Skip writing advancements for player {} (empty data)", player_uuid);
            return;
        }
        try {
            final byte[] bytes = advancementsData.getBytes(StandardCharsets.UTF_8);
            Path path = server.getServerDirectory().resolve(getSyncWorldForServer());
            File gameDir = path.toFile();

            if (server.isDedicatedServer()) {
                File advancements = new File(gameDir, "/advancements" + "/" + player_uuid + ".json");
                if (advancements.isFile() && java.util.Arrays.equals(Files.readAllBytes(advancements.toPath()), bytes)) {
                    PlayerSync.LOGGER.debug("Advancements of {} already match the stored copy — no reload", player_uuid);
                    return;
                }

                File advancementsDir = advancements.getParentFile();
                if (advancementsDir != null && !advancementsDir.exists()) {
                    PlayerSync.LOGGER.info("Creating advancements directory {}", advancementsDir.getPath());
                    if (!advancementsDir.mkdirs()) {
                        PlayerSync.LOGGER.error("Aborting advancements sync. Failed to create advancements directory at {}", advancementsDir.getPath());
                        return;
                    }
                }
                PlayerSync.LOGGER.debug("Writing advancement file {} for player {}", advancements.toPath(), player_uuid);
                Files.write(advancements.toPath(), bytes);

                // reload the JSON files on the server after updating them
                PlayerAdvancements playeradvancements = serverPlayer.getAdvancements();
                playeradvancements.reload(server.getAdvancements());
            } else {
                PlayerSync.LOGGER.debug("Writing non-dedicated server advancement files");
                File[] files = scanAdvancementsFile(player_uuid, gameDir);
                for (File file : files) {
                    if (file == null)
                        continue;
                    Files.write(file.toPath(), bytes);
                }
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error applying advancements for player {}", player_uuid, e);
        }
    }

    public static void doPlayerConnect(PlayerNegotiationEvent event) {
        try {
            String player_uuid = event.getProfile().getId().toString();
            PlayerSync.LOGGER.info("Detected connection from player {}, starting checking", player_uuid);
            boolean online;
            int lastServer;

            // First query: check basic player data using prepared statement
            try (JDBCsetUp.QueryResult qr1 = JDBCsetUp.executePreparedQuery(
                    "SELECT online, last_server FROM " + Tables.playerData() + " WHERE uuid=?", player_uuid)) {
                ResultSet rs1 = qr1.resultSet();
                if (!rs1.next()) {
                    PlayerSync.LOGGER.info("A new-player connection detected");
                    connectCheckCache.put(player_uuid, new CachedConnectCheck(new int[]{0, 0, 0, 0}, System.currentTimeMillis())); // new player
                    return;
                }
                online = rs1.getBoolean("online");
                lastServer = rs1.getInt("last_server");
            }

            // Second query: Check if player is already online on another server
            int serverAlive = 0;
            int alreadyKicked = 0;
            if (JdbcConfig.KICK_WHEN_ALREADY_ONLINE.get() && online && lastServer != JdbcConfig.SERVER_ID.get()) {
                try (JDBCsetUp.QueryResult qr2 = JDBCsetUp.executePreparedQuery(
                        "SELECT last_update, enable FROM " + Tables.serverInfo() + " WHERE id=?", lastServer)) {
                    ResultSet rs2 = qr2.resultSet();
                    if (rs2.next()) {
                        long last_update = rs2.getLong("last_update");
                        boolean enable = rs2.getBoolean("enable");
                        if (enable && System.currentTimeMillis() < last_update + 300000L) {
                            serverAlive = 1;
                            event.getConnection().disconnect(Component.translatableWithFallback("playersync.already_online","You can't join more than one synchronization server at the same time."));
                            alreadyKicked = 1;
                        } else {
                            JDBCsetUp.executePreparedUpdate("UPDATE " + Tables.serverInfo() + " SET enable=0 WHERE id=?", lastServer);
                        }
                    }
                }
            }

            // FIX PERF: Cache the result for onPlayerLoggedInKickCheck (avoids re-querying on main thread)
            connectCheckCache.put(player_uuid, new CachedConnectCheck(
                    new int[]{online ? 1 : 0, lastServer, serverAlive, alreadyKicked},
                    System.currentTimeMillis()));
        } catch (Exception e) {
            PlayerSync.LOGGER.error("SqlException detected!", e);
            event.getConnection().disconnect(Component.translatableWithFallback("playersync.sqlexception","SqlException detected!Connection lost,please contact with your admin."));
        }
    }

    // Use string uuid as key
    public static Set<String> deadPlayerWhileLogging = ConcurrentHashMap.newKeySet();
    public static Set<String> syncNotCompletedPlayer = ConcurrentHashMap.newKeySet();

    /**
     * 2.2.0: identity of the login a background join task is working for. A fresh token is
     * installed on the main thread when PlayerLoggedInEvent fires and removed when that
     * player logs out. The join task polls for minutes in the worst case, so it must be able
     * to tell "my player is still here" from "my player left" or "my player left and came
     * back as a new session": the uuid alone cannot, and acting for a departed session is
     * how a claim used to be left behind (online=1 on a server the player had already left).
     */
    private static final ConcurrentHashMap<String, Object> joinSessions = new ConcurrentHashMap<>();

    private static boolean isJoinSessionCurrent(String uuid, Object token) {
        return token != null && joinSessions.get(uuid) == token;
    }

    /**
     * Ends the "sync pending" state of a join, but only for the session that started it. A
     * departed session's logout already cleared the flag, and a newer session owns its own
     * entry: clearing it from an old task would let the new session save before its restore.
     */
    private static void finishJoin(String uuid, Object token) {
        if (isJoinSessionCurrent(uuid, token)) syncNotCompletedPlayer.remove(uuid);
    }

    /**
     * Kicks the joining player when their row says they are online on another server whose
     * heartbeat is fresh — the {@code kick_when_already_online} rule, with the same decision
     * the main-thread check used to take. A peer whose heartbeat is stale is marked disabled
     * instead. Runs on the join thread; the kick itself hops to the main thread.
     *
     * @return true when the player is being kicked and the join must stop here
     */
    private static boolean checkAlreadyOnlineElsewhere(MinecraftServer server, ServerPlayer serverPlayer,
                                                       String player_uuid, Object session) {
        try {
            int lastServer = onlineElsewhere(player_uuid);
            if (lastServer == 0) return false;
            // 2.2.0: kick_grace_period_ms was documented ("lets the legitimate session
            // re-establish") but never read. A player switching servers usually arrives
            // while the previous server is still committing their logout: give it the grace
            // period, then look again before calling it a duplicate login.
            int graceMs = JdbcConfig.KICK_GRACE_PERIOD_MS.get();
            if (graceMs > 0) {
                Thread.sleep(graceMs);
                if (!isJoinSessionCurrent(player_uuid, session)) return true; // gone; nothing left to do
                lastServer = onlineElsewhere(player_uuid);
                if (lastServer == 0) return false;
            }
            boolean peerAlive = false;
            boolean peerKnown = false;
            try (JDBCsetUp.QueryResult qr2 = JDBCsetUp.executePreparedQuery(
                    "SELECT last_update, enable FROM " + Tables.serverInfo() + " WHERE id=?", lastServer)) {
                ResultSet rs2 = qr2.resultSet();
                if (rs2.next()) {
                    peerKnown = true;
                    peerAlive = rs2.getBoolean("enable") && System.currentTimeMillis() < rs2.getLong("last_update") + 300000L;
                }
            }
            if (!peerAlive) {
                if (peerKnown) {
                    JDBCsetUp.executePreparedUpdate("UPDATE " + Tables.serverInfo() + " SET enable=0 WHERE id=?", lastServer);
                }
                return false;
            }
            final int peer = lastServer;
            server.execute(() -> {
                if (!isJoinSessionCurrent(player_uuid, session)) return;
                kickedForDuplicateLogin.add(player_uuid);
                PlayerSync.LOGGER.warn("Kicking player {} - already online on server {}", player_uuid, peer);
                serverPlayer.connection.disconnect(Component.translatableWithFallback(
                        "playersync.already_online",
                        "You can't join more than one synchronization server at the same time."));
            });
            return true;
        } catch (Exception e) {
            // Same as before: a failed check does not block the login. The claim protocol
            // below still refuses to take a row another server holds.
            PlayerSync.LOGGER.error("Error during kick check for player {}", player_uuid, e);
            return false;
        }
    }

    /**
     * @return the id of the other server the row says the player is online on, or 0 when
     *         the row is free, ours, or that server is visibly finishing the player's logout
     *         save (a server switch in progress, which the join poll waits for — not a
     *         duplicate login)
     */
    private static int onlineElsewhere(String player_uuid) throws SQLException {
        try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(
                "SELECT online, last_server, COALESCE(logout_started_at, 0) AS lsa FROM " + Tables.playerData()
                        + " WHERE uuid=?", player_uuid)) {
            ResultSet rs = qr.resultSet();
            if (!rs.next()) return 0;
            if (!rs.getBoolean("online") || rs.getLong("lsa") > 0) return 0;
            int lastServer = rs.getInt("last_server");
            return lastServer == JdbcConfig.SERVER_ID.get() ? 0 : lastServer;
        }
    }

    /**
     * Clears the online flag of a player who leaves without a data save, on the executor.
     *
     * <p>2.2.0: guarded by {@code last_server}. The unguarded version also ran for players
     * whose join never claimed the row (they left while waiting for the previous server),
     * and so cleared a flag that belonged to another server: that server's auto-saves then
     * saw "offline" and skipped the player for the rest of the session.
     */
    private static void markOfflineIfOwned(String uuid, String what) {
        executorService.execute(() -> {
            try {
                releaseOwnFlag(uuid, false);
            } catch (SQLException e) {
                PlayerSync.LOGGER.error("Error marking {} offline: {}", what, uuid, e);
            }
        });
    }

    /**
     * Gives back a claim taken for a session that no longer exists. Only touches the row
     * while it is still ours, so it can never clear a flag another server set meanwhile.
     */
    private static void releaseOrphanedClaim(String uuid) {
        try {
            if (releaseOwnFlag(uuid, true)) {
                SyncLogger.raceCondition(uuid, "Player left during join — claim released");
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.warn("Could not release the claim of departed player {}: {}", uuid, e.getMessage());
        }
    }

    /**
     * Sets {@code online=0} on a row this server owns, for a session that has ended.
     *
     * <p>The row cannot say WHICH session of this server holds it. If the player has already
     * logged in here again, the newer session owns the flag and it must stay set: clearing it
     * would let a peer claim the player mid-session and make every periodic save of the new
     * session miss its {@code online=1} guard. Hence the check before the write, and the
     * re-assert after it for a newer login that started in between (guarded by
     * {@code last_server}, so a peer that claimed in that instant keeps its claim).
     *
     * @return true when a row was released
     */
    private static boolean releaseOwnFlag(String uuid, boolean clearLogoutMarker) throws SQLException {
        if (joinSessions.containsKey(uuid)) return false;
        int serverId = JdbcConfig.SERVER_ID.get();
        int n = JDBCsetUp.executePreparedUpdateRet(
                "UPDATE " + Tables.playerData() + " SET online=0"
                        + (clearLogoutMarker ? ", logout_started_at=NULL" : "")
                        + " WHERE uuid=? AND last_server=? AND online=1",
                uuid, serverId);
        if (n > 0 && joinSessions.containsKey(uuid)) {
            JDBCsetUp.executePreparedUpdate(
                    "UPDATE " + Tables.playerData() + " SET online=1 WHERE uuid=? AND last_server=?",
                    uuid, serverId);
            return false;
        }
        return n > 0;
    }
    // Players kicked for being already online on another server - their logout must NOT set online=0
    public static Set<String> kickedForDuplicateLogin = ConcurrentHashMap.newKeySet();

    // FIX PERF: Cache from doPlayerConnect (network thread) for onPlayerLoggedInKickCheck (main thread).
    // Eliminates 2-4 redundant DB queries per join on the main thread.
    // Entry: uuid → {online, lastServer, serverAlive, alreadyHandled}
    // int[0]=online(0/1), int[1]=lastServer, int[2]=serverAlive(0/1), int[3]=alreadyKicked(0/1)
    //
    // PERF (A7): wrapped in a record with insertion timestamp so we can purge stale
    // entries when PlayerNegotiationEvent fires but PlayerLoggedInEvent never does
    // (player drops the connection mid-handshake). The sweep runs on the existing
    // server-tick handler — no extra thread.
    private record CachedConnectCheck(int[] data, long insertedAt) {}
    private static final ConcurrentHashMap<String, CachedConnectCheck> connectCheckCache = new ConcurrentHashMap<>();
    private static final long CONNECT_CHECK_TTL_MS = 60_000L; // 60 s — generous: handshake usually completes in < 5 s
    private static long lastConnectCheckSweepTick = 0L;

    public static void doPlayerJoin(PlayerEvent.PlayerLoggedInEvent event, Object session) {
        ServerPlayer serverPlayer = (ServerPlayer) event.getEntity();
        String player_uuid = serverPlayer.getUUID().toString();
        MinecraftServer server = serverPlayer.getServer();

        if (server == null) {
            PlayerSync.LOGGER.error("Server is null for player {}", player_uuid);
            finishJoin(player_uuid, session);
            return;
        }

        // FIX: If the player entity spawned dead/dying, kick+respawn them.
        // All entity modifications (removeTag, teleport, disconnect) are scheduled on the
        // main thread — the old code called removeTag from this background thread which is unsafe.
        // FIX: ReviveMe compatibility — check if the player is in a "downed" state (not truly dead).
        // ReviveMe cancels LivingDeathEvent and puts players at low health with special effects.
        // These players have health > 0 and should NOT be kicked. Only kick if actually dead (health <= 0).
        // 2.2.0: a join task that starts late may belong to a session that already ended;
        // it must not act on that entity (nor flag the uuid for the NEXT session's logout).
        if (!isJoinSessionCurrent(player_uuid, session)) return;
        if (serverPlayer.isDeadOrDying() && serverPlayer.getHealth() <= 0) {
            server.execute(() -> {
                if (!isJoinSessionCurrent(player_uuid, session)) return;
                deadPlayerWhileLogging.add(player_uuid);
                serverPlayer.removeTag("player_synced");
                ResourceKey<Level> respawnLevel = serverPlayer.getRespawnDimension();
                BlockPos respawnPos = serverPlayer.getRespawnPosition();
                if (respawnPos != null) {
                    ServerLevel level = server.getLevel(respawnLevel);
                    if (level != null) {
                        serverPlayer.teleportTo(level, respawnPos.getX(), respawnPos.getY() + 1, respawnPos.getZ(), 0, 0);
                    }
                }
                serverPlayer.setHealth(1);
                serverPlayer.connection.disconnect(Component.translatableWithFallback("playersync.wrong_entity_status","An error occurred while creating playerEntity in the world,please login again."));
            });
            // online=1 already set by onPlayerLoggedInKickCheck — no duplicate DB write here
            return;
        }

        // kick_when_already_online, evaluated here on the join thread (see onPlayerLoggedInKickCheck).
        if (JdbcConfig.KICK_WHEN_ALREADY_ONLINE.get() && checkAlreadyOnlineElsewhere(server, serverPlayer, player_uuid, session)) {
            return;
        }

        // FIX ANTI-DUPLICATION: Wait for any pending logout save from a previous session
        // on THIS server. Without this, a fast disconnect+reconnect reads stale DB data
        // while the previous session's async save is still in flight.
        CompletableFuture<Void> pendingSave = pendingLogoutSaves.get(player_uuid);
        if (pendingSave != null) {
            PlayerSync.LOGGER.info("Waiting for pending logout save to complete for player {}", player_uuid);
            // Same 15 s budget as before, in short steps so a player who leaves in the
            // meantime frees this join thread instead of holding it for the full wait.
            final long deadline = System.currentTimeMillis() + 15_000L;
            while (true) {
                if (!isJoinSessionCurrent(player_uuid, session)) return;
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    PlayerSync.LOGGER.error("Timeout waiting for pending logout save for player {}", player_uuid);
                    break;
                }
                try {
                    pendingSave.get(Math.min(left, 500L), TimeUnit.MILLISECONDS);
                    break;
                } catch (TimeoutException e) {
                    // keep waiting
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    PlayerSync.LOGGER.warn("Pending logout save failed for player {}", player_uuid, e);
                    break;
                }
            }
        }

        ReentrantLock lock = getPlayerLock(player_uuid);
        lock.lock();
        final long restoreT0 = System.currentTimeMillis();
        try {
            PlayerSync.LOGGER.info("Starting synchronization for player {}", player_uuid);
            SyncLogger.restoreStarted(player_uuid);

            // FIX ANTI-DUPLICATION: Wait for the PREVIOUS server to finish saving this player's data.
            // The old server's writeSnapshotToDB uses AND last_server=? — once we claim last_server,
            // the old server's write is blocked. So we must wait BEFORE claiming.
            //
            // The poll checks: if last_server != this server, the old server's save may still
            // be in flight. Wait for it to set online=0 (which happens atomically with the data
            // write via the combined UPDATE). Once online=0, the data is guaranteed fresh.
            //
            // NOTE: onPlayerLoggedInKickCheck deliberately does NOT set last_server — only online=1.
            // This keeps last_server pointing to the old server so this poll can detect it.
            // FIX P1-3: raised max attempts 60→120 (30s→60s) to cover slow-shutdown peers
            // + added server_info freshness short-circuit: if the other server hasn't
            // heartbeated in >60s, treat it as dead and stop waiting immediately.
            // This fixes the user-reported "attempt 60/60" log flood for server_id=0
            // and zombie server_ids whose player_data.last_server never gets cleared.
            // ================================================================
            // PHASE 15: 2-phase-commit-aware join protocol
            // ================================================================
            // The player_data row now carries three cross-server signals:
            //   online            (0 = not on any server, 1 = on some server)
            //   last_server       (which server claimed ownership)
            //   logout_started_at (NOT NULL = save in progress on that server,
            //                      NULL = no in-flight save)
            //
            // Decision matrix (online=1 branch):
            //   last_server=self  -> we already own (shouldn't happen on fresh
            //                        join, but harmless — proceed)
            //   last_server=peer  + logout_started_at IS NULL
            //                     -> peer has ACTIVE session. Kick if the
            //                        kick_when_already_online policy is on;
            //                        otherwise force-claim (accepts the risk).
            //   last_server=peer  + logout_started_at = recent (< 10s)
            //                     -> peer is mid-save. Wait briefly.
            //   last_server=peer  + logout_started_at = stale (> 10s)
            //                     -> ghost session (peer crashed mid-save,
            //                        SIGKILL, process frozen). Force-claim.
            //   peer heartbeat stale (> peer_stale_threshold_seconds)
            //                     -> peer is dead regardless of logout flag.
            //                        Force-claim instantly.
            // online=0            -> clean state, claim immediately.
            //
            // The claim UPDATE is a CAS:
            //   WHERE uuid=? AND (online=0 OR last_server=? OR <force-claim>)
            // so two concurrent joining servers can never both succeed.
            // ================================================================
            final int MAX_POLL = JdbcConfig.JOIN_POLL_MAX_ATTEMPTS.get();
            final int POLL_INTERVAL_MS = JdbcConfig.JOIN_POLL_INTERVAL_MS.get();
            final long STALE_HEARTBEAT_MS = JdbcConfig.PEER_STALE_THRESHOLD_SECONDS.get() * 1000L;
            // logout_started_at age beyond which we treat a 'save in progress'
            // as actually stuck (peer crashed mid-save). Saves typically complete
            // in < 1s.
            // 2.2.0: never below the JDBC socket timeout plus a margin. A peer whose save
            // is merely slow (struggling database) can legitimately sit on one statement
            // for up to jdbc_socket_timeout_seconds; claiming at 10 s made its final write
            // fail the last_server guard and lost the player's last session. The stamp is
            // also written when the save starts now, not when it was queued.
            final long LOGOUT_SAVE_MAX_MS = Math.max(10_000L,
                    JdbcConfig.JDBC_SOCKET_TIMEOUT_SECONDS.get() * 1000L + 10_000L);
            // How long an ALIVE peer holding online=1 without a logout marker is tolerated
            // before we take ownership anyway. The effective ceiling is also bounded by the
            // poll budget (join_poll_max_attempts x join_poll_interval_ms): once that runs
            // out the loop exits without a force-claim and the guarded CAS below asks the
            // player to reconnect, which is the safe outcome.
            final long ALIVE_PEER_MAX_WAIT_MS = JdbcConfig.JOIN_PEER_ALIVE_MAX_WAIT_SECONDS.get() * 1000L;
            final int SELF = JdbcConfig.SERVER_ID.get();

            boolean forceClaim = false;   // bypass online=0 / last_server=self guard
            // PHASE 18.1 FIX: track whether the row exists at all. A brand-new player
            // has no row yet — the CAS claim below must be skipped (it would return
            // 0 rows affected, which the old code misinterpreted as 'another server
            // claimed first' and wrongly kicked the player with the 'finalizing your
            // save' message on their very first connection). For new players the row
            // gets INSERTed later by the async init write in the new-player branch.
            boolean isNewPlayer = false;
            final long pollStartTime = System.currentTimeMillis();
            for (int attempt = 0; attempt < MAX_POLL; attempt++) {
                // 2.2.0: stop waiting for a player who is gone. Polling on would end in a
                // claim taken for nobody: the departed session's logout has already run, so
                // nothing would ever clear online=1 and the player would be refused on every
                // server ("already online") until this one restarted.
                if (!isJoinSessionCurrent(player_uuid, session)) {
                    PlayerSync.LOGGER.info("Player {} left while waiting for the previous server — join abandoned", player_uuid);
                    SyncLogger.raceCondition(player_uuid, "Player left during join poll — abandoned before claim");
                    return;
                }
                int otherServer;
                boolean otherOnline;
                long logoutStartedAt;     // 0 = NULL (no save in progress)
                boolean rowExists;

                try (JDBCsetUp.QueryResult qrCheck = JDBCsetUp.executePreparedQuery(
                        "SELECT online, last_server, COALESCE(logout_started_at, 0) AS lsa FROM "
                                + Tables.playerData() + " WHERE uuid=?", player_uuid)) {
                    ResultSet rsCheck = qrCheck.resultSet();
                    rowExists = rsCheck.next();
                    if (!rowExists) {
                        isNewPlayer = true;
                        break; // new player — nothing to wait for, skip CAS
                    }
                    otherServer = rsCheck.getInt("last_server");
                    otherOnline = rsCheck.getBoolean("online");
                    logoutStartedAt = rsCheck.getLong("lsa");
                }

                // Fast path: row is clean or already ours.
                if (!otherOnline || otherServer == SELF) break;

                // Peer heartbeat fully stale => peer process dead, force-claim.
                if (otherServer == 0 || isPeerServerStale(otherServer, STALE_HEARTBEAT_MS)) {
                    SyncLogger.raceCondition(player_uuid,
                            "Peer " + otherServer + " heartbeat stale — force-claiming after " + attempt + " attempts");
                    forceClaim = true;
                    break;
                }

                long now = System.currentTimeMillis();
                long waitedMs = now - pollStartTime;

                if (logoutStartedAt > 0) {
                    long saveAgeMs = now - logoutStartedAt;
                    if (saveAgeMs > LOGOUT_SAVE_MAX_MS) {
                        // Peer marked logout-in-progress but never cleared it ->
                        // save thread died mid-flight. Force-claim.
                        SyncLogger.raceCondition(player_uuid,
                                "Peer " + otherServer + " logout save stalled " + saveAgeMs
                                        + "ms (> " + LOGOUT_SAVE_MAX_MS + "ms) — force-claiming");
                        forceClaim = true;
                        break;
                    }
                    // Peer is actively committing; it writes logout_started_at=NULL
                    // + online=0 atomically on success. Give it a short poll cycle.
                    if ((attempt % 10) == 0) {
                        SyncLogger.raceCondition(player_uuid,
                                "Peer " + otherServer + " save in flight (logout_age=" + saveAgeMs
                                        + "ms, attempt=" + (attempt + 1) + "/" + MAX_POLL + ")");
                    }
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }

                // online=1 AND logout_started_at IS NULL: the peer has an ACTIVE session and
                // its heartbeat is fresh. Either the joining player is racing a real session
                // there, or the peer holds a ghost flag it has not cleared yet.
                //
                // 2.1.6: this is the point `join_peer_alive_max_wait_seconds` was documented
                // to govern — and never did. The hardcoded 2 s force-claim it actually used
                // is a duplication vector in its own right: claiming ownership makes every
                // subsequent write from the peer fail its last_server guard, so anything the
                // peer recorded after our claim (a pickup, a drop, a deposit) is discarded
                // while the world still holds the matching entity. The key now applies.
                if (ALIVE_PEER_MAX_WAIT_MS <= waitedMs) {
                    SyncLogger.raceCondition(player_uuid,
                            "Peer " + otherServer + " online=1 without logout flag — force-claiming after "
                                    + waitedMs + "ms (join_peer_alive_max_wait_seconds reached)");
                    forceClaim = true;
                    break;
                }
                if ((attempt % 10) == 0) {
                    SyncLogger.raceCondition(player_uuid,
                            "Peer " + otherServer + " online=1 but no logout_started_at — brief grace period (waited=" + waitedMs + "ms)");
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }

            // ================================================================
            // CLAIM with atomic CAS. Two concurrent joining servers can never
            // both succeed — the one that lands its UPDATE second sees 0 rows
            // affected and aborts its restore.
            //
            // PHASE 18.1: new players skip the CAS entirely. No row exists yet,
            // so UPDATE affects 0 rows by definition — the old code was kicking
            // FIRST-TIME joiners with "another server is finalizing your save".
            // The new-player branch will INSERT the row with the correct state
            // in a moment (async init write).
            // ================================================================
            if (!isNewPlayer) {
                int claimed;
                if (forceClaim) {
                    // Unconditional — we've decided the previous owner is defunct.
                    claimed = JDBCsetUp.executePreparedUpdateRet(
                            "UPDATE " + Tables.playerData()
                                    + " SET last_server=?, online=1, logout_started_at=NULL WHERE uuid=?",
                            SELF, player_uuid);
                } else {
                    // Guarded — only claim if the row is actually clean or already ours.
                    claimed = JDBCsetUp.executePreparedUpdateRet(
                            "UPDATE " + Tables.playerData()
                                    + " SET last_server=?, online=1, logout_started_at=NULL"
                                    + " WHERE uuid=? AND (online=0 OR last_server=?)",
                            SELF, player_uuid, SELF);
                }
                if (claimed == 0) {
                    // Row exists (we checked in the poll) but the guard blocked us —
                    // meaning another server claimed between our poll read and our
                    // UPDATE. Defer to that winner and ask the player to retry.
                    PlayerSync.LOGGER.warn("Player {} claim CAS lost — another server claimed first; kicking this session", player_uuid);
                    SyncLogger.raceCondition(player_uuid, "Claim CAS lost — deferring to the winner");
                    server.execute(() -> {
                        if (serverPlayer.connection != null) {
                            serverPlayer.connection.disconnect(Component.translatableWithFallback(
                                    "playersync.claim_lost",
                                    "PlayerSync: another server is finalizing your save. Please reconnect in a few seconds."));
                        }
                    });
                    // 2.2.0: the session stays sync-pending (frozen, never saved) until the
                    // kick lands; its logout cleans up through the unsynced path.
                    return;
                }
                // The player may have left between the last poll and the claim. Their logout
                // ran before the claim existed, so the claim is ours to give back.
                if (!isJoinSessionCurrent(player_uuid, session)) {
                    releaseOrphanedClaim(player_uuid);
                    return;
                }
            }

            // === PHASE 1: DB reads on background thread (thread-safe) ===

            // PERF (A8): single SELECT with explicit column list — covers both the
            // existence check (rs.next()==false means "new player, run init path") and
            // the full data read. Previously two separate SELECTs on the same row
            // produced two MySQL round-trips per join.
            // 2.2.0: `advancements` is read HERE, after the claim, when advancement sync is
            // on. It used to be fetched by onDataPackSyncEvent the moment the player logged
            // in — before the previous server had committed its logout save — so a quick
            // server switch applied the older blob to disk and the next save made that
            // rollback permanent. It is the same single round trip as before.
            final boolean syncAdvancements = JdbcConfig.SYNC_ADVANCEMENTS.get();
            final int health, foodLevel, xp, score;
            final String leftHand, cursors, armorData, inventoryData, enderChestData, effectData, advancementsData;

            try (JDBCsetUp.QueryResult qr2 = JDBCsetUp.executePreparedQuery(
                    "SELECT health, food_level, xp, score, left_hand, cursors, armor, inventory, enderchest, effects"
                            + (syncAdvancements ? ", advancements" : "") + " FROM "
                            + Tables.playerData() + " WHERE uuid=?", player_uuid)) {
                ResultSet rs2 = qr2.resultSet();
                if (!rs2.next()) {
                    // No row in DB → brand new player.
                    // AUDIT FIX (main-thread I/O): the old init path ran store() —
                    // a synchronous INSERT plus one REPLACE per backpack/SS/RS2 item —
                    // entirely inside server.execute() on the MAIN thread (worst case
                    // 10s connectionTimeout stall per first-time joiner). Converted to
                    // the established pattern: DB pre-read here (BG), entity work +
                    // snapshot on main, DB writes back on the executor.
                    final String newPlayerCuriosData;
                    if (ModList.get().isLoaded("curios")) {
                        try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(
                                "SELECT curios_item FROM " + Tables.curios() + " WHERE uuid=?", player_uuid)) {
                            ResultSet rs = qr.resultSet();
                            newPlayerCuriosData = rs.next() ? rs.getString("curios_item") : null;
                        }
                    } else { newPlayerCuriosData = null; }

                    server.execute(() -> {
                        if (!isJoinSessionCurrent(player_uuid, session)
                                || server.getPlayerList().getPlayer(serverPlayer.getUUID()) != serverPlayer) {
                            return;
                        }
                        try {
                            // Orphaned curios row (player_data wiped, curios kept):
                            // apply it to the entity so the init snapshot captures it.
                            if (newPlayerCuriosData != null) {
                                ModsSupport.applyCuriosFromData(serverPlayer, newPlayerCuriosData);
                            }

                            // MAIN THREAD: freeze entity state (fast copies, no DB).
                            final DeferredPlayerSnapshot frozen = snapshotPlayerData(serverPlayer);
                            final Map<UUID, CompoundTag> storageSnapshots = snapshotAllStorages(serverPlayer);

                            // BACKGROUND: INSERT + mod writes. The player_synced tag is
                            // added only AFTER the INSERT lands — otherwise an instant
                            // disconnect would fire a logout save whose last_server-guarded
                            // UPDATE hits 0 rows (no row yet) and the first session is lost.
                            executorService.submit(() -> {
                                try {
                                    PlayerDataSnapshot s = frozen.materialize();
                                    PlayerSync.LOGGER.info("Storing data for new player {}", player_uuid);
                                    // INSERT IGNORE: idempotent against a quick
                                    // disconnect+reconnect racing two init paths.
                                    JDBCsetUp.executePreparedUpdate(
                                            "INSERT IGNORE INTO " + Tables.playerData()
                                                    + " (uuid, armor, inventory, enderchest, advancements, effects, xp, food_level, health, score, left_hand, cursors, online, last_server)"
                                                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)",
                                            s.uuid(), s.equipment(), s.inventory(), s.enderChest(),
                                            s.advancements() == null ? "" : s.advancements(), s.effects(),
                                            s.xp(), s.foodLevel(), s.health(), s.score(), s.leftHand(), s.cursors(),
                                            JdbcConfig.SERVER_ID.get());
                                    if (s.curiosData() != null) {
                                        JDBCsetUp.executePreparedUpdate(
                                                "REPLACE INTO " + Tables.curios() + " (uuid, curios_item) VALUES (?, ?)",
                                                s.uuid(), s.curiosData());
                                    }
                                    ModsSupport.saveBackpackSnapshots(storageSnapshots);
                                    // The INSERT writes online=1. If the player left while it
                                    // was queued, their logout's online=0 may already have run
                                    // against a row that did not exist yet: give the flag back.
                                    if (!isJoinSessionCurrent(player_uuid, session)) {
                                        releaseOrphanedClaim(player_uuid);
                                        return;
                                    }
                                    // Tag first, then lift the pending flag, both on the main
                                    // thread: a logout can never see the session as finished
                                    // but not yet tagged.
                                    server.execute(() -> {
                                        if (isJoinSessionCurrent(player_uuid, session)) {
                                            serverPlayer.addTag("player_synced");
                                        }
                                        finishJoin(player_uuid, session);
                                    });
                                } catch (Exception e) {
                                    PlayerSync.LOGGER.error("Error persisting new player {}", player_uuid, e);
                                    // 2.2.0: without a row and the tag nothing of this session
                                    // could ever be saved; ask for a reconnect instead.
                                    if (isJoinSessionCurrent(player_uuid, session)) {
                                        server.execute(() -> abortUnsyncedSession(serverPlayer, player_uuid,
                                                "could not create the player row"));
                                    }
                                }
                            });
                        } catch (Exception e) {
                            PlayerSync.LOGGER.error("Error initializing new player {}", player_uuid, e);
                            finishJoin(player_uuid, session);
                        }
                    });
                    return;
                }
                health = rs2.getInt("health");
                foodLevel = rs2.getInt("food_level");
                xp = rs2.getInt("xp");
                score = rs2.getInt("score");
                leftHand = rs2.getString("left_hand");
                cursors = rs2.getString("cursors");
                armorData = rs2.getString("armor");
                inventoryData = rs2.getString("inventory");
                enderChestData = rs2.getString("enderchest");
                effectData = rs2.getString("effects");
                advancementsData = syncAdvancements ? rs2.getString("advancements") : null;
            }

            // Pre-read ALL mod data on BACKGROUND THREAD (no entity access).
            final String curiosData;
            if (ModList.get().isLoaded("curios")) {
                try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(
                        "SELECT curios_item FROM " + Tables.curios() + " WHERE uuid=?", player_uuid)) {
                    ResultSet rs = qr.resultSet();
                    curiosData = rs.next() ? rs.getString("curios_item") : null;
                }
            } else { curiosData = null; }

            // AUDIT FIX (N+1): single range scan on the (uuid, mod_id) PK instead of
            // three sequential round-trips (accessories / cosmeticarmor / attachments).
            // isLoaded gating preserved: a stale row for an uninstalled mod yields null
            // exactly as before; neoforge_attachments is read unconditionally.
            final String accessoriesData, cosmeticArmorData, attachmentsData, persistentData;
            {
                String acc = null, cos = null, att = null, per = null;
                final boolean accessoriesLoaded = ModList.get().isLoaded("accessories");
                final boolean cosmeticLoaded = ModList.get().isLoaded("cosmeticarmorreworked");
                try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(
                        "SELECT mod_id, data_value FROM " + Tables.modPlayerData() + " WHERE uuid=?",
                        player_uuid)) {
                    ResultSet rs = qr.resultSet();
                    while (rs.next()) {
                        switch (rs.getString("mod_id")) {
                            case "accessories"          -> { if (accessoriesLoaded) acc = rs.getString("data_value"); }
                            case "cosmeticarmor"        -> { if (cosmeticLoaded)    cos = rs.getString("data_value"); }
                            case "neoforge_attachments" -> att = rs.getString("data_value");
                            case "persistent_data"      -> per = rs.getString("data_value");
                            default -> { /* rows written by a newer version — ignore */ }
                        }
                    }
                }
                accessoriesData = acc; cosmeticArmorData = cos; attachmentsData = att; persistentData = per;
            }

            // === PHASE 2: Apply to player on MAIN SERVER THREAD ===
            // The server.execute() callback fires when the main thread is ready.
            // Note: Backpack/SS/RS2 restore still does DB reads on main thread (1-5 queries
            // per player). This is acceptable because players join one at a time, not 35 at once.
            // The real performance fix is staggering the auto-save (see onServerTick).
            server.execute(() -> {
                // 2.2.0: the sync-pending flag is only lifted once the restore really
                // happened. A restore that throws half-way used to lift it anyway, leaving
                // the player with a partially restored inventory that the next save wrote
                // over the database. Now the session stays unsynced (nothing it holds is
                // ever written) and the player is asked to reconnect for a clean retry.
                boolean completed = false;
                try {
                    // FIX: Verify the player is still connected before applying data.
                    // 2.2.0: compare the session and the entity itself, not just the uuid —
                    // after a quick leave + rejoin the uuid is online again, but as a new
                    // entity that this task must not touch.
                    if (!isJoinSessionCurrent(player_uuid, session)
                            || server.getPlayerList().getPlayer(serverPlayer.getUUID()) != serverPlayer) {
                        PlayerSync.LOGGER.warn("Player {} disconnected before sync apply, skipping", player_uuid);
                        SyncLogger.dataLoss(player_uuid, "Player disconnected before sync apply — .dat data may persist, DB data not applied");
                        completed = true; // nothing to retry: the session is gone
                        return;
                    }

                    // === FIX DUP-REVIVE ===
                    // If the rejoining player is still in Revive Me's "fallen" (downed)
                    // state — or has already died during the join delay — DO NOT apply
                    // the DB data. The vanilla .dat file is the single source of truth
                    // for the player's items in this transient phase:
                    //
                    //  - Fallen player reconnecting: the .dat carries the exact inventory
                    //    they had when they fell. Restoring the DB copy here is what
                    //    caused the dup — Revive Me resumes the fall timer on reconnect,
                    //    the death finalizes, a corpse / gravestone mod captures the
                    //    inventory, and THEN this apply re-granted a second copy from DB.
                    //  - Player who died mid-join: their entity inventory was already
                    //    emptied by the death; re-applying the DB copy would dup with
                    //    the corpse.
                    //
                    // Skipping the apply leaves the .dat state untouched. When the player
                    // leaves the fallen state the normal sync resumes: a successful revive
                    // is captured by the next auto-save / logout-save; a finalized death is
                    // captured by onPlayerRespawn (empty inventory → DB). No dup, no loss.
                    final boolean joinFallen = isReviveMeFallen(serverPlayer);
                    final boolean joinDead = serverPlayer.isDeadOrDying();
                    if (joinFallen || joinDead) {
                        PlayerSync.LOGGER.info("[revive-detect] player {} joined in a fallen/dead state (fallen={}, deadOrDying={}) — skipping DB apply, keeping .dat inventory to avoid corpse dup",
                                player_uuid, joinFallen, joinDead);
                        SyncLogger.playerEvent(player_uuid, "JOIN_FALLEN_SKIP",
                                "Joined in ReviveMe fallen/dead state — DB apply skipped, vanilla .dat kept (corpse dup prevention)");
                        // Advancements are progression, not items: they apply either way.
                        applyAdvancementsFromData(server, serverPlayer, player_uuid, advancementsData);
                        serverPlayer.addTag("player_synced");
                        completed = true;
                        return; // syncNotCompletedPlayer cleanup runs in the finally block
                    }

                    // 2.2.0: the [sync_toggles] for vanilla data are honoured on both sides.
                    // A category that is off here is neither restored (the local .dat value
                    // stays) nor written (the stored value is kept for the servers that do
                    // sync it). Every toggle defaults to true, which is the historical path.
                    final boolean syncInventory = JdbcConfig.SYNC_INVENTORY.get();
                    final boolean syncEnderChest = JdbcConfig.SYNC_ENDER_CHEST.get();

                    // ANTI-DUPLICATION: Clear all inventories BEFORE restoring
                    if (syncInventory) {
                        serverPlayer.getInventory().clearContent();
                        serverPlayer.setItemInHand(InteractionHand.OFF_HAND, ItemStack.EMPTY);
                        serverPlayer.containerMenu.setCarried(ItemStack.EMPTY);
                        // The 2x2 crafting grid is not part of the Inventory: items parked there
                        // from the stale .dat state would survive the restore and come back to
                        // the inventory when the grid closes.
                        for (int i = net.minecraft.world.inventory.InventoryMenu.CRAFT_SLOT_START;
                             i < net.minecraft.world.inventory.InventoryMenu.CRAFT_SLOT_END; i++) {
                            serverPlayer.inventoryMenu.getSlot(i).set(ItemStack.EMPTY);
                        }
                        for (int i = 0; i < serverPlayer.getInventory().armor.size(); i++) {
                            serverPlayer.getInventory().armor.set(i, ItemStack.EMPTY);
                        }
                    }
                    if (syncEnderChest) {
                        serverPlayer.getEnderChestInventory().clearContent();
                    }

                    // Restore basic attributes
                    if (JdbcConfig.SYNC_HEALTH_FOOD.get()) {
                        serverPlayer.setHealth(health <= 0 ? 1 : health);
                        serverPlayer.getFoodData().setFoodLevel(foodLevel);
                    }
                    if (JdbcConfig.SYNC_XP.get()) {
                        setXpForPlayer(serverPlayer, xp);
                        serverPlayer.setScore(score);
                    }

                    // Restore items. Each slot is restored on its own: one unreadable item
                    // becomes a placeholder that keeps its stored data instead of aborting
                    // the restore of everything after it.
                    if (syncInventory) {
                        serverPlayer.setItemInHand(InteractionHand.OFF_HAND, restoreItemSafely(leftHand, player_uuid, "offhand"));
                        serverPlayer.containerMenu.setCarried(restoreItemSafely(cursors, player_uuid, "cursor"));

                        if (armorData != null && armorData.length() > 2) {
                            Map<Integer, String> equipment = LocalJsonUtil.StringToEntryMap(armorData);
                            for (Map.Entry<Integer, String> entry : equipment.entrySet()) {
                                if (!validSlot(entry.getKey(), serverPlayer.getInventory().armor.size(), player_uuid, "armor")) continue;
                                serverPlayer.getInventory().armor.set(entry.getKey(),
                                        restoreItemSafely(entry.getValue(), player_uuid, "armor:" + entry.getKey()));
                            }
                        }
                        if (inventoryData != null && inventoryData.length() > 2) {
                            Map<Integer, String> inventory = LocalJsonUtil.StringToEntryMap(inventoryData);
                            for (Map.Entry<Integer, String> entry : inventory.entrySet()) {
                                if (!validSlot(entry.getKey(), serverPlayer.getInventory().items.size(), player_uuid, "inventory")) continue;
                                serverPlayer.getInventory().setItem(entry.getKey(),
                                        restoreItemSafely(entry.getValue(), player_uuid, "inventory:" + entry.getKey()));
                            }
                        }
                    }
                    if (syncEnderChest && enderChestData != null && enderChestData.length() > 2) {
                        Map<Integer, String> ender_chest = LocalJsonUtil.StringToEntryMap(enderChestData);
                        for (Map.Entry<Integer, String> entry : ender_chest.entrySet()) {
                            if (!validSlot(entry.getKey(), serverPlayer.getEnderChestInventory().getContainerSize(), player_uuid, "enderchest")) continue;
                            serverPlayer.getEnderChestInventory().setItem(entry.getKey(),
                                    restoreItemSafely(entry.getValue(), player_uuid, "enderchest:" + entry.getKey()));
                        }
                    }

                    // Always clear effects, then restore from DB
                    if (JdbcConfig.SYNC_EFFECTS.get()) {
                        serverPlayer.removeAllEffects();
                        if (effectData != null && effectData.length() > 2) {
                            Map<Integer, String> effects = LocalJsonUtil.StringToEntryMap(effectData);
                            for (Map.Entry<Integer, String> entry : effects.entrySet()) {
                                try {
                                    CompoundTag effectTag = NbtUtils.snbtToStructure(deserializeString(entry.getValue()));
                                    MobEffectInstance mobEffectInstance = MobEffectInstance.load(effectTag);
                                    if (mobEffectInstance != null) {
                                        serverPlayer.addEffect(mobEffectInstance);
                                    }
                                } catch (Exception effectError) {
                                    // An effect is not worth failing the whole restore for.
                                    PlayerSync.LOGGER.warn("Skipping an unreadable effect for player {}: {}",
                                            player_uuid, effectError.toString());
                                }
                            }
                        }
                    }

                    // Apply mod data from pre-read strings (NO DB calls on main thread).
                    ModsSupport.applyCuriosFromData(serverPlayer, curiosData);
                    ModCompatSync.applyAccessoriesFromData(serverPlayer, accessoriesData);
                    ModCompatSync.applyCosmeticArmorFromData(serverPlayer, cosmeticArmorData);
                    ModCompatSync.applyAttachmentsFromData(serverPlayer, attachmentsData);
                    // Corail Tombstone knowledge / alignment / perks and every other mod still
                    // using the legacy scratchpad ride along here.
                    ModCompatSync.applyPersistentDataFromData(serverPlayer, persistentData);
                    applyAdvancementsFromData(server, serverPlayer, player_uuid, advancementsData);

                    // PHASE 12 PERF: prefetch ALL storage UUIDs (backpacks + SS + RS2)
                    // in a single batched SELECT, then apply from the in-memory cache
                    // instead of making N sequential round-trips on the main thread.
                    // Shulker-heavy players see ~8-10× reduction in restore latency
                    // because backpack_data is shared across the three mod sources.
                    java.util.List<UUID> prefetchUuids = new java.util.ArrayList<>();
                    if (JdbcConfig.SYNC_BACKPACKS.get()) {
                        prefetchUuids.addAll(ModsSupport.collectBackpackUuids(serverPlayer, true));
                        if (ModList.get().isLoaded("sophisticatedstorage")) {
                            prefetchUuids.addAll(ModsSupport.collectSSUuids(serverPlayer));
                        }
                    }
                    if (JdbcConfig.SYNC_REFINED_STORAGE.get() && ModList.get().isLoaded("refinedstorage")) {
                        prefetchUuids.addAll(ModsSupport.collectRS2DiskUuids(serverPlayer));
                    }
                    // The prefetch scope doubles as proof-of-absence: a UUID that was queried
                    // and came back without a row provably has no DB copy, so the restore
                    // path can adopt the local contents without issuing its own SELECT.
                    java.util.Map<UUID, CompoundTag> prefetched = prefetchUuids.isEmpty()
                            ? java.util.Map.of()
                            : ModsSupport.prefetchStorageContents(prefetchUuids);
                    ModsSupport.setStoragePrefetchCache(prefetched, prefetchUuids);
                    if (!prefetchUuids.isEmpty()) {
                        PlayerSync.LOGGER.debug("[perf-restore] prefetched {}/{} storage UUIDs for player {}",
                                prefetched.size(), prefetchUuids.size(), player_uuid);
                    }
                    try {
                        // Backpacks/SS/RS2 all read from the prefetch cache installed above.
                        ModsSupport.doBackPackRestore(serverPlayer);
                        ModsSupport.restoreSophisticatedStorageItems(serverPlayer);
                        ModsSupport.restoreRefinedStorageDisks(serverPlayer);
                    } finally {
                        ModsSupport.clearStoragePrefetchCache();
                    }

                    serverPlayer.addTag("player_synced");
                    completed = true;
                    long totalRestore = System.currentTimeMillis() - restoreT0;
                    PlayerSync.LOGGER.info("Sync data for player {} completed in {}ms", player_uuid, totalRestore);
                    SyncLogger.restoreCompleted(player_uuid, totalRestore);
                    if (totalRestore > 1000) {
                        PlayerSync.LOGGER.warn("[perf-restore] slow restore for {} ({}ms) — enable log level=TRACE to profile",
                                player_uuid, totalRestore);
                    }
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error applying sync data for player {}", player_uuid, e);
                } finally {
                    if (completed) {
                        finishJoin(player_uuid, session);
                    } else {
                        abortUnsyncedSession(serverPlayer, player_uuid, "restore failed while applying data");
                    }
                }
            });

        } catch (Exception e) {
            PlayerSync.LOGGER.error("Internal Exception detected!", e);
            // 2.2.0: a failed read (database hiccup) used to lift the sync-pending flag and
            // let the player play on with the stale local .dat state — which the next save
            // then wrote over the database. Keep the session unsynced and ask for a retry.
            if (isJoinSessionCurrent(player_uuid, session)) {
                server.execute(() -> abortUnsyncedSession(serverPlayer, player_uuid, "could not read stored data"));
            }
        } finally {
            if (lock.isHeldByCurrentThread()) lock.unlock();
        }
    }

    /**
     * Ends a session whose restore did not complete without ever saving it. The player
     * stays in {@link #syncNotCompletedPlayer}, so every save path skips them and their
     * logout only clears the online flag: the database keeps the last good state and the
     * next login retries the restore from it. Main thread only.
     */
    private static void abortUnsyncedSession(ServerPlayer player, String uuid, String reason) {
        PlayerSync.LOGGER.error("Sync for player {} did not complete ({}). Nothing from this session will be saved;"
                + " disconnecting so the next login retries from the database.", uuid, reason);
        SyncLogger.saveSkipped(uuid, "JOIN", "restore incomplete (" + reason + ") — session left unsynced, player disconnected");
        try {
            if (player.connection != null && !player.hasDisconnected()) {
                player.connection.disconnect(Component.translatableWithFallback("playersync.sync_failed",
                        "PlayerSync could not load your data. Nothing was lost, please reconnect."));
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Could not disconnect unsynced player {}", uuid, e);
        }
    }

    /**
     * A stored slot index outside the container would throw and fail the whole restore on
     * every login. Logged and skipped instead (the entry stays in the database until the
     * next save of this player).
     */
    private static boolean validSlot(Integer slot, int size, String playerUuid, String container) {
        if (slot != null && slot >= 0 && slot < size) return true;
        PlayerSync.LOGGER.error("Stored {} slot {} is out of range (size {}) for player {} — skipped", container, slot, size, playerUuid);
        SyncLogger.nbtAnomaly(playerUuid, container + " slot " + slot + " out of range");
        return false;
    }

    /**
     * Restores one stored item. Never throws: a slot whose data cannot be read becomes a
     * placeholder holding the raw stored string, which {@link #getNbtForStorage} writes back
     * unchanged. The item therefore survives in the database for an admin to look at instead
     * of being replaced by an empty slot on the next save.
     */
    private static ItemStack restoreItemSafely(String serialized, String playerUuid, String slot) {
        try {
            return deserializeAndCreatePlaceholderIfNeeded(serialized);
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Unreadable item in {} for player {} ({} chars) — kept as a placeholder: {}",
                    slot, playerUuid, serialized == null ? 0 : serialized.length(), e.toString());
            SyncLogger.nbtAnomaly(playerUuid, "unreadable item in " + slot + " kept as placeholder");
            return createPreservingPlaceholder(serialized, "unknown", 0);
        }
    }

    @SubscribeEvent
    public static void onPlayerConnect(PlayerNegotiationEvent event) {
        // MUST run synchronously to block login until the duplicate check completes.
        // Running async allowed players to join before the kick check finished.
        try {
            doPlayerConnect(event);
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error during player connection check", e);
            event.getConnection().disconnect(Component.translatableWithFallback("playersync.sqlexception","SqlException detected!Connection lost,please contact with your admin."));
        }
    }

    /**
     * FIX: Full duplicate-login kick check during PlayerLoggedInEvent.
     * PlayerNegotiationEvent.getConnection().disconnect() does NOT reliably disconnect
     * the player in NeoForge 1.21.1. By the time PlayerLoggedInEvent fires, we have
     * a full ServerPlayer with player.connection.disconnect() which is reliable.
     *
     * Also marks online=1 SYNCHRONOUSLY to close the race condition window.
     */
    @SubscribeEvent(priority = net.neoforged.bus.api.EventPriority.HIGHEST)
    public static void onPlayerLoggedInKickCheck(PlayerEvent.PlayerLoggedInEvent event) {
        ServerPlayer player = (ServerPlayer) event.getEntity();
        String player_uuid = player.getUUID().toString();

        // FIX PERF: Use cached data from doPlayerConnect (network thread) instead of
        // re-querying the DB. Eliminates 2-4 blocking DB queries from the MAIN THREAD.
        // doPlayerConnect already ran the same checks on the network thread and cached results.
        CachedConnectCheck cachedEntry = connectCheckCache.remove(player_uuid);
        // PERF (A7): treat stale entries (> TTL) as cache miss → forces the safe fallback DB
        // path. Stops a stale 5-min-old entry from making the wrong kick decision.
        int[] cached = (cachedEntry != null && System.currentTimeMillis() - cachedEntry.insertedAt() <= CONNECT_CHECK_TTL_MS)
                ? cachedEntry.data() : null;

        if (!JdbcConfig.KICK_WHEN_ALREADY_ONLINE.get()) {
            // PHASE 14 FIX: do NOT pre-mark online=1 here. Previously this UPDATE ran on
            // the executor BEFORE doPlayerJoin's poll, overwriting a peer's freshly-committed
            // online=0 — the poll would then see online=1 + last_server=OldPeer and wait the
            // full 60s even though the peer had already flushed (observed in production logs
            // 2026-04-22 07:43:41 -> 07:45:01, 60s of 'Waiting for server X to finish saving'
            // when X had actually committed 19s earlier).
            // doPlayerJoin now sets online=1 atomically with last_server=self as part of its
            // claim UPDATE, after the poll has seen the true state.
            return;
        }

        try {
            if (cached != null && cached[3] == 1) {
                // doPlayerConnect already determined this player should be kicked (server alive)
                // but PlayerNegotiationEvent.disconnect() is unreliable in NeoForge 1.21.1
                // — use the reliable ServerPlayer.connection.disconnect() instead.
                kickedForDuplicateLogin.add(player_uuid);
                PlayerSync.LOGGER.warn("Kicking player {} - already online on server {} (cached check)", player_uuid, cached[1]);
                player.connection.disconnect(Component.translatableWithFallback(
                        "playersync.already_online",
                        "You can't join more than one synchronization server at the same time."));
                return;
            }

            if (cached != null && cached[0] == 1 && cached[1] != JdbcConfig.SERVER_ID.get() && cached[2] == 0) {
                // Player was online on another server but that server is dead — already handled
                // by doPlayerConnect (server disabled). No need to re-query.
            }
            // 2.2.0: the DB fallback that used to run here (cache miss) is now done by the
            // join task, off the main thread — see checkAlreadyOnlineElsewhere. On NeoForge
            // 21.1 PlayerNegotiationEvent is never posted, so the cache above is never filled
            // and that fallback ran one to three blocking queries on the tick thread for
            // every single login.

            // PHASE 14 FIX: online=1 is no longer written here. See doPlayerJoin's claim
            // UPDATE for the replacement — setting the flag earlier raced the poll and
            // caused every cross-server join to wait the full 60s.
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error during kick check for player {}", player_uuid, e);
        }
    }

    @SubscribeEvent
    public static void onPlayerJoin(PlayerEvent.PlayerLoggedInEvent event) {
        String puuid = ((ServerPlayer) event.getEntity()).getUUID().toString();

        // FIX: Don't start sync for players that were already kicked by onPlayerLoggedInKickCheck.
        // Without this, doPlayerJoin runs on a background thread for a kicked player, wastes
        // resources, and leaves stale entries in syncNotCompletedPlayer / playerLocks.
        if (kickedForDuplicateLogin.contains(puuid)) return;

        // 2.2.0: entity tags are saved in the player's .dat, so "player_synced" arrives
        // already set from the previous session on this server. Strip it so the tag only
        // ever means "restored from the database in THIS session": the shutdown path and
        // the auto-save gate on it, and must never persist a .dat state that the restore
        // has not replaced yet.
        ((ServerPlayer) event.getEntity()).removeTag("player_synced");

        // Mark sync as pending BEFORE submitting to thread pool.
        syncNotCompletedPlayer.add(puuid);
        final Object session = new Object();
        joinSessions.put(puuid, session);
        joinExecutor.execute(() -> {
            try {
                doPlayerJoin(event, session);
            } catch (Throwable e) {
                // Never restored: nothing from this session may be saved, so do not let the
                // player play it. Ask for a reconnect instead (see abortUnsyncedSession).
                PlayerSync.LOGGER.error("Join task failed for player {}", puuid, e);
                ServerPlayer joining = (ServerPlayer) event.getEntity();
                MinecraftServer srv = joining.getServer();
                if (srv != null && isJoinSessionCurrent(puuid, session)) {
                    srv.execute(() -> abortUnsyncedSession(joining, puuid, "join task failed"));
                }
            }
        });
    }

    /**
     * AUDIT FIX (security — log injection / disk flood): item NBT contains
     * player-authored content (anvil renames, book pages, mod data). SNBT escaping
     * does NOT escape raw newlines inside string tags, so logging it verbatim lets
     * a crafted book forge multi-line entries in latest.log — the same file that
     * carries [admin-dump]/[admin-wipe] audit records. Multi-hundred-KB payloads
     * also flooded the log on every join. This helper truncates to 256 chars and
     * neutralizes control characters before the payload reaches a logger.
     */
    private static String nbtPreview(String s) {
        if (s == null) return "null";
        String t = s.length() > 256 ? s.substring(0, 256) + "...(" + s.length() + " chars)" : s;
        return t.replaceAll("[\\r\\n]", "\\\\n").replaceAll("\\p{Cntrl}", "?");
    }

    // deserialize item and potentially create placeholders
    public static ItemStack deserializeAndCreatePlaceholderIfNeeded(String serializedNbt)
            throws CommandSyntaxException {
        if (serializedNbt == null || serializedNbt.isEmpty() || serializedNbt.equals("B64:e30=")) {
            // Check for empty NBT (Base64 encoded '{}')
            return ItemStack.EMPTY;
        }

        CompoundTag compoundTag;
        String nbtString = serializedNbt; // Will be overwritten with decoded SNBT for legacy formats

        // Try binary NBT format first (new format, avoids SNBT round-trip issues)
        if (serializedNbt.startsWith("BNBT:")) {
            try {
                compoundTag = deserializeBinaryBase64Tag(serializedNbt);
            } catch (Exception e) {
                // 2.2.0: keep the unreadable data instead of dropping it. Returning EMPTY made
                // the next save write an empty slot, deleting the stored item for good.
                PlayerSync.LOGGER.error("Failed to deserialize binary NBT data ({} chars), keeping it as a placeholder.",
                        serializedNbt.length(), e);
                return createPreservingPlaceholder(serializedNbt, "unknown", 0);
            }
        } else {
            // Legacy SNBT-based deserialization (B64: or old custom format)
            nbtString = deserializeString(serializedNbt);
            try {
                compoundTag = TagParser.parseTag(nbtString);
            } catch (CommandSyntaxException e) {
                // TagParser may fail on certain 1.21.1 component SNBT formats (e.g. nested lists [[{...}]])
                // Try NbtUtils.snbtToStructure as a fallback
                PlayerSync.LOGGER.warn("TagParser.parseTag failed, trying NbtUtils.snbtToStructure fallback ({} chars): {}",
                        nbtString.length(), nbtPreview(nbtString));
                try {
                    compoundTag = NbtUtils.snbtToStructure(nbtString);
                } catch (CommandSyntaxException e2) {
                    // 2.2.0: keep the data rather than throw. Every caller used to either abort
                    // its whole restore or leave the slot empty — and an empty slot is what
                    // the next save writes back, deleting the stored item.
                    PlayerSync.LOGGER.error("Both SNBT parsers failed ({} chars), keeping it as a placeholder: {}",
                            nbtString.length(), nbtPreview(nbtString));
                    return createPreservingPlaceholder(serializedNbt, "unknown", 0);
                }
            }
        }

        if (compoundTag.isEmpty() || !compoundTag.contains("id", Tag.TAG_STRING)) {
            return ItemStack.EMPTY; // Invalid or empty tag
        }

        ResourceLocation registryName = ResourceLocation.tryParse(compoundTag.getString("id"));

        if (registryName == null) {
            PlayerSync.LOGGER.warn("Failed to parse registry name from NBT ({} chars): {}", nbtString.length(), nbtPreview(nbtString));
            return ItemStack.EMPTY; // Cannot determine item type
        }

        if (BuiltInRegistries.ITEM.containsKey(registryName)) {
            // Item exists (could be vanilla or a loaded mod item), restore normally
            try {
                ItemStack restoredItem = ItemStack.parse(ServerLifecycleHooks.getCurrentServer().registryAccess(),compoundTag).get();
                // Only return the restored item if the ItemStack.of did not unexpectedly
                // return an empty item
                // Either the item is not empty, or it is empty and the original tag was also
                // empty or it was an empty inventory slot
                if (!restoredItem.isEmpty() || compoundTag.isEmpty()
                        || registryName.equals(ResourceLocation.tryParse("air"))) {
                    verifyComponentsSurvived(registryName, compoundTag, restoredItem);
                    return restoredItem;
                }
                // ItemStack.of unexpectedly returned empty for a known, non-air item.
                PlayerSync.LOGGER.warn(
                        "ItemStack.of returned EMPTY for known item {} ({} chars NBT). Creating placeholder as fallback.",
                        registryName, nbtString.length());
            } catch (Exception e) {
                PlayerSync.LOGGER.error(
                        "Error creating ItemStack for known item {} ({} chars NBT): {}. Creating placeholder as fallback.",
                        registryName, nbtString.length(), nbtPreview(nbtString), e);
            }
        }

        // Create placeholder
        PlayerSync.LOGGER.debug("Item {} not found in registry. Creating placeholder.", registryName);
        // AUDIT FIX: removed the unconditional WARN that dumped the FULL compound tag
        // for every placeholder item on every join/restore (log-flood + injection vector).
        // 1.21 stores the stack size as "count"; "Count" is the pre-1.20.5 spelling.
        int storedCount = compoundTag.contains("count") ? compoundTag.getInt("count") : compoundTag.getInt("Count");
        return createPreservingPlaceholder(serializedNbt, registryName.toString(), storedCount);
    }

    /**
     * Largest stored item string a placeholder may carry. The placeholder's custom data is
     * sent to the client with the inventory, so an arbitrarily large payload could exceed
     * the network limits and kick the player on every login.
     */
    private static final int MAX_PRESERVED_ITEM_CHARS = 256 * 1024;

    /**
     * Builds the paper placeholder that stands in for an item this server cannot rebuild
     * (mod absent, or stored data unreadable). It carries the original stored string, which
     * {@link #getNbtForStorage} writes back verbatim, so the real item reappears on a server
     * that can read it and is never replaced by an empty slot.
     */
    private static ItemStack createPreservingPlaceholder(String serializedNbt, String itemId, int count) {
        if (serializedNbt == null || serializedNbt.isEmpty()) return ItemStack.EMPTY;
        if (serializedNbt.length() > MAX_PRESERVED_ITEM_CHARS) {
            PlayerSync.LOGGER.error("Stored item {} is too large to keep as a placeholder ({} chars) — slot left empty",
                    itemId, serializedNbt.length());
            return ItemStack.EMPTY;
        }
        ItemStack placeholder = new ItemStack(Items.PAPER);

        CompoundTag placeholderNbt = new CompoundTag();
        // Store the original serialized NBT string, not the parsed CompoundTag string
        placeholderNbt.putString("playersync:original_item_nbt", serializedNbt);
        placeholderNbt.putString("playersync:original_item_id", itemId);

        // Add a unique UUID to ensure the item is unstackable
        // Stacked placerholders would be converted into a single item when restoring item
        placeholderNbt.putUUID("playersync:unique_id", UUID.randomUUID());

        CustomData.set(DataComponents.CUSTOM_DATA,placeholder, placeholderNbt);
        // Add display name and lore
        String placeholderItemTitleOverride = JdbcConfig.ITEM_PLACEHOLDER_TITLE_OVERRIDE.get();
        placeholder.set(DataComponents.ITEM_NAME,
                Component
                        .literal(!placeholderItemTitleOverride.isBlank()
                                ? placeholderItemTitleOverride
                                : Component.translatable("playersync.item_placeholder_title").getString())
                        .setStyle(Style.EMPTY.withColor(ChatFormatting.RED).withItalic(true)));

        List<Component> loreList = new ArrayList<>();
        String placeholderItemDetails = itemId;

        // add a stack size if it is available
        if (count > 1) {
            placeholderItemDetails = count + "x " + placeholderItemDetails;
        }

        loreList.add(
                Component.literal(placeholderItemDetails)
                        .setStyle(Style.EMPTY.withColor(ChatFormatting.GRAY).withItalic(false)));
        // add newline
        loreList.add(Component.literal(""));

        String placeholderItemDescriptionOverride = JdbcConfig.ITEM_PLACEHOLDER_DESCRIPTION_OVERRIDE.get();
        String placeholderItemDescriptionLines = ! placeholderItemDescriptionOverride.isBlank()
                ? placeholderItemDescriptionOverride
                : Component.translatable("playersync.item_placeholder_description").getString();

        for (String descriptionLine : placeholderItemDescriptionLines.split("\n")) {
            loreList.add(
                    Component.literal(descriptionLine)
                            .setStyle(Style.EMPTY.withColor(ChatFormatting.DARK_GRAY)));
        }

        placeholder.set(DataComponents.LORE,new ItemLore(loreList));

        return placeholder;
    }

    /**
     * Canary for silently-dropped item components.
     *
     * <p>Everything a modern mod keeps "inside" an item is a DataComponent: an Applied
     * Energistics storage cell holds its whole inventory in {@code ae2:storage_cell_inv},
     * a shulker box in {@code minecraft:container}, Apotheosis gear in its affix
     * components. PlayerSync round-trips those through {@code ItemStack#save} /
     * {@code ItemStack#parse}, the same path vanilla uses, so they normally survive a
     * server transfer untouched — which is precisely why a failure here is invisible:
     * the item arrives looking correct and simply empty.
     *
     * <p>This compares the component keys the stored tag declared against the ones the
     * parsed stack actually carries and reports the difference. It is expected to never
     * fire; when it does, it names the exact item and component instead of leaving an
     * admin with "my cell came back empty".
     */
    private static void verifyComponentsSurvived(ResourceLocation registryName, CompoundTag storedTag, ItemStack restored) {
        try {
            if (restored.isEmpty()) return;
            if (!storedTag.contains("components", Tag.TAG_COMPOUND)) return;
            CompoundTag components = storedTag.getCompound("components");
            if (components.isEmpty()) return;
            List<String> lost = null;
            for (String key : components.getAllKeys()) {
                // A patch entry starting with '!' is a REMOVAL, not a value to preserve.
                if (key.isEmpty() || key.charAt(0) == '!') continue;
                ResourceLocation typeId = ResourceLocation.tryParse(key);
                if (typeId == null) continue;
                var type = BuiltInRegistries.DATA_COMPONENT_TYPE.get(typeId);
                // Not registered here => the mod is absent on this server. That is the
                // placeholder path's job to report, not a silent-drop bug.
                if (type == null) continue;
                if (!restored.has(type)) {
                    if (lost == null) lost = new ArrayList<>(2);
                    lost.add(key);
                }
            }
            if (lost != null) {
                PlayerSync.LOGGER.error("[item-integrity] {} lost component(s) {} while being restored —"
                        + " the item's stored contents did not survive deserialization", registryName, lost);
                SyncLogger.dataLoss("-", "item " + registryName + " lost components " + lost + " on restore");
            }
        } catch (Throwable ignored) {
            // A diagnostic must never break a restore.
        }
    }

    /**
     * Deserializes a string from the database back into an NBT string.
     * Handles both the new Base64 format (prefixed with "B64:") and the old custom format.
     *
     * @param encoded The string retrieved from the database.
     * @return The deserialized NBT string.
     */
    public static String deserializeString(String encoded) {
        if (encoded.startsWith("B64:")) {
            String base64 = encoded.substring(4);
            try {
                return new String(Base64.getDecoder().decode(base64), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException ex) {
                PlayerSync.LOGGER.error("Base64 decoding failed for data: {}", encoded, ex);
                // fallback to legacy decoding below
            }
        }
        // Legacy fallback using custom replacement
        // cleanSnbt is applied here because legacy serialization could produce stray {"":""} type markers
        // B64-decoded data must NOT be cleaned as it contains verbatim NBT from modern mods
        return LocalJsonUtil.cleanSnbt(encoded.replace("|", ",")
                .replace("^", "\"")
                .replace("<", "{")
                .replace(">", "}")
                .replace("~", "'"));
    }

    /**
     * Serializes an NBT string for database storage.
     * Uses Base64 encoding by default (prefixed with "B64:").
     * If USE_LEGACY_SERIALIZATION config is true, uses the old custom replacement format.
     *
     * @param object The NBT string to serialize.
     * @return The serialized string.
     */
    public static String serialize(String object) {
        // Check the config option for backwards compatibility during writing
        if (JdbcConfig.USE_LEGACY_SERIALIZATION.get()) {
            // Use old custom replacement logic
            return object.replace(",", "|")
                         .replace("\"", "^")
                         .replace("{", "<")
                         .replace("}", ">")
                         .replace("'", "~");
        }

        // Base64 encode with a "B64:" marker for new data
        return "B64:" + Base64.getEncoder().encodeToString(object.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * FIX CRITICAL (performance): PlayerEvent.SaveToFile fires on the MAIN THREAD
     * during Minecraft's own autosave cycle (every 6000 ticks) and on player logout.
     * The previous implementation called store() synchronously, which includes:
     *   - Full inventory serialization
     *   - Multiple JDBC UPDATE/INSERT statements (each one a synchronous network round-trip
     *     to MySQL — 5ms to 4846ms depending on network latency)
     * With 35 players this caused MSPT spikes of up to 4846ms (97× the 50ms limit).
     *
     * NEW APPROACH:
     *   1. Update server heartbeat ASYNCHRONOUSLY (no main-thread DB call).
     *   2. If the player has been synced, snapshot all entity state on the main thread
     *      (fast — pure memory serialization, no I/O).
     *   3. Submit all DB writes to the background executor thread pool.
     *   4. The main thread NEVER waits for MySQL — it returns immediately.
     *
     * Safety: backpack / SophisticatedStorage / RS2 contents are NOT saved here
     * (they are saved completely on logout and shutdown, which is the correct moment).
     * The snapshot covers inventory, effects, XP, curios, accessories, cosmetic armor,
     * and NeoForge attachments — everything that changes frequently during gameplay.
     */
    @SubscribeEvent
    public static void onPlayerSaveToFile(PlayerEvent.SaveToFile event) {
        // 2.2.0: the vanilla autosave fires this for EVERY player in the same tick, so
        // snapshotting here produced a 35-player snapshot spike every autosave — the exact
        // spike the staggered queue exists to avoid. The player now joins that queue and is
        // snapshotted within the next few ticks with the same guard stack. Also fires right
        // after a logout (PlayerList.remove saves the .dat after the logout event); the
        // drain finds no online player for that uuid and does nothing.
        Player player = event.getEntity();
        MinecraftServer server = player.getServer();
        if (server == null || !server.isSameThread()) return;
        enqueueForStaggeredSave(player.getUUID());
    }

    /**
     * PHASE 19: optional save on respawn — gated by {@code save_on_respawn}.
     * Runs AFTER the respawn is complete so the snapshot captures the final
     * post-death inventory (vanilla drops + whatever keeping-charms preserved).
     * This OVERWRITES the pre-death snapshot taken in onPlayerDeath with the
     * correct authoritative state, so the next restore sees the real inventory.
     *
     * <p>Essential when mods like Twilight Forest's Charm of Keeping or
     * Corail Tombstone restore items on respawn — without this event,
     * PlayerSync's DB row stays at the pre-death snapshot until the next
     * auto-save, and a quick disconnect loses the keep-charm state.
     */
    @SubscribeEvent
    public static void onPlayerRespawn(PlayerEvent.PlayerRespawnEvent event) {
        try {
            if (!JdbcConfig.SAVE_ON_RESPAWN.get()) return;
            if (event.isEndConquered()) return; // End-portal exit, not a death respawn
            Player player = event.getEntity();
            SyncLogger.playerEvent(player.getUUID().toString(), "RESPAWN",
                    "Snapshot post-respawn inventory (keeping-charm / tombstone mods)");
            snapshotAndQueueSave(player, "RESPAWN");
        } catch (Exception e) {
            PlayerSync.LOGGER.warn("[respawn-save] trigger failed: {}", e.getMessage());
        }
    }

    /**
     * Phase 4: optional save on dimension change — gated by
     * {@code save_on_dimension_change} config. Protects against mid-teleport
     * crashes when the player is about to serialize into a new world file.
     */
    @SubscribeEvent
    public static void onPlayerChangeDimension(PlayerEvent.PlayerChangedDimensionEvent event) {
        try {
            if (!JdbcConfig.SAVE_ON_DIMENSION_CHANGE.get()) return;
            PlayerSync.LOGGER.debug("[dimension-change] queuing save for {} ({} -> {})",
                    event.getEntity().getUUID(), event.getFrom().location(), event.getTo().location());
            SyncLogger.playerEvent(event.getEntity().getUUID().toString(), "DIMENSION_CHANGE",
                    event.getFrom().location() + " -> " + event.getTo().location());
            snapshotAndQueueSave(event.getEntity(), "DIMENSION");
        } catch (Exception e) {
            PlayerSync.LOGGER.warn("[dimension-change] save trigger failed: {}", e.getMessage());
        }
    }

    /**
     * Phase 4: public entry point used by PeriodicSaveService and dimension-change
     * handler. Snapshots on main thread, queues async DB write with the full P0
     * guard stack (pendingLogoutSaves + online=0 + bgLock tryLock).
     *
     * @param player the player to snapshot — MUST be called on the server main thread
     * @param label  a short tag used in log lines for diagnosis (e.g. "SaveToFile",
     *               "PERIODIC", "DIMENSION")
     */
    public static void snapshotAndQueueSave(Player player, String label) {
        // 2.2.0: the heartbeat piggyback is gone. HeartbeatService already writes it on its
        // own thread every heartbeat_interval_seconds; the copy here fired one UPDATE of the
        // same server_info row per player per save.
        String puuid = player.getUUID().toString();

        if (!player.getTags().contains("player_synced")) return;
        if (syncNotCompletedPlayer.contains(puuid)) return;
        if (player.isDeadOrDying()) return;
        // FIX: Skip if a logout save is already in flight for this player.
        // Without this, the SaveToFile background task could overwrite the fresher
        // logout snapshot with a stale one if it runs after the logout save.
        if (pendingLogoutSaves.containsKey(puuid)) return;

        try {
            // === MAIN THREAD: FREEZE entity state into ItemStack copies (no serialization yet) ===
            final DeferredPlayerSnapshot frozen = snapshotPlayerData(player);
            // === BACKGROUND THREAD: serialize + all DB writes — main thread continues immediately ===
            queuePeriodicWrite(puuid, joinSessions.get(puuid), frozen, null, label);
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting player {} for {}", puuid, label, e);
        }
    }

    /**
     * Background half of every periodic save (staggered auto-save, dimension change,
     * respawn, admin flush). One implementation so the guard stack can no longer drift
     * between paths.
     *
     * <p>Guards, in order:
     * <ul>
     *   <li>the login session captured with the snapshot must still be current — a save
     *       queued before a logout, or before a quick leave + rejoin, describes a session
     *       that is over (2.2.0: replaces {@code playerLocks.containsKey}, which a rejoin
     *       re-created and which the staggered auto-save never checked);</li>
     *   <li>no logout save pending, re-checked under the per-player lock (P0-a / P0-b);</li>
     *   <li>{@code online=1 AND last_server=self} enforced INSIDE the UPDATE (P0-c). The
     *       separate {@code SELECT online} pre-check that used to do this left a gap between
     *       the check and the write and cost a round trip per save.</li>
     * </ul>
     *
     * @param storages carried-container blobs to write with the player row, or null
     */
    private static void queuePeriodicWrite(String puuid, Object session, DeferredPlayerSnapshot frozen,
                                           Map<UUID, CompoundTag> storages, String label) {
        if (session == null) return;
        executorService.submit(() -> {
            if (!isJoinSessionCurrent(puuid, session)) return;
            // FIX CRITICAL ANTI-DUP (P0-a): early skip if logout is already in flight.
            if (pendingLogoutSaves.containsKey(puuid)) return;
            ReentrantLock bgLock = getPlayerLock(puuid);
            if (!bgLock.tryLock()) return; // another save is writing this player right now
            try {
                // FIX CRITICAL ANTI-DUP (P0-b): re-check under lock — a logout task may
                // have been submitted between the check above and tryLock success.
                if (pendingLogoutSaves.containsKey(puuid) || !isJoinSessionCurrent(puuid, session)) return;
                // PHASE 18: heavy NBT serialization runs HERE on BG, not main.
                PlayerDataSnapshot snapshot = frozen.materialize();
                // PHASE 7 PERF: skip the player row when it hashes identical to the last write.
                int newHash = computeSnapshotHash(snapshot);
                Integer prev = lastWrittenSnapshotHash.get(puuid);
                if (prev != null && prev == newHash) {
                    // AUDIT FIX (dup/perf): the core hash does NOT cover backpack/SS blobs.
                    // A player moving items INSIDE a backpack leaves it unchanged, so the
                    // changed containers are still written (guarded on their own).
                    if (storages != null && !storages.isEmpty()) writeStoragesOnly(puuid, storages);
                    return;
                }
                if (writeSnapshotToDB(snapshot, false, storages, true)) {
                    lastWrittenSnapshotHash.put(puuid, newHash);
                } else {
                    SyncLogger.saveSkipped(puuid, label, "core guard blocked");
                }
            } catch (Exception e) {
                PlayerSync.LOGGER.error("Error writing {} snapshot for player {}", label, puuid, e);
            } finally {
                bgLock.unlock();
            }
        });
    }

    /**
     * Writes changed container blobs on their own, under {@code online=1 AND last_server=self}.
     * Only the blobs whose upsert actually landed are recorded as written.
     */
    private static void writeStoragesOnly(String puuid, Map<UUID, CompoundTag> storages) throws SQLException {
        ModsSupport.StorageWrite write = ModsSupport.prepareStorageWrite(
                storages, true, puuid, JdbcConfig.SERVER_ID.get(), true);
        if (write.isEmpty()) return;
        int[] counts = JDBCsetUp.executeBatchTransaction(write.statements().toArray(new Object[0][]));
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] != 0) write.markCommitted(i);
        }
    }

    /**
     * 2.2.0: set when the graceful stop begins and released once its saves are written. The
     * JVM shutdown hook waits on it: on SIGINT / SIGTERM the server runs its own graceful
     * stop at the same time as the hook, and the emergency flush used to read live entities
     * from the hook thread and write them unlocked, racing the proper shutdown save.
     */
    private static final java.util.concurrent.atomic.AtomicBoolean gracefulShutdownStarted =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    private static final CountDownLatch gracefulShutdownDone = new CountDownLatch(1);
    private static final long EMERGENCY_FLUSH_GRACE_SECONDS = 60L;
    private static final long EMERGENCY_FLUSH_START_WAIT_MS = 5_000L;

    /** What a final-state save (server stop, emergency flush) writes for one player. */
    private enum FinalSave { SKIP, NORMAL, CLEAR_ITEMS }

    /**
     * The decision {@link #onPlayerLogout} takes, applied to a server stop.
     *
     * <p>2.2.0: the stop paths skipped dead players outright and saved fallen ones normally.
     * A player on the death screen during a stop kept their pre-death inventory in the
     * database while the drops stayed in the world, and a fallen player's full inventory was
     * written right before Revive Me turned it into a corpse: both came back as duplicates
     * on the next login. Players whose restore never completed are skipped as well — what
     * they hold is the stale local copy.
     */
    private static FinalSave finalSaveFor(ServerPlayer player) {
        String uuid = player.getUUID().toString();
        if (!player.getTags().contains("player_synced") || syncNotCompletedPlayer.contains(uuid)) return FinalSave.SKIP;
        boolean fallen = isReviveMeFallen(player);
        boolean dead = player.isDeadOrDying();
        if (!fallen && !dead) return FinalSave.NORMAL;
        boolean keepInv;
        try {
            keepInv = player.serverLevel().getGameRules().getBoolean(net.minecraft.world.level.GameRules.RULE_KEEPINVENTORY);
        } catch (Exception e) {
            keepInv = false; // same fallback as the logout path
        }
        return keepInv ? FinalSave.NORMAL : FinalSave.CLEAR_ITEMS;
    }

    private static void releaseFlagOnShutdown(String uuid) {
        executorService.execute(() -> {
            try {
                JDBCsetUp.executePreparedUpdate(
                        "UPDATE " + Tables.playerData() + " SET online=0, logout_started_at=NULL WHERE uuid=? AND last_server=?",
                        uuid, JdbcConfig.SERVER_ID.get());
            } catch (SQLException e) {
                PlayerSync.LOGGER.warn("Could not release unsynced player {} on shutdown: {}", uuid, e.getMessage());
            }
        });
    }

    /** Writes one final-state save. Background thread or shutdown hook; no entity access. */
    private static boolean writeFinalSave(FinalSave mode, DeferredPlayerSnapshot frozen,
                                          Map<UUID, CompoundTag> storages) throws Exception {
        PlayerDataSnapshot snapshot = frozen.materialize();
        return mode == FinalSave.CLEAR_ITEMS
                ? writeReviveLogoutClearItemsToDB(snapshot)
                : writeSnapshotToDB(snapshot, true, storages, false);
    }

    @SubscribeEvent
    public static void onServerShutdown(ServerStoppingEvent event) throws SQLException {
        gracefulShutdownStarted.set(true);
        try {
            shutdownSaveAll();
        } finally {
            gracefulShutdownDone.countDown();
        }
    }

    private static void shutdownSaveAll() throws SQLException {
        // FIX PERF: Snapshot ALL players on main thread (fast, no DB I/O), then write
        // ALL saves in PARALLEL on background threads. Previously this was sequential:
        // 35 players × 200ms = 7 seconds blocking the main thread → watchdog "server thread stuck".
        // Now: snapshot 35 players (~50ms total), then up to 16 concurrent DB writes
        // (executor width — see the AUDIT FIX on the pool config above).
        MinecraftServer server = ServerLifecycleHooks.getCurrentServer();
        if (server != null) {
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (ServerPlayer player : server.getPlayerList().getPlayers()) {
                final FinalSave mode = finalSaveFor(player);
                if (mode == FinalSave.SKIP) {
                    // Nothing of this session may be written, but a claim this server holds
                    // must not outlive it: peers would wait for our heartbeat to go stale.
                    releaseFlagOnShutdown(player.getUUID().toString());
                    continue;
                }

                String puuid = player.getUUID().toString();
                try {
                    // === MAIN THREAD: Snapshot (entity reads, fast) ===
                    // PHASE 18: returns DeferredPlayerSnapshot — item NBT serialization happens on BG.
                    final DeferredPlayerSnapshot frozen = snapshotPlayerData(player);
                    // Backpacks / Sophisticated Storage / RS2 disks are all captured here: their
                    // SavedData is not thread-safe and RS2 encoding must not run on the writer.
                    // A corpse-bound player's containers follow the items into the corpse.
                    final Map<UUID, CompoundTag> storageSnapshots =
                            mode == FinalSave.NORMAL ? snapshotAllStorages(player) : Map.of();

                    // === BACKGROUND THREAD: DB writes (parallel across all players) ===
                    futures.add(CompletableFuture.runAsync(() -> {
                        long t0 = System.currentTimeMillis();
                        // 2.2.0: wait for an auto-save of this player that is already writing,
                        // so its older snapshot cannot commit after this one.
                        ReentrantLock bgLock = getPlayerLock(puuid);
                        bgLock.lock();
                        try {
                            boolean persisted = writeFinalSave(mode, frozen, storageSnapshots);
                            if (persisted) {
                                long dur = System.currentTimeMillis() - t0;
                                PlayerSync.LOGGER.info("Saved player {} data on server shutdown in {}ms{}", puuid, dur,
                                        mode == FinalSave.CLEAR_ITEMS ? " (fallen/dead: item columns cleared)" : "");
                                SyncLogger.saveCompleted(puuid, mode == FinalSave.CLEAR_ITEMS ? "SHUTDOWN_FALLEN" : "SHUTDOWN", dur);
                            } else {
                                PlayerSync.LOGGER.warn("Shutdown save: core guard blocked for {} — nothing written", puuid);
                                SyncLogger.saveSkipped(puuid, "SHUTDOWN", "core guard blocked");
                            }
                        } catch (Exception e) {
                            PlayerSync.LOGGER.error("Error saving player {} on shutdown", puuid, e);
                            try {
                                JDBCsetUp.executePreparedUpdate("UPDATE " + Tables.playerData() + " SET online=0 WHERE uuid=? AND last_server=?",
                                        puuid, JdbcConfig.SERVER_ID.get());
                            } catch (Exception e2) {
                                PlayerSync.LOGGER.error("CRITICAL: Failed to mark player {} offline on shutdown", puuid, e2);
                            }
                        } finally {
                            bgLock.unlock();
                        }
                    }, executorService));

                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error snapshotting player {} on shutdown", puuid, e);
                    markOfflineIfOwned(puuid, "player after a failed shutdown snapshot");
                }
            }

            // Wait for all parallel saves to complete (30s max to avoid watchdog kill)
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                PlayerSync.LOGGER.error("Timeout waiting for shutdown saves — {} tasks may not have completed", futures.size());
            } catch (Exception e) {
                PlayerSync.LOGGER.error("Error waiting for shutdown saves", e);
            }
        }
        JDBCsetUp.executePreparedUpdate("UPDATE " + Tables.serverInfo() + " SET enable=0 WHERE id=?", JdbcConfig.SERVER_ID.get());

        // Every session ends here — drop the container-storage handles so a restart starts
        // from a clean slate instead of trusting ownership recorded before the shutdown.
        ModsSupport.releaseAllSessions();

        // Phase 3: stop heartbeat before pool shutdown so its tick doesn't race with pool close.
        vip.fubuki.playersync.util.HeartbeatService.stop();
        // Phase 4: stop periodic-save scheduler before pool shutdown.
        vip.fubuki.playersync.util.PeriodicSaveService.stop();
        // Phase 5: stop pool-stats reporter.
        vip.fubuki.playersync.util.PoolStatsReporter.stop();

        // Shut down the background executors — no new tasks after this point.
        // Join tasks only wait and poll; interrupting them loses nothing (no claim is taken
        // for a session that cannot be restored any more).
        joinExecutor.shutdownNow();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            executorService.shutdownNow();
        }
        // Drain the overflow lane too (rarely holds anything — only queue-overflow spill).
        overflowExecutor.shutdown();
        try {
            if (!overflowExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                overflowExecutor.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            overflowExecutor.shutdownNow();
        }

        // Close the HikariCP pool LAST — after all DB writes are guaranteed complete.
        // Previously this was in PlayerSync.onServerStopping which could fire BEFORE
        // this handler, closing the pool while shutdown saves were still running.
        JDBCsetUp.shutdownPool();
        // FIX REGRESSION: flush+shutdown the dedicated logger here, AFTER all shutdown
        // saves have logged their completion. Previously SyncLogger.shutdown() fired in
        // PlayerSync.onServerStopping, dropping every save log entry on the floor.
        vip.fubuki.playersync.util.SyncLogger.shutdown();
    }

    /**
     * Emergency flush invoked from the JVM shutdown hook when the graceful stop did not save
     * the players (main thread hung, JVM exiting on a fatal error). Runs on the hook thread,
     * synchronously, WITHOUT the executor.
     *
     * <p>A {@code kill -9} runs no hook at all; that case is covered at the next boot by
     * {@code CrashRecovery.clearOrphanedOnlineFlags}.
     *
     * <p>2.2.0: on SIGINT / SIGTERM the server stops gracefully in parallel with this hook.
     * The flush now waits for that graceful stop and only runs if it did not finish, instead
     * of reading live entities from this thread while the server was still saving them.
     */
    public static void emergencyFlushAll() {
        try {
            MinecraftServer server = ServerLifecycleHooks.getCurrentServer();
            if (server == null) {
                PlayerSync.LOGGER.warn("[emergency-flush] no server instance — nothing to flush");
                return;
            }
            // On SIGINT / SIGTERM the server's own hook starts the graceful stop within a
            // moment. A hung main thread (watchdog exit) never starts it, and must be flushed
            // straight away: hosting panels kill the process 10-30 s after the signal.
            try {
                long startDeadline = System.currentTimeMillis() + EMERGENCY_FLUSH_START_WAIT_MS;
                while (!gracefulShutdownStarted.get() && server.getRunningThread().isAlive()
                        && System.currentTimeMillis() < startDeadline) {
                    Thread.sleep(100L);
                }
                if (gracefulShutdownStarted.get()) {
                    if (gracefulShutdownDone.await(EMERGENCY_FLUSH_GRACE_SECONDS, TimeUnit.SECONDS)) {
                        // The graceful path ran to its end (and closed the pool): whatever it
                        // could save is saved, the hook has nothing left to write with.
                        PlayerSync.LOGGER.info("[emergency-flush] graceful shutdown finished — nothing to flush");
                        return;
                    }
                    PlayerSync.LOGGER.warn("[emergency-flush] graceful shutdown did not finish within {}s — flushing from the hook",
                            EMERGENCY_FLUSH_GRACE_SECONDS);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            int flushed = 0;
            for (ServerPlayer player : new ArrayList<>(server.getPlayerList().getPlayers())) {
                String puuid = player.getUUID().toString();
                FinalSave mode = finalSaveFor(player);
                if (mode == FinalSave.SKIP) continue;
                try {
                    final DeferredPlayerSnapshot frozen = snapshotPlayerData(player);
                    final Map<UUID, CompoundTag> storageSnapshots =
                            mode == FinalSave.NORMAL ? snapshotAllStorages(player) : Map.of();
                    // Direct synchronous write (no executor, no lock) — materialize inline.
                    boolean persisted = writeFinalSave(mode, frozen, storageSnapshots);
                    if (persisted) {
                        SyncLogger.saveCompleted(puuid, "EMERGENCY_FLUSH", 0);
                        flushed++;
                    } else {
                        SyncLogger.saveSkipped(puuid, "EMERGENCY_FLUSH", "core guard blocked");
                    }
                } catch (Throwable t) {
                    PlayerSync.LOGGER.error("[emergency-flush] failed for {}: {}", puuid, t.getMessage());
                    SyncLogger.saveFailed(puuid, "EMERGENCY_FLUSH", t.getMessage());
                }
            }
            PlayerSync.LOGGER.warn("[emergency-flush] flushed {} players via shutdown hook", flushed);
        } catch (Throwable t) {
            PlayerSync.LOGGER.error("[emergency-flush] top-level failure", t);
        }
    }

    /**
     * FIX: Logout saves are now FULLY NON-BLOCKING on the main thread.
     *
     * OLD APPROACH (bad): snapshot on main thread, wait up to 15s for DB write → blocks
     * ALL server processing (ticks, other players' events) during that time.
     *
     * NEW APPROACH: snapshot on main thread (fast, pure memory), submit async DB write,
     * return immediately. The online flag stays 1 until the async save completes, which
     * naturally prevents premature rejoin via the kick mechanism + doPlayerJoin's new
     * pending-save wait logic.
     *
     * All branches now properly clean up syncNotCompletedPlayer + removePlayerLock
     * (previously leaked in the dead/sync-not-completed branches).
     */
    @SubscribeEvent
    public static void onPlayerLogout(PlayerEvent.PlayerLoggedOutEvent event) {
        String player_uuid = event.getEntity().getUUID().toString();
        // Any join task still running for this login now knows its player is gone.
        joinSessions.remove(player_uuid);

        // Players kicked for duplicate login must NOT set online=0 — they're still
        // online on the OTHER server.
        if (kickedForDuplicateLogin.remove(player_uuid)) {
            PlayerSync.LOGGER.info("Player {} was kicked for duplicate login, NOT marking offline (still on other server)", player_uuid);
            SyncLogger.playerEvent(player_uuid, "KICKED_DUPLICATE", "Player on another server, not marking offline");
            syncNotCompletedPlayer.remove(player_uuid);
            removePlayerLock(player_uuid);
            return;
        }

        if (deadPlayerWhileLogging.remove(player_uuid)) {
            PlayerSync.LOGGER.warn("A dead or dying player was kicked, uuid: {}", player_uuid);
            // FIX PERF (C1): async — main thread does not wait for MySQL.
            markOfflineIfOwned(player_uuid, "dead player");
            syncNotCompletedPlayer.remove(player_uuid);
            removePlayerLock(player_uuid);
            return;
        }

        // 2.2.0: "player_synced" is stripped at login and only set once the restore has
        // replaced the .dat state, so a session without it never restored anything. Its
        // state is the stale local copy and must not be written, exactly like a session
        // still marked sync-pending.
        boolean neverRestored = !event.getEntity().getTags().contains("player_synced");
        if (syncNotCompletedPlayer.remove(player_uuid) || neverRestored) {
            PlayerSync.LOGGER.warn("Player {} logged out with uncompleted sync. Data won't be saved for safety.", player_uuid);
            SyncLogger.saveSkipped(player_uuid, "LOGOUT", "Sync not completed — data preserved in DB, .dat data discarded");
            // FIX PERF (C1): async.
            markOfflineIfOwned(player_uuid, "unsynced player");
            removePlayerLock(player_uuid);
            return;
        }

        // === FIX DUP-REVIVE: disconnect while fallen (ReviveMe downed) or dead ===
        // Decompiling revive_me-1.21.1-5.7.14 + corpse-neoforge-1.1.13 revealed the
        // exact mechanism:
        //   - ReviveMe.CapabilityEvents.onLogout (NORMAL priority) runs on disconnect:
        //     for a fallen player it pauses the fall timer, removes effects, and — when
        //     the config dieOnDisconnect is true — calls FallenData.forceDeath().
        //   - forceDeath() applies lethal damage → LivingDeathEvent → LivingDropsEvent.
        //   - Corpse mod (DeathEvents) creates the corpse on LivingDropsEvent; its
        //     curios / cosmetic compat mods pull those items into the corpse too.
        // So on a disconnect from the fallen state the player's items are bound for a
        // corpse. If PlayerSync's normal logout-save wrote the still-attached inventory
        // (it runs at NORMAL priority — ordering vs ReviveMe's onLogout is undefined),
        // the rejoining player would get the DB copy AND the corpse would hold one =
        // duplication.
        //
        // Detection is EXACT (no heuristics): the player is fallen iff Revive Me's
        // FallenData.isFallen() says so; or already dead iff isDeadOrDying(). Either
        // state — regardless of whether PlayerSync's handler ran before or after
        // ReviveMe's forceDeath — means the items are corpse-bound. We clear every
        // item-bearing DB column (inventory / armor / left_hand / cursors / curios /
        // accessories / cosmetic_armor) so the corpse is the single source of truth.
        //
        // The matching JOIN-side guard (doPlayerJoin skips the data apply when the
        // rejoining player is still fallen) ensures the dieOnDisconnect=false path —
        // where the player reconnects STILL fallen — keeps their vanilla .dat
        // inventory instead of having it overwritten by this cleared DB row.
        //
        // keepInventory game rule: when ON, a death drops NOTHING and forms NO corpse
        // — the items stay on the player. Clearing the DB then would DESTROY them. So
        // the clear is gated on keepInventory being OFF; with it ON the normal save
        // path runs and the player keeps their inventory across the death.
        if (event.getEntity() instanceof ServerPlayer fallenOrDead) {
            // AUDIT FIX (perf): evaluate the fallen/dead state ONCE and pass the
            // booleans down — handleFallenLogout previously re-invoked the reflective
            // isReviveMeFallen just for its log line.
            final boolean logoutFallen = isReviveMeFallen(fallenOrDead);
            final boolean logoutDead = fallenOrDead.isDeadOrDying();
            if (logoutFallen || logoutDead) {
                boolean keepInv;
                try {
                    keepInv = fallenOrDead.serverLevel().getGameRules()
                            .getBoolean(net.minecraft.world.level.GameRules.RULE_KEEPINVENTORY);
                } catch (Exception e) {
                    keepInv = false; // unreadable → err toward clearing (dup is the reported bug)
                }
                if (!keepInv) {
                    handleFallenLogout(fallenOrDead, player_uuid, logoutFallen, logoutDead);
                    return;
                }
                PlayerSync.LOGGER.info("[revive-detect] player {} disconnecting fallen/dead but keepInventory=ON — normal save (items stay with player, no corpse)", player_uuid);
            }
        }

        // === Normal save path ===
        // 2.2.0: no per-player lock on the MAIN thread any more. Background auto-save,
        // SaveToFile and death tasks hold that lock across their JDBC transaction, so a
        // logout landing on one of them froze the tick for the whole write (up to the
        // socket timeout on a struggling database). It added no ordering: the background
        // logout task below takes the same lock BLOCKING before writing, and
        // pendingLogoutSaves is registered before that task is submitted. Both orders:
        //  - auto-save holds the lock first: it writes its older snapshot, releases, then
        //    the logout task writes the newer one (last writer wins = logout);
        //  - logout task holds it first: the auto-save's tryLock fails, or it acquires
        //    afterwards and its re-check sees the pending logout / the committed online=0
        //    (now also enforced inside its UPDATE) and skips.
        Player player = event.getEntity();
        // Declared outside the try so the outer catch can complete/remove the future
        // if snapshot capture or task submission fails (see FIX REGRESSION below).
        CompletableFuture<Void> saveFuture = null;
        try {
            // FIX ANTI-DUPLICATION: Force-close the disconnecting player's container FIRST.
            // If another player is viewing this player's backpack, the container stays open
            // after disconnect. Items taken after the snapshot would be duplicated.
            // Closing the container menu ensures no further modifications can occur.
            if (player instanceof ServerPlayer sp && sp.containerMenu != sp.inventoryMenu) {
                sp.closeContainer();
                SyncLogger.containerForceClosed(player_uuid, "self container on logout");
            }
            // FIX CRITICAL ANTI-DUP: close every other player's container menu if it was
            // opened against this disconnecting player's inventory/backpack. If another
            // player keeps the container open and takes items after our snapshot, those
            // items are duplicated (the snapshot contains them, and the other player has them).
            // We conservatively close all non-inventory containers referencing this player's
            // inventory slots or any menu whose class name hints at a Sophisticated Backpacks
            // container. The viewer just sees their GUI close — no data loss.
            // FIX COMPAT: Close only containers that actually reference the disconnecting
            // player's inventory/enderchest. Previous version also closed any menu whose
            // class name contained "accessor"/"curio"/... which could force-close unrelated
            // mod menus mid-transaction. The slot-reference scan is both correct and safe
            // across every modded menu.
            if (player instanceof ServerPlayer disconnecting && disconnecting.getServer() != null) {
                net.minecraft.world.entity.player.Inventory srcInv = disconnecting.getInventory();
                net.minecraft.world.SimpleContainer srcEnder = disconnecting.getEnderChestInventory();
                // PHASE 18 PERF: fast-path early return when no other player has a non-own-inventory
                // menu open. On an empty server or one where nobody is looking at someone else's
                // stuff, this saves iterating the player list + slots per logout.
                boolean anyOtherWithForeignMenu = false;
                for (ServerPlayer other : disconnecting.getServer().getPlayerList().getPlayers()) {
                    if (other == disconnecting) continue;
                    if (other.containerMenu != other.inventoryMenu) { anyOtherWithForeignMenu = true; break; }
                }
                if (anyOtherWithForeignMenu) {
                    for (ServerPlayer other : disconnecting.getServer().getPlayerList().getPlayers()) {
                        if (other == disconnecting) continue;
                        net.minecraft.world.inventory.AbstractContainerMenu menu = other.containerMenu;
                        if (menu == other.inventoryMenu) continue;
                        boolean shouldClose = false;
                        try {
                            for (net.minecraft.world.inventory.Slot slot : menu.slots) {
                                if (slot.container == srcInv || slot.container == srcEnder) {
                                    shouldClose = true;
                                    break;
                                }
                            }
                        } catch (Exception ignored) {}
                        if (shouldClose) {
                            try {
                                other.closeContainer();
                                SyncLogger.containerForceClosed(player_uuid,
                                        "viewer " + other.getUUID() + " had a menu referencing disconnecting player's inv/enderchest");
                            } catch (Exception ignored) {}
                        }
                    }
                }
            }

            // === MAIN THREAD: Snapshot ALL entity state (fast, no DB I/O) ===
            // PHASE 18: freeze on main thread (fast copies), materialize on BG.
            final DeferredPlayerSnapshot frozen = snapshotPlayerData(player);

            // Backpacks / Sophisticated Storage / RS2 disks: all captured here on the main
            // thread. Their SavedData is a plain HashMap and RS2 additionally has to run a
            // codec encode, neither of which is safe off-thread.
            final Map<UUID, CompoundTag> storageSnapshots = snapshotAllStorages(player);

            // === NON-BLOCKING: submit async save, main thread returns immediately ===
            // The online flag stays 1 until the async save completes → kick mechanism
            // prevents premature rejoin on other servers, and pendingLogoutSaves prevents
            // premature rejoin on the same server.
            //
            // FIX CRITICAL RACE (B1): Register the future in pendingLogoutSaves BEFORE
            // submitting the work. Previously runAsync was submitted first — a fast
            // reconnect could observe pendingLogoutSaves.get(uuid)==null while the save
            // was already queued → doPlayerJoin would proceed without waiting.
            saveFuture = new CompletableFuture<>();
            pendingLogoutSaves.put(player_uuid, saveFuture);

            final CompletableFuture<Void> futureRef = saveFuture;
            // FIX REGRESSION: handle RejectedExecutionException if the executor is
            // already shut down (concurrent with server stop). Without this, the future
            // stays forever in pendingLogoutSaves and blocks future rejoins for 15s+.
            // 2.2.0: the backpressure policy drops tasks silently once the executor is shut
            // down instead of throwing, so the check has to be explicit.
            try {
                if (executorService.isShutdown()) {
                    throw new java.util.concurrent.RejectedExecutionException("executor shut down");
                }
                executorService.execute(() -> {
                // PHASE 15: mark logout-in-progress for cross-server visibility. Joining
                // servers read this column to distinguish 'peer saving' from 'ghost session'.
                // 2.2.0: stamped here, when the save actually starts, instead of on the main
                // thread when it was queued: that was a blocking JDBC call on every logout,
                // and a save waiting in a busy queue looked "stalled" to peers after 10 s and
                // got its ownership force-claimed away. Guarded so it can never touch a row
                // another server has claimed meanwhile.
                markLogoutStarted(player_uuid);
                // FIX CRITICAL ANTI-DUP (P0-d): acquire bgLock BEFORE any DB write so
                // concurrent SaveToFile / death-save BG tasks (using tryLock) either skip
                // cleanly OR wait until this logout finishes. Without this, a stale
                // auto-save queued before logout could overwrite fresh logout data.
                ReentrantLock bgLock = getPlayerLock(player_uuid);
                bgLock.lock();
                try {
                    // PHASE 10 OBSERVABILITY: measure every stage so sync.log shows REAL
                    // durations instead of hardcoded 0ms. Helps diagnose user-reported
                    // 20s latencies: we can see which stage actually takes the time.
                    final long t0 = System.currentTimeMillis();
                    // PHASE 18: heavy NBT serialization runs on BG, not main thread.
                    PlayerDataSnapshot snapshot = frozen.materialize();
                    // Player row and carried containers commit together (see StorageWrite).
                    boolean persisted = writeSnapshotToDB(snapshot, true, storageSnapshots, false);
                    final long tCore = System.currentTimeMillis();
                    if (persisted) {
                        lastWrittenSnapshotHash.put(player_uuid, computeSnapshotHash(snapshot));
                        final long tEnd = System.currentTimeMillis();
                        long total = tEnd - t0;
                        PlayerSync.LOGGER.info("Logout save completed for player {} in {}ms", player_uuid, total);
                        SyncLogger.saveCompleted(player_uuid, "LOGOUT", total);
                        SyncLogger.perf("LOGOUT breakdown [" + player_uuid + "]",
                                (tCore - t0));
                        if (total > 200) {
                            String detail = "write=" + (tCore - t0)
                                    + "ms blobs=" + storageSnapshots.size() + " total=" + total + "ms";
                            PlayerSync.LOGGER.info("[perf-logout] {} {}", player_uuid, detail);
                            // PHASE 11: also log to sync.log so field reports don't miss the breakdown.
                            SyncLogger.perf("LOGOUT " + player_uuid + " " + detail, total);
                        }
                    } else {
                        PlayerSync.LOGGER.warn("Logout save skipped downstream backpack/SS/RS2 for player {} — core guard blocked",
                                player_uuid);
                        SyncLogger.saveSkipped(player_uuid, "LOGOUT", "core guard blocked (another server claimed)");
                    }
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error saving player {} data on logout", player_uuid, e);
                    SyncLogger.saveFailed(player_uuid, "LOGOUT", e.getMessage());
                    // If the atomic write failed, still try to set online=0
                    try {
                        JDBCsetUp.executePreparedUpdate("UPDATE " + Tables.playerData() + " SET online=0 WHERE uuid=? AND last_server=?",
                                player_uuid, JdbcConfig.SERVER_ID.get());
                    } catch (Exception e2) {
                        PlayerSync.LOGGER.error("CRITICAL: Failed to mark player {} offline", player_uuid, e2);
                    }
                } finally {
                    // FIX P0-d: remove playerLocks BEFORE unlocking bgLock so any
                    // auto-save BG that wakes right after unlock sees containsKey=false
                    // and skips cleanly.
                    removePlayerLock(player_uuid);
                    pendingLogoutSaves.remove(player_uuid);
                    futureRef.complete(null);
                    try { bgLock.unlock(); } catch (Exception ignored) {}
                }
                });
            } catch (java.util.concurrent.RejectedExecutionException rex) {
                // Executor is shut down (server stopping, or pool in unusable state) —
                // drain the future so no join thread is stuck waiting 15 s on .get().
                PlayerSync.LOGGER.warn("Logout save executor rejected task for player {} (likely shutdown in progress)", player_uuid);
                pendingLogoutSaves.remove(player_uuid);
                futureRef.completeExceptionally(rex);
                removePlayerLock(player_uuid);
            }

        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error during player logout save for {}", player_uuid, e);
            markOfflineIfOwned(player_uuid, "player after a failed logout snapshot");
            removePlayerLock(player_uuid);
            // FIX REGRESSION: if snapshot failed AFTER pendingLogoutSaves.put, complete
            // the future so a rejoining doPlayerJoin doesn't hang 15 s on .get().
            if (saveFuture != null) {
                pendingLogoutSaves.remove(player_uuid);
                saveFuture.completeExceptionally(e);
            }
        }
    }

    /** Stamps logout_started_at for peers, on the executor. Best effort. */
    private static void markLogoutStarted(String uuid) {
        try {
            JDBCsetUp.executePreparedUpdate(
                    "UPDATE " + Tables.playerData() + " SET logout_started_at=? WHERE uuid=? AND last_server=?",
                    System.currentTimeMillis(), uuid, JdbcConfig.SERVER_ID.get());
        } catch (Exception e) {
            PlayerSync.LOGGER.warn("[phase15] could not mark logout_started_at for {}: {}", uuid, e.getMessage());
        }
    }

    /**
     * FIX DUP-REVIVE: logout path for a player disconnecting while in Revive Me's
     * "fallen" state or already dead. The items are bound for a corpse (Revive Me's
     * forceDeath on dieOnDisconnect=true, or the post-disconnect revive-timer death
     * on dieOnDisconnect=false), so the DB item columns are explicitly cleared via
     * {@link #writeReviveLogoutClearItemsToDB} — the corpse becomes the single source
     * of truth and a later restore can no longer dup with it.
     *
     * <p>Mirrors the normal logout-save's structure (per-player lock, pendingLogoutSaves
     * future, logout_started_at marker, bg executor) so every cross-server race guard
     * still holds. Backpack / SS / RS2 snapshots are intentionally NOT captured: those
     * stores are keyed by item UUID and follow the item into the corpse.
     */
    private static void handleFallenLogout(ServerPlayer player, String player_uuid, boolean fallen, boolean dead) {
        PlayerSync.LOGGER.info("[revive-detect] player {} disconnecting while fallen/dead (fallen={}, deadOrDying={}) — clearing DB item columns to prevent corpse dup",
                player_uuid, fallen, dead);
        SyncLogger.playerEvent(player_uuid, "LOGOUT_FALLEN",
                "Disconnect while fallen/dead — DB item columns cleared (corpse dup prevention)");

        // 2.2.0: no main-thread lock, same reasoning as the normal logout path.
        CompletableFuture<Void> saveFuture = null;
        try {
            // Snapshot non-item progression. The deferred item arrays are captured too
            // (snapshotPlayerData has no non-item variant) but writeReviveLogoutClearItemsToDB
            // ignores them and writes empty item columns instead.
            final DeferredPlayerSnapshot frozen = snapshotPlayerData(player);

            saveFuture = new CompletableFuture<>();
            pendingLogoutSaves.put(player_uuid, saveFuture);

            final CompletableFuture<Void> futureRef = saveFuture;
            try {
                if (executorService.isShutdown()) {
                    throw new java.util.concurrent.RejectedExecutionException("executor shut down");
                }
                executorService.execute(() -> {
                    markLogoutStarted(player_uuid);
                    ReentrantLock bgLock = getPlayerLock(player_uuid);
                    bgLock.lock();
                    try {
                        long t0 = System.currentTimeMillis();
                        PlayerDataSnapshot snapshot = frozen.materialize();
                        boolean persisted = writeReviveLogoutClearItemsToDB(snapshot);
                        long total = System.currentTimeMillis() - t0;
                        if (persisted) {
                            // Invalidate the hash cache so a pending auto-save BG cannot
                            // resurrect the cleared inventory via the hash-skip shortcut.
                            lastWrittenSnapshotHash.remove(player_uuid);
                            PlayerSync.LOGGER.info("Fallen-logout completed for player {} in {}ms (item columns cleared)", player_uuid, total);
                            SyncLogger.saveCompleted(player_uuid, "LOGOUT_FALLEN", total);
                        } else {
                            PlayerSync.LOGGER.warn("Fallen-logout: core write blocked for {} (another server claimed)", player_uuid);
                            SyncLogger.saveSkipped(player_uuid, "LOGOUT_FALLEN", "core guard blocked");
                        }
                    } catch (Exception e) {
                        PlayerSync.LOGGER.error("Error during fallen-logout save for {}", player_uuid, e);
                        SyncLogger.saveFailed(player_uuid, "LOGOUT_FALLEN", e.getMessage());
                        try {
                            JDBCsetUp.executePreparedUpdate(
                                    "UPDATE " + Tables.playerData() + " SET online=0, logout_started_at=NULL WHERE uuid=? AND last_server=?",
                                    player_uuid, JdbcConfig.SERVER_ID.get());
                        } catch (Exception ignored) {}
                    } finally {
                        removePlayerLock(player_uuid);
                        pendingLogoutSaves.remove(player_uuid);
                        futureRef.complete(null);
                        try { bgLock.unlock(); } catch (Exception ignored) {}
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException rex) {
                PlayerSync.LOGGER.warn("Fallen-logout executor rejected task for player {} (likely shutdown in progress)", player_uuid);
                pendingLogoutSaves.remove(player_uuid);
                futureRef.completeExceptionally(rex);
                removePlayerLock(player_uuid);
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error during fallen-logout for {}", player_uuid, e);
            markOfflineIfOwned(player_uuid, "player after a failed fallen-logout snapshot");
            removePlayerLock(player_uuid);
            if (saveFuture != null) {
                pendingLogoutSaves.remove(player_uuid);
                saveFuture.completeExceptionally(e);
            }
        }
    }

    // Helper function to get the NBT string to be saved
    // If item is a placeholder, get original NBT; otherwise, get current NBT
    public static String getNbtForStorage(ItemStack itemStack) {
        // 2.2.0: every empty slot used to be gzipped from scratch on every save (~45 per
        // player per save). The bytes are identical each time — compute them once.
        if (itemStack == null || itemStack.isEmpty()) return emptySlotNbt();
        if (itemStack.is(Items.PAPER) && itemStack.getComponents().has(DataComponents.CUSTOM_DATA)
                && itemStack.getComponents().get(DataComponents.CUSTOM_DATA).contains("playersync:original_item_nbt")) {
            // It's our placeholder, retrieve the original NBT string
            return itemStack.getComponents().get(DataComponents.CUSTOM_DATA).copyTag().getString("playersync:original_item_nbt");
        } else {
            // It's a normal item or empty, serialize using binary NBT to avoid SNBT round-trip issues
            Tag tag = serializeNBT(itemStack);
            if (tag instanceof CompoundTag compoundTag) {
                return serializeTagToBinaryBase64(compoundTag);
            }
            // Fallback to SNBT-based serialization for non-compound tags
            return serialize(tag.toString());
        }
    }

    private static volatile String EMPTY_SLOT_NBT;

    private static String emptySlotNbt() {
        String cached = EMPTY_SLOT_NBT;
        if (cached == null) {
            // Same path as before: an empty stack serializes to an empty CompoundTag.
            cached = serializeTagToBinaryBase64(new CompoundTag());
            EMPTY_SLOT_NBT = cached;
        }
        return cached;
    }

    /**
     * Serializes a CompoundTag to a Base64-encoded binary NBT string.
     * This avoids SNBT round-trip issues where Tag.toString() produces SNBT
     * that TagParser.parseTag() cannot parse back (e.g. with nested lists [[{...}]]).
     */
    public static String serializeTagToBinaryBase64(CompoundTag tag) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            net.minecraft.nbt.NbtIo.writeCompressed(tag, baos);
            return "BNBT:" + Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (IOException e) {
            PlayerSync.LOGGER.error("Failed to serialize NBT to binary, falling back to SNBT", e);
            return serialize(tag.toString());
        }
    }

    /**
     * AUDIT FIX (security — zip bomb): heap quota for decompressed NBT read from the
     * DB. unlimitedHeap() allowed a crafted/corrupted row (gzip ratios up to ~1000×)
     * to allocate multi-GB tags and OOM the server. 64 MB is far beyond any
     * legitimate item / backpack payload (write side already caps the serialized
     * form at max_inventory_size_bytes, 10 MB default).
     */
    private static final long MAX_DECOMPRESSED_NBT_BYTES = 64L * 1024 * 1024;

    /**
     * Deserializes a Base64-encoded binary NBT string back to a CompoundTag.
     */
    public static CompoundTag deserializeBinaryBase64Tag(String encoded) throws IOException {
        String base64 = encoded.substring(5); // Remove "BNBT:" prefix
        byte[] bytes = Base64.getDecoder().decode(base64);
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(bytes);
        return net.minecraft.nbt.NbtIo.readCompressed(bais, net.minecraft.nbt.NbtAccounter.create(MAX_DECOMPRESSED_NBT_BYTES));
    }

    public static Tag serializeNBT(ItemStack itemStack) {
        if (itemStack == null || itemStack.isEmpty()) {
            return new CompoundTag();
        }
        // Serialize the ItemStack to NBT
        HolderLookup.Provider provider = ServerLifecycleHooks.getCurrentServer().registryAccess();
        Tag compoundTag;
        compoundTag = itemStack.save(provider);
        return compoundTag;
    }

    /**
     * Immutable snapshot of all player data, captured on the main thread.
     * Can be safely passed to a background thread for DB writes.
     */
    record PlayerDataSnapshot(
            String uuid, int xp, int score, int foodLevel, int health,
            String leftHand, String cursors,
            String equipment, String inventory, String enderChest, String effects,
            String advancements,
            // Mod data snapshots (serialized strings, thread-safe)
            String curiosData, String accessoriesData, String cosmeticArmorData, String attachmentsData,
            String persistentData
    ) {}

    /**
     * PHASE 18: frozen ItemStack copies captured on main thread; item NBT
     * serialization is deferred to the BG write task. Saves 100-250ms of
     * main-thread CPU per logout for a full inventory (69+ items × NBT→SNBT→
     * Base64 previously ran synchronously during PlayerLoggedOutEvent).
     *
     * <p>ItemStack.copy() is O(1) component clone + count snapshot — safe to
     * hand to another thread because components are effectively immutable
     * (modifications create a new ItemStack via a setter, not in-place mutation).
     *
     * <p>Curios / accessories / cosmetic / effects / attachments / advancements
     * are still pre-serialized on main thread: they either require live entity
     * access (main-thread only in NeoForge) or are small enough that deferring
     * is overkill.
     */
    record DeferredPlayerSnapshot(
            String uuid, int xp, int score, int foodLevel, int health,
            String effects, String advancements,
            // 2.2.0: mod data is deferred too — slot copies and detached tags captured on
            // main, gzip + Base64 on BG. Null means "do not write" (see each capture method).
            Map<String, ItemStack> curios, Map<String, ItemStack> accessories, Map<Integer, ItemStack> cosmeticArmor,
            CompoundTag attachments, CompoundTag persistentData,
            // Deferred — ItemStack copies, serialized to strings on BG via materialize()
            ItemStack leftHand, ItemStack cursors,
            ItemStack[] armor, ItemStack[] inventory, ItemStack[] enderChest
    ) {
        /** Serializes all deferred ItemStack arrays. Runs on the caller's thread — typically BG. */
        PlayerDataSnapshot materialize() {
            String leftHandStr = getNbtForStorage(leftHand);
            String cursorsStr  = getNbtForStorage(cursors);

            Map<Integer, String> armorMap = new HashMap<>(armor.length);
            for (int i = 0; i < armor.length; i++) armorMap.put(i, getNbtForStorage(armor[i]));

            Map<Integer, String> inventoryMap = new HashMap<>(inventory.length);
            for (int i = 0; i < inventory.length; i++) inventoryMap.put(i, getNbtForStorage(inventory[i]));

            Map<Integer, String> enderChestMap = new HashMap<>(enderChest.length);
            for (int i = 0; i < enderChest.length; i++) enderChestMap.put(i, getNbtForStorage(enderChest[i]));

            return new PlayerDataSnapshot(
                    uuid, xp, score, foodLevel, health,
                    leftHandStr, cursorsStr,
                    armorMap.toString(), inventoryMap.toString(), enderChestMap.toString(), effects,
                    advancements,
                    ModCompatSync.serializeSlots(curios), ModCompatSync.serializeSlots(accessories),
                    ModCompatSync.serializeSlots(cosmeticArmor),
                    ModCompatSync.serializeTag(attachments), ModCompatSync.serializeTag(persistentData)
            );
        }
    }

    /**
     * Captures all player data into an immutable snapshot on the MAIN THREAD.
     * PHASE 18: returns a {@link DeferredPlayerSnapshot} where the item arrays
     * are frozen via {@link ItemStack#copy()} but NOT yet serialized. The heavy
     * NBT→SNBT→Base64 work (dozens of items × several ms each) happens later
     * when the BG task calls {@code materialize()}.
     *
     * <p>Main-thread cost drops from ~200-300ms to ~20-50ms for a full inventory.
     */
    /**
     * MAIN-THREAD capture of every external container the player carries: Sophisticated
     * Backpacks, Sophisticated Storage items and Refined Storage 2 disks.
     *
     * <p>All three mods persist into the same {@code backpack_data} table keyed by the
     * container's own UUID, so merging them into one map means one batched transaction per
     * save instead of three, and one place where the main-thread/background-thread boundary
     * is enforced.
     *
     * <p>Must be called on the server main thread: the mods' SavedData are plain HashMaps
     * and the RS2 codec encode reads live storage objects.
     */
    private static Map<UUID, CompoundTag> snapshotAllStorages(Player player) {
        Map<UUID, CompoundTag> all = new HashMap<>();
        try {
            all.putAll(ModsSupport.snapshotBackpackData(player));
        } catch (Exception e) {
            PlayerSync.LOGGER.error("[snapshot-storage] backpack capture failed for {}", player.getUUID(), e);
        }
        try {
            all.putAll(ModsSupport.snapshotSSData(player));
        } catch (Exception e) {
            PlayerSync.LOGGER.error("[snapshot-storage] Sophisticated Storage capture failed for {}", player.getUUID(), e);
        }
        try {
            all.putAll(ModsSupport.snapshotRS2Disks(player));
        } catch (Exception e) {
            PlayerSync.LOGGER.error("[snapshot-storage] RS2 disk capture failed for {}", player.getUUID(), e);
        }
        return all;
    }

    private static DeferredPlayerSnapshot snapshotPlayerData(Player player) throws Exception {
        String uuid = player.getUUID().toString();
        int XP = getTotalExperience(player);
        int score = player.getScore();
        int foodLevel = player.getFoodData().getFoodLevel();
        int health = (int) player.getHealth();

        // PHASE 18: copy ItemStacks (fast component clone — no NBT serialization yet).
        ItemStack leftHandStack = player.getItemInHand(net.minecraft.world.InteractionHand.OFF_HAND).copy();
        ItemStack cursorsStack  = player.containerMenu.getCarried().copy();

        int armorSize = player.getInventory().armor.size();
        ItemStack[] armor = new ItemStack[armorSize];
        for (int i = 0; i < armorSize; i++) armor[i] = player.getInventory().armor.get(i).copy();

        int invSize = player.getInventory().items.size();
        ItemStack[] inventory = new ItemStack[invSize];
        for (int i = 0; i < invSize; i++) inventory[i] = player.getInventory().items.get(i).copy();

        int enderSize = player.getEnderChestInventory().getContainerSize();
        ItemStack[] enderChest = new ItemStack[enderSize];
        for (int i = 0; i < enderSize; i++) enderChest[i] = player.getEnderChestInventory().getItem(i).copy();
        // FIX: Don't save effects for dead/dying players. Minecraft clears effects on
        // respawn, not on death — so a dead player's getActiveEffectsMap() still returns
        // pre-death effects. Previously, the death handler and logout-while-dead path both
        // saved these stale effects to DB, causing "phantom effects" on the next login
        // (player reconnects alive with effects they should have lost on death).
        Map<Integer, String> effectMap = new HashMap<>();
        if (!player.isDeadOrDying()) {
            for (Map.Entry<Holder<MobEffect>, MobEffectInstance> entry : player.getActiveEffectsMap().entrySet()) {
                MobEffectInstance effect = entry.getValue();
                // FIX: Skip infinite-duration effects. These come from:
                // - ReviveMe mod (downed state effects with Integer.MAX_VALUE duration)
                // - Beacons (ambient effects re-applied every tick while in range)
                // - Other mods that add permanent effects
                // Syncing these across servers causes phantom effects (player gets
                // downed-state effects or beacon effects on a server without the source).
                if (effect.isInfiniteDuration()) continue;
                Tag effectTag = effect.save();
                effectMap.put(BuiltInRegistries.MOB_EFFECT.getId(entry.getKey().value()), serialize(effectTag.toString()));
            }
        }

        // PHASE 17 PERF: advancements file read — main-thread I/O was ~10-50ms per
        // snapshot on mechanical disk / slow network mount. Cache the content by
        // (absolute path + last-modified timestamp); reuse the cached string if
        // neither changed since the last snapshot. Minecraft's advancement save
        // only writes the file when something actually changed, so mtime is a
        // reliable freshness signal. PlayerAdvancements.save() is still called
        // to flush pending changes to disk.
        String advancements = null;
        if (JdbcConfig.SYNC_ADVANCEMENTS.get() && player instanceof ServerPlayer sp) {
            try { sp.getAdvancements().save(); } catch (Exception ignored) {}
            Path path = sp.getServer().getServerDirectory().resolve(getSyncWorldForServer());
            File advFile = new File(path.toFile(), "/advancements/" + uuid + ".json");
            if (advFile.exists()) {
                String absPath = advFile.getAbsolutePath();
                long mtime = advFile.lastModified();
                AdvancementsCacheEntry cached = advancementsFileCache.get(absPath);
                if (cached != null && cached.mtime == mtime && cached.content != null) {
                    advancements = cached.content;
                } else {
                    String content = new String(Files.readAllBytes(advFile.toPath()), StandardCharsets.UTF_8);
                    if (content != null && !content.isEmpty()) {
                        advancements = content;
                        advancementsFileCache.put(absPath, new AdvancementsCacheEntry(mtime, content));
                    }
                }
            }
        }

        // Mod data snapshots — entity reads, MUST be on main thread.
        // These are included in the snapshot so the background writer can persist them
        // without touching the entity again. Only copies are taken here; serialization
        // happens in materialize() on the writer thread.
        Map<String, ItemStack> curios = ModList.get().isLoaded("curios") && !player.isDeadOrDying()
                ? ModsSupport.captureCurios(player) : null;
        Map<String, ItemStack> accessories = ModCompatSync.captureAccessories(player);
        Map<Integer, ItemStack> cosmeticArmor = ModCompatSync.captureCosmeticArmor(player);
        CompoundTag attachments = ModCompatSync.captureAttachments(player);
        // Legacy persistent-data scratchpad (Corail Tombstone progression lives here).
        CompoundTag persistentData = ModCompatSync.capturePersistentData(player);

        // NOTE: Sophisticated Backpacks/Storage/RS2 saves are intentionally NOT in the
        // periodic snapshot — their contents live in server-side SavedData and are
        // always saved completely on logout / server shutdown.

        return new DeferredPlayerSnapshot(
                uuid, XP, score, foodLevel, health,
                effectMap.toString(), advancements,
                curios, accessories, cosmeticArmor, attachments, persistentData,
                leftHandStack, cursorsStack, armor, inventory, enderChest
        );
    }

    /**
     * Writes a snapshot to the DB. Runs on BACKGROUND THREAD — no entity access.
     * All data (basic + curios + mod compat) is written here in one pass.
     */
    /**
     * Writes a snapshot to the DB. Runs on BACKGROUND THREAD — no entity access.
     * All data (basic + curios + mod compat) is written here in one pass.
     *
     * FIX ANTI-DUPLICATION: All writes include AND last_server=? to prevent a stale
     * server (e.g. Server A crashing/shutting down slowly) from overwriting fresher
     * data saved by Server B after the player switched. If another server has already
     * claimed the player (changed last_server), these writes silently no-op.
     *
     * @param setOffline if true, atomically sets online=0 in the same UPDATE (used by
     *                   logout and shutdown saves). This eliminates the gap between data
     *                   write and flag set that previously allowed race conditions.
     */
    /**
     * Writes the core player snapshot to {@code player_data} (+ related tables)
     * under the {@code last_server} guard.
     *
     * @return {@code true} if the core UPDATE actually persisted rows, {@code false}
     *         if the guard blocked (another server claimed this player). Callers
     *         MUST short-circuit downstream writes (backpack / SS / RS2) when this
     *         returns {@code false} — otherwise they overwrite the claiming
     *         server's data. See P0-2 audit finding.
     */
    private static boolean writeSnapshotToDB(PlayerDataSnapshot s, boolean setOffline) throws Exception {
        return writeSnapshotToDB(s, setOffline, null, false);
    }

    /**
     * @param storages              carried-container blobs captured with the snapshot, written
     *                              in the same transaction (may be null)
     * @param skipUnchangedStorages skip blobs identical to the last written ones — periodic
     *                              saves only; final-state saves must pass false
     */
    private static boolean writeSnapshotToDB(PlayerDataSnapshot s, boolean setOffline,
                                             Map<UUID, CompoundTag> storages, boolean skipUnchangedStorages) throws Exception {
        int serverId = JdbcConfig.SERVER_ID.get();

        // PHASE 8: safety guards — abort before corrupting DB with garbage or wipes.
        // (Not applicable when the inventory is not synced here: nothing is written to it.)
        if (JdbcConfig.SYNC_INVENTORY.get() && JdbcConfig.REFUSE_EMPTY_INVENTORY_WRITE.get()
                && (s.inventory() == null || s.inventory().isEmpty() || s.inventory().length() < 4)) {
            // Only skip if DB currently has real data — new players legitimately have empty inventories
            try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(
                    "SELECT LENGTH(inventory) AS len FROM " + Tables.playerData() + " WHERE uuid=?", s.uuid())) {
                ResultSet rs = qr.resultSet();
                if (rs.next() && rs.getInt("len") > 50) {
                    SyncLogger.dataLoss(s.uuid(),
                            "REFUSED empty inventory write (DB has " + rs.getInt("len") + " bytes). Set refuse_empty_inventory_write=false to override.");
                    PlayerSync.LOGGER.warn("[write-guard] refused empty inventory write for {} (DB has {} bytes)",
                            s.uuid(), rs.getInt("len"));
                    return false;
                }
            } catch (Exception ignored) {}
        }
        int maxBytes = JdbcConfig.MAX_INVENTORY_SIZE_BYTES.get();
        if (s.inventory() != null && s.inventory().length() > maxBytes) {
            SyncLogger.nbtAnomaly(s.uuid(),
                    "inventory payload " + s.inventory().length() + " bytes exceeds max_inventory_size_bytes=" + maxBytes + " — REJECTED");
            PlayerSync.LOGGER.error("[write-guard] inventory too large for {} ({} bytes > {} max)",
                    s.uuid(), s.inventory().length(), maxBytes);
            return false;
        }

        // FIX PERF: All writes batched into a SINGLE transaction on ONE connection.
        // Previously 4-8 separate connections × round-trips per player.
        // Now: 1 connection, 1 commit, automatic rollback on failure.
        String serverGuard = "(last_server=? OR last_server IS NULL)";
        // 2.2.0: every data column goes through COALESCE so a NULL bind keeps the stored
        // value. That is how a disabled [sync_toggles] category is left alone (the servers
        // that do sync it keep their data) and how an unchanged advancements blob is not
        // re-sent on every periodic save.
        // A periodic save also requires online=1 inside the UPDATE itself: once a logout
        // has committed online=0, an older snapshot still queued can no longer land on top
        // of it. Only the background pre-check enforced that before, with a gap between
        // the check and the write.
        String coreSql = "UPDATE " + Tables.playerData() + " SET inventory=COALESCE(?, inventory), armor=COALESCE(?, armor),"
                + " xp=COALESCE(?, xp), effects=COALESCE(?, effects), enderchest=COALESCE(?, enderchest),"
                + " score=COALESCE(?, score), food_level=COALESCE(?, food_level), health=COALESCE(?, health),"
                + " advancements=COALESCE(?, advancements), left_hand=COALESCE(?, left_hand), cursors=COALESCE(?, cursors),"
                // PHASE 15: atomic clear of logout_started_at when the logout save commits.
                // Joining servers see logout_started_at=NULL + online=0 = clean, take over instantly.
                + (setOffline ? " online=0, last_server=?, logout_started_at=NULL" : " last_server=?")
                + " WHERE uuid=? AND " + serverGuard + (setOffline ? "" : " AND online=1");

        final boolean syncInventory = JdbcConfig.SYNC_INVENTORY.get();
        final boolean syncEnderChest = JdbcConfig.SYNC_ENDER_CHEST.get();
        final boolean syncXp = JdbcConfig.SYNC_XP.get();
        final boolean syncEffects = JdbcConfig.SYNC_EFFECTS.get();
        final boolean syncHealthFood = JdbcConfig.SYNC_HEALTH_FOOD.get();
        // Final-state saves (logout / shutdown) always carry the advancements.
        final String advancements = setOffline ? s.advancements() : advancementsIfChanged(s.uuid(), s.advancements());

        // Build batch of all statements
        List<Object[]> batch = new ArrayList<>();

        // 1. Core player data
        batch.add(new Object[]{coreSql,
                syncInventory ? s.inventory() : null, syncInventory ? s.equipment() : null,
                syncXp ? s.xp() : null, syncEffects ? s.effects() : null, syncEnderChest ? s.enderChest() : null,
                syncXp ? s.score() : null, syncHealthFood ? s.foodLevel() : null, syncHealthFood ? s.health() : null,
                advancements, syncInventory ? s.leftHand() : null, syncInventory ? s.cursors() : null,
                serverId, s.uuid(), serverId});

        // Side rows follow the core row's guard. A periodic save also needs online=1 here:
        // these upserts must not land after a logout committed, any more than the core row.
        // (The logout's own UPDATE sets online=0 earlier in the same transaction, so the
        // final-state saves keep the last_server guard alone.)
        final String rowGuard = setOffline ? serverGuard : serverGuard + " AND online=1";

        // 2. Curios
        // AUDIT FIX (batching): single guarded upsert instead of UPDATE + INSERT IGNORE
        // pair — halves the statements and binds the (potentially large) blob ONCE.
        // The INSERT...SELECT keeps the last_server guard: 0 rows selected = no write.
        if (s.curiosData() != null) {
            batch.add(new Object[]{
                    "INSERT INTO " + Tables.curios() + " (uuid, curios_item) SELECT ?, ? FROM " + Tables.playerData()
                            + " WHERE uuid=? AND " + rowGuard
                            + " ON DUPLICATE KEY UPDATE curios_item=VALUES(curios_item)",
                    s.uuid(), s.curiosData(), s.uuid(), serverId});
        }

        // 3. Mod compat data (Accessories, CosmeticArmor, NeoForge attachments)
        addModDataToBatch(batch, s.uuid(), "accessories", s.accessoriesData(), serverId, rowGuard);
        addModDataToBatch(batch, s.uuid(), "cosmeticarmor", s.cosmeticArmorData(), serverId, rowGuard);
        addModDataToBatch(batch, s.uuid(), "neoforge_attachments", s.attachmentsData(), serverId, rowGuard);
        addModDataToBatch(batch, s.uuid(), "persistent_data", s.persistentData(), serverId, rowGuard);

        // Execute all in one transaction. First statement is the core UPDATE on
        // player_data — if it affects 0 rows, the last_server guard blocked the write
        // (another server already claimed this player). Logging this is crucial for
        // diagnosing silent data-loss scenarios that were previously invisible.
        // 4. Carried containers, in the SAME commit as the core row (see StorageWrite).
        //    Each upsert re-checks last_server, which the core UPDATE above has just set
        //    when it passed its own guard, so a blocked core write blocks them as well.
        final ModsSupport.StorageWrite storageWrite = ModsSupport.prepareStorageWrite(
                storages, skipUnchangedStorages, s.uuid(), serverId, !setOffline);
        final int coreStatements = batch.size();
        batch.addAll(storageWrite.statements());

        int[] counts;
        try {
            counts = JDBCsetUp.executeBatchTransaction(batch.toArray(new Object[0][]));
            if (counts.length > 0 && counts[0] > 0) storageWrite.markCommitted();
        } catch (SQLException e) {
            if (storageWrite.isEmpty()) throw e;
            // One bad container blob (oversized packet, say) must not cost the player the
            // whole save. Commit the player first, then the containers one by one, still
            // guarded — the behaviour before containers joined the transaction.
            PlayerSync.LOGGER.error("Save transaction with {} container blob(s) failed for {} — retrying the player row alone",
                    storageWrite.size(), s.uuid(), e);
            counts = JDBCsetUp.executeBatchTransaction(batch.subList(0, coreStatements).toArray(new Object[0][]));
            if (counts.length > 0 && counts[0] > 0) {
                List<Object[]> storageStatements = storageWrite.statements();
                for (int i = 0; i < storageStatements.size(); i++) {
                    Object[] stmt = storageStatements.get(i);
                    try {
                        JDBCsetUp.executePreparedUpdate((String) stmt[0], stmt[1], stmt[2], stmt[3], stmt[4]);
                        storageWrite.markCommitted(i);
                    } catch (Exception e2) {
                        PlayerSync.LOGGER.error("[save-storage] container {} could not be saved for {}", stmt[1], s.uuid(), e2);
                        SyncLogger.saveFailed(s.uuid(), "STORAGE", "container " + stmt[1] + ": " + e2.getMessage());
                    }
                }
            }
        }
        if (counts.length > 0 && counts[0] == 0) {
            SyncLogger.guardBlocked(s.uuid(), serverId,
                    "core UPDATE affected 0 rows — player_data.last_server no longer matches this server or row was removed");
            PlayerSync.LOGGER.warn(
                    "PlayerSync: core write blocked by last_server guard for {} (server={}). Data was NOT persisted — another server has claimed this player.",
                    s.uuid(), serverId);
            return false;
        }
        if (advancements != null) rememberWrittenAdvancements(s.uuid(), advancements);
        return true;
    }

    /**
     * Fingerprint of the advancements blob last committed for each online player. The blob
     * runs from 100 KB to over 1 MB on a large modpack and rarely changes between two
     * periodic saves, yet it was re-sent with every one of them. Kept as a fingerprint, not
     * the string itself, so 35 players do not pin 35 MB of JSON. Only ever used to skip a
     * periodic write: logout and shutdown always send the full blob.
     */
    private record AdvancementsFingerprint(int length, int hash, long crc) {
        static AdvancementsFingerprint of(String s) {
            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(s.getBytes(StandardCharsets.UTF_8));
            return new AdvancementsFingerprint(s.length(), s.hashCode(), crc.getValue());
        }
    }

    private static final ConcurrentHashMap<String, AdvancementsFingerprint> lastWrittenAdvancements = new ConcurrentHashMap<>();

    /** @return the blob to bind, or null (keep the stored value) when it is unchanged. */
    private static String advancementsIfChanged(String uuid, String advancements) {
        if (advancements == null) return null;
        AdvancementsFingerprint last = lastWrittenAdvancements.get(uuid);
        return last != null && last.equals(AdvancementsFingerprint.of(advancements)) ? null : advancements;
    }

    private static void rememberWrittenAdvancements(String uuid, String advancements) {
        lastWrittenAdvancements.put(uuid, AdvancementsFingerprint.of(advancements));
    }

    private static void addModDataToBatch(List<Object[]> batch, String uuid, String modId, String data, int serverId, String serverGuard) {
        if (data == null) return;
        // AUDIT FIX (batching): single guarded upsert per mod_id (PK is (uuid, mod_id))
        // instead of UPDATE + INSERT IGNORE pair. The batch drops from 9 to 5 statements
        // per save and each data_value blob is bound once instead of twice.
        batch.add(new Object[]{
                "INSERT INTO " + Tables.modPlayerData() + " (uuid, mod_id, data_value) SELECT ?, ?, ? FROM " + Tables.playerData()
                        + " WHERE uuid=? AND " + serverGuard
                        + " ON DUPLICATE KEY UPDATE data_value=VALUES(data_value)",
                uuid, modId, data, uuid, serverId});
    }

    /** Backwards-compatible overload for periodic saves (no offline flag). */
    private static boolean writeSnapshotToDB(PlayerDataSnapshot s) throws Exception {
        return writeSnapshotToDB(s, false);
    }

    /**
     * FIX DUP-REVIVE: write path used when a player disconnects while in Revive Me's
     * "fallen" state OR already dead. Persists progression (xp / effects / score /
     * food / health / advancements) AND explicitly clears every item-bearing column
     * that would otherwise dup with the corpse the corpse/gravestone mod forms:
     *
     * <ul>
     *   <li>{@code player_data} — inventory / armor / left_hand / cursors</li>
     *   <li>{@code curios} — curios_item (caught into the corpse by corpsecurioscompat)</li>
     *   <li>{@code mod_player_data} where {@code mod_id IN ('accessories','cosmeticarmor')}
     *       (Accessories slots used by The Aether; Cosmetic Armor Reworked, caught by
     *       cosmeticcorpsecompat)</li>
     * </ul>
     *
     * <p>NOT touched: {@code enderchest} (does not drop on death), backpack/SS/RS2
     * (keyed by ITEM UUID — the item itself drops into the corpse with its data), and
     * {@code mod_id='neoforge_attachments'} (per-player progression: Aether portals /
     * darts / flight / life-shards, Apotheosis world tier, Ars Nouveau / Iron's
     * Spellbooks mana, etc. — never lost on death, MUST persist).
     *
     * <p>online=0 + logout_started_at=NULL are set atomically in the core UPDATE.
     * Bypasses {@code refuse_empty_inventory_write} — the empty write is intentional.
     *
     * @return true if the core UPDATE persisted, false if the last_server guard blocked.
     */
    private static boolean writeReviveLogoutClearItemsToDB(PlayerDataSnapshot s) throws Exception {
        int serverId = JdbcConfig.SERVER_ID.get();
        String serverGuard = "(last_server=? OR last_server IS NULL)";
        // "{}" — canonical empty-map encoding; LocalJsonUtil.StringToEntryMap and the
        // apply*FromData functions skip restoration when data.length() <= 2.
        // "B64:e30=" — canonical empty-item encoding (Base64 of "{}").
        final String emptyMap = "{}";
        final String emptyItem = "B64:e30=";

        List<Object[]> batch = new ArrayList<>();

        String coreSql = "UPDATE " + Tables.playerData()
                + " SET inventory=?, armor=?, left_hand=?, cursors=?,"
                + "     xp=?, effects=?, score=?, food_level=?, health=?,"
                + "     advancements=COALESCE(?, advancements),"
                + "     online=0, last_server=?, logout_started_at=NULL"
                + " WHERE uuid=? AND " + serverGuard;
        batch.add(new Object[]{coreSql,
                emptyMap, emptyMap, emptyItem, emptyItem,
                s.xp(), s.effects(), s.score(), s.foodLevel(), s.health(),
                s.advancements(), serverId,
                s.uuid(), serverId});

        String curioGuard = "EXISTS (SELECT 1 FROM " + Tables.playerData()
                + " WHERE uuid=? AND " + serverGuard + ")";
        batch.add(new Object[]{
                "UPDATE " + Tables.curios() + " SET curios_item=? WHERE uuid=? AND " + curioGuard,
                emptyMap, s.uuid(), s.uuid(), serverId});

        String modDataGuard = "EXISTS (SELECT 1 FROM " + Tables.playerData()
                + " WHERE uuid=? AND " + serverGuard + ")";
        batch.add(new Object[]{
                "UPDATE " + Tables.modPlayerData() + " SET data_value=?"
                        + " WHERE uuid=? AND mod_id=? AND " + modDataGuard,
                emptyMap, s.uuid(), "accessories", s.uuid(), serverId});
        batch.add(new Object[]{
                "UPDATE " + Tables.modPlayerData() + " SET data_value=?"
                        + " WHERE uuid=? AND mod_id=? AND " + modDataGuard,
                emptyMap, s.uuid(), "cosmeticarmor", s.uuid(), serverId});

        // Persistent data is progression, not inventory: a Tombstone player must not lose
        // their knowledge and perks because they disconnected while downed.
        addModDataToBatch(batch, s.uuid(), "persistent_data", s.persistentData(), serverId, serverGuard);

        int[] counts = JDBCsetUp.executeBatchTransaction(batch.toArray(new Object[0][]));
        if (counts.length > 0 && counts[0] == 0) {
            SyncLogger.guardBlocked(s.uuid(), serverId,
                    "revive-logout clear-items UPDATE affected 0 rows — last_server mismatch");
            return false;
        }
        return true;
    }

    /**
     * FIX DUP-DEATH: writes ONLY non-item progression fields (XP, food, score, health,
     * effects, advancements). Used by the death-save to preserve recent progress as a
     * safety net for server crashes between death and logout, WITHOUT touching the
     * inventory.
     *
     * <p>Why this matters: when a corpse / gravestone mod is loaded, items dropped on
     * death are persisted in the corpse entity / block forever. If the death-save wrote
     * the pre-death inventory to the DB, it could win a race against the logout-save's
     * post-death snapshot (empty) and the player would rejoin with their full inventory
     * AND a corpse still holding the items — duplication.
     *
     * <p>Inventory / armor / enderchest / left-hand / cursors / curios / accessories /
     * cosmetic armor / backpacks / SS / RS2 are all handled exclusively by the
     * logout-save (or shutdown-save), which captures post-drop state.
     *
     * @return true if the core write affected ≥ 1 row, false if the last_server guard
     *         blocked it (a non-fatal no-op).
     */
    private static boolean writeNonItemSnapshotToDB(PlayerDataSnapshot s) throws Exception {
        int serverId = JdbcConfig.SERVER_ID.get();
        String serverGuard = "(last_server=? OR last_server IS NULL)";
        // 2.2.0: same COALESCE / online=1 rules as writeSnapshotToDB's periodic UPDATE.
        String sql = "UPDATE " + Tables.playerData()
                + " SET xp=COALESCE(?, xp), effects=COALESCE(?, effects), score=COALESCE(?, score),"
                + "     food_level=COALESCE(?, food_level), health=COALESCE(?, health),"
                + "     advancements=COALESCE(?, advancements), last_server=?"
                + " WHERE uuid=? AND " + serverGuard + " AND online=1";
        final boolean syncXp = JdbcConfig.SYNC_XP.get();
        final boolean syncHealthFood = JdbcConfig.SYNC_HEALTH_FOOD.get();
        final String advancements = advancementsIfChanged(s.uuid(), s.advancements());
        List<Object[]> batch = new ArrayList<>(2);
        batch.add(new Object[]{sql,
                syncXp ? s.xp() : null, JdbcConfig.SYNC_EFFECTS.get() ? s.effects() : null, syncXp ? s.score() : null,
                syncHealthFood ? s.foodLevel() : null, syncHealthFood ? s.health() : null,
                advancements, serverId,
                s.uuid(), serverId});
        // Death is exactly when Corail Tombstone rewrites knowledge / alignment, so the
        // persistent-data blob belongs in the non-item death save.
        addModDataToBatch(batch, s.uuid(), "persistent_data", s.persistentData(), serverId, serverGuard + " AND online=1");
        int[] counts = JDBCsetUp.executeBatchTransaction(batch.toArray(new Object[0][]));
        if (counts.length > 0 && counts[0] == 0) {
            SyncLogger.guardBlocked(s.uuid(), serverId,
                    "death-save non-item UPDATE affected 0 rows — last_server mismatch or player already offline");
            return false;
        }
        if (advancements != null) rememberWrittenAdvancements(s.uuid(), advancements);
        return true;
    }

    private static String getSyncWorldForServer() {
        if (!JdbcConfig.SYNC_WORLD.get().isEmpty()) {
            PlayerSync.LOGGER.warn("Using configuration 'sync_world' on servers is deprecated. Please leave the array empty. Falling back to first entry.");
            return JdbcConfig.SYNC_WORLD.get().getFirst();
        }

        final MinecraftServer server = ServerLifecycleHooks.getCurrentServer();
        if (server == null) {
            PlayerSync.LOGGER.error("Unable to get current server. Assuming default level-name 'world'.");
            return "world";
        }

        final WorldData worldData = server.getWorldData();
        final String levelName = worldData.getLevelName();
        PlayerSync.LOGGER.debug("Using server level-name: {}", levelName);

        return levelName;
    }

    private static File[] scanAdvancementsFile(String player_uuid, File gameDir) {
        File[] files = new File[JdbcConfig.SYNC_WORLD.get().size()];
        for (int i = 0; i < JdbcConfig.SYNC_WORLD.get().size(); i++) {
            File advanceFile = new File(gameDir, "saves/" + JdbcConfig.SYNC_WORLD.get().get(i) + "/advancements" + "/" + player_uuid + ".json");
            if (!advanceFile.exists()) continue;
            files[i] = advanceFile;
        }
        return files;
    }

    // All periodic tasks merged into a single ServerTickEvent handler.
    // FIX: Previously used LevelTickEvent which fires once per dimension, causing the tick counter
    // to increment 3x faster than expected (once per overworld, nether, end).
    private static int autoSaveTickCounter = 0;
    private static final int AUTO_SAVE_INTERVAL_TICKS = 6000; // Every 5 minutes (20 tps × 300s)
    // FIX PERF: Staggered auto-save. Instead of snapshotting ALL 35 players in one tick
    // (770-3605ms spike → 15-36s TPS drop), we save 1 player per tick over 35 ticks
    // (22-103ms per tick → imperceptible). The queue is refilled every AUTO_SAVE_INTERVAL.
    //
    // 2.1.6: the queue holds UUIDs, not ServerPlayer references. Holding entities kept a
    // disconnected player's whole object graph (inventory, container menus, level links)
    // reachable for up to a full auto-save cycle, and the entity could go stale under us.
    // Resolving through the PlayerList at drain time is O(1) and always yields the live one.
    private static final List<UUID> autoSaveQueue = new ArrayList<>();

    /**
     * PHASE 18: public entry point for PeriodicSaveService to enqueue all online
     * players for the SAME staggered 1-player/tick drain as the vanilla auto-save.
     * Previously PeriodicSaveService called {@code snapshotAndQueueSave} for every
     * player in a single {@code server.execute}, dumping 35 snapshots into one tick
     * and causing the observable lag spike. This unifies both pathways behind the
     * existing {@link #onServerTick} staggered drain.
     *
     * <p>Must be called from the main thread (mutates the shared queue).
     * Deduplicates against the current queue so overlapping triggers don't double-
     * enqueue a player.
     */
    public static void enqueueAllOnlineForStaggeredSave(MinecraftServer server) {
        if (server == null) return;
        // Build a quick lookup of current queue UUIDs (the queue is typically small).
        java.util.Set<UUID> already = new java.util.HashSet<>(autoSaveQueue);
        int added = 0;
        for (ServerPlayer p : server.getPlayerList().getPlayers()) {
            if (already.add(p.getUUID())) {
                autoSaveQueue.add(p.getUUID());
                added++;
            }
        }
        if (added > 0) {
            PlayerSync.LOGGER.debug("[periodic-save] enqueued {} players for staggered save (queue size={})", added, autoSaveQueue.size());
        }
    }

    /** Adds one player to the staggered save queue unless already queued. Main thread only. */
    private static void enqueueForStaggeredSave(UUID uuid) {
        if (!autoSaveQueue.contains(uuid)) autoSaveQueue.add(uuid);
    }

    /**
     * {@code skip_saves_when_tps_below}: true when periodic saves should wait for the server
     * to recover. Never applies to logout, shutdown, death or respawn saves.
     */
    private static boolean periodicSavesPaused(MinecraftServer server) {
        int minTps = JdbcConfig.SKIP_SAVES_WHEN_TPS_BELOW.get();
        if (minTps <= 0 || server == null) return false;
        double msptLimit = 1000.0 / minTps;
        return server.getAverageTickTimeNanos() / 1_000_000.0 > msptLimit;
    }

    private static long lastTpsSkipLog = 0L;

    @SubscribeEvent
    public static void onServerTick(ServerTickEvent.Post event) {
        autoSaveTickCounter++;

        // PERF (A7): every 600 ticks (~30 s) drop any connectCheckCache entry older
        // than CONNECT_CHECK_TTL_MS. Stops the map from leaking when a player triggers
        // PlayerNegotiationEvent but never reaches PlayerLoggedInEvent (e.g. client
        // closes the window mid-handshake).
        if (++lastConnectCheckSweepTick >= 600L) {
            lastConnectCheckSweepTick = 0L;
            if (!connectCheckCache.isEmpty()) {
                long cutoff = System.currentTimeMillis() - CONNECT_CHECK_TTL_MS;
                connectCheckCache.entrySet().removeIf(e -> e.getValue().insertedAt() < cutoff);
            }
        }

        // 2.2.0: the tick-driven heartbeat is gone. HeartbeatService writes the same row
        // on its own thread every heartbeat_interval_seconds (this one ignored that key).

        // Auto-save: snapshot ALL entity data on MAIN THREAD (fast, no I/O), then write
        // to DB on a BACKGROUND THREAD.
        //
        // FIX PERF: Staggered auto-save — saves ONE player per tick instead of ALL at once.
        // Old behavior: 35 players snapshotted in ONE tick → 770-3605ms MSPT spike every 5 min.
        // New behavior: queue refilled every 5 min, then drained 1 player/tick → 22-103ms/tick max.
        // Backpack contents are included (prevents data loss on hard crash).
        // The vanilla autosave (SaveToFile) and PeriodicSaveService feed the same queue.
        MinecraftServer srv = ServerLifecycleHooks.getCurrentServer();
        if (autoSaveTickCounter >= AUTO_SAVE_INTERVAL_TICKS) {
            autoSaveTickCounter = 0;
            if (srv != null) {
                for (ServerPlayer p : srv.getPlayerList().getPlayers()) {
                    enqueueForStaggeredSave(p.getUUID());
                }
            }
        }

        // Process ONE player from the queue per tick (staggered)
        if (!autoSaveQueue.isEmpty()) {
            // 2.2.0: skip_saves_when_tps_below was documented but never read. The queue is
            // kept, not dropped: the players are saved as soon as the server recovers.
            if (periodicSavesPaused(srv)) {
                long now = System.currentTimeMillis();
                if (now - lastTpsSkipLog > 60_000L) {
                    lastTpsSkipLog = now;
                    PlayerSync.LOGGER.info("[auto-save] paused: average tick time above skip_saves_when_tps_below ({} players waiting)",
                            autoSaveQueue.size());
                }
                return;
            }
            UUID queued = autoSaveQueue.removeFirst();
            ServerPlayer player = srv == null ? null : srv.getPlayerList().getPlayer(queued);
            String puuid = queued.toString();

            // A player who left while queued simply is not in the list any more, so the
            // lookup returns null and the whole snapshot + DB round-trip is skipped.
            if (player != null
                    && !player.hasDisconnected() && !player.isDeadOrDying() && !syncNotCompletedPlayer.contains(puuid)
                    && !pendingLogoutSaves.containsKey(puuid) && player.getTags().contains("player_synced")) {
                try {
                    // PHASE 18: freeze on main thread (fast copies), materialize on BG.
                    final DeferredPlayerSnapshot frozen = snapshotPlayerData(player);
                    // 2.1.6: Sophisticated Storage and RS2 disks join the auto-save. They
                    // used to be persisted only at logout / shutdown, so a hard crash lost
                    // everything a player had moved into a shulker or a disk since login.
                    // Cost stays bounded: one player per tick, and unchanged blobs are
                    // skipped by their per-UUID hash.
                    final Map<UUID, CompoundTag> storageSnapshots = snapshotAllStorages(player);
                    queuePeriodicWrite(puuid, joinSessions.get(puuid), frozen, storageSnapshots, "AUTO");
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error snapshotting player {}", puuid, e);
                }
            }
        }

        // NOTE: the death-time curios cache was removed in 2.1.6. It serialized every
        // player's curios on each logout, death and shutdown into a map whose only reader
        // had itself been unreachable for several releases — pure work on the hot paths.
        // Curios are captured by snapshotPlayerData like every other slot type.
    }

    private static void setXpForPlayer(ServerPlayer serverPlayer, int databaseXp) {
        // Don't use giveExperience() as it has several side-effects:
        // triggers an event, sends network packets, increases the score, ...
        serverPlayer.totalExperience = databaseXp;
        serverPlayer.experienceLevel = 0;
        serverPlayer.experienceProgress = 0;

        int xpForLevel;

        while (databaseXp >= (xpForLevel = serverPlayer.getXpNeededForNextLevel())) {
            databaseXp -= xpForLevel;
            serverPlayer.experienceLevel++;
        }

        serverPlayer.experienceProgress = serverPlayer.experienceLevel > 0
                ? (float) databaseXp / serverPlayer.getXpNeededForNextLevel()
                : 0f;

        PlayerSync.LOGGER.debug("Giving player {} levels and {}% experience progress, calculated from {} XP.", serverPlayer.experienceLevel, serverPlayer.experienceProgress * 100, serverPlayer.totalExperience);
    }

    private static int getTotalExperience(final Player player) {
        int level = player.experienceLevel;
        int totalXp = 0;

        // Calculate total XP for completed levels
        if (level > 30) {
            totalXp = (int) (4.5 * Math.pow(level, 2) - 162.5 * level + 2220);
        } else if (level > 15) {
            totalXp = (int) (2.5 * Math.pow(level, 2) - 40.5 * level + 360);
        } else {
            totalXp = level * level + 6 * level;
        }

        // Add partial level progress
        totalXp += Math.round(player.getXpNeededForNextLevel() * player.experienceProgress);

        PlayerSync.LOGGER.debug("Experience calcuation for {} levels and {}% experience progress yields {} XP.", player.experienceLevel, player.experienceProgress * 100, totalXp);

        return totalXp;
    }

    // FIX COMPAT: priority=LOW so we run after revive mods (Revive Me / HardcoreRevival
    // / CorailTombstone) that cancel LivingDeathEvent at NORMAL/HIGH priority. NeoForge
    // bus 8.x skips canceled events at the @SubscribeEvent dispatcher level, so this
    // handler runs ONLY for un-canceled (real, finalized) deaths — exactly when the
    // death-save (non-item progression) should fire.
    @SubscribeEvent(priority = net.neoforged.bus.api.EventPriority.LOW)
    public static void onPlayerDeath(LivingDeathEvent event) {
        if (!(event.getEntity() instanceof ServerPlayer player)) return;
        String puuid = player.getUUID().toString();

        if (deadPlayerWhileLogging.contains(puuid)) return;

        // PHASE 19: honour save_on_death config. Keeping-charm / death-drop-replacement
        // mods (Twilight Forest Charm of Keeping, Corail Tombstone items, etc.) run
        // their own event handlers during LivingDeathEvent. When their priority is
        // higher than ours (LOW), they've already moved items out of the drops list
        // — our snapshot at this point captures the post-keep inventory, which is
        // usually the desired behaviour.
        // If admins diagnose a keeping-charm interaction, setting save_on_death=false
        // disables this snapshot entirely; the normal onPlayerLogout save still fires
        // on disconnect and captures the post-respawn state.
        if (!JdbcConfig.SAVE_ON_DEATH.get()) return;

        // Immediately save ALL player data on death (snapshot + async).
        // LivingDeathEvent fires BEFORE vanilla items are dropped, so the snapshot
        // captures whatever keeping-charms have already reserved + the rest.
        // This protects against: server crash after death, network disconnect before
        // onPlayerLogout fires, or any scenario where the logout handler is skipped.
        // The normal logout save will overwrite this with the final post-death state.
        if (!player.getTags().contains("player_synced")) return;
        if (syncNotCompletedPlayer.contains(puuid)) return;
        if (pendingLogoutSaves.containsKey(puuid)) return; // logout save already in flight

        ReentrantLock lock = getPlayerLock(puuid);
        if (!lock.tryLock()) return; // Skip if another save is in progress
        try {
            // FIX DUP-DEATH: death-save writes ONLY non-item progression fields, so we
            // skip the backpack / SS / RS2 main-thread snapshots that the logout-save
            // would normally produce. Saves CPU on the hot death path too.
            final DeferredPlayerSnapshot frozen = snapshotPlayerData(player);
            final Object session = joinSessions.get(puuid);

            executorService.submit(() -> {
                // 2.2.0: session check instead of playerLocks.containsKey (see queuePeriodicWrite).
                if (!isJoinSessionCurrent(puuid, session)) return;
                // FIX CRITICAL ANTI-DUP (P0-a): early skip if logout is already in flight.
                if (pendingLogoutSaves.containsKey(puuid)) return;
                ReentrantLock bgLock = getPlayerLock(puuid);
                if (!bgLock.tryLock()) return;
                try {
                    // FIX CRITICAL ANTI-DUP (P0-b): re-check under lock.
                    if (pendingLogoutSaves.containsKey(puuid) || !isJoinSessionCurrent(puuid, session)) return;
                    // FIX CRITICAL ANTI-DUP (P0-c): a committed logout (online=0) is now
                    // enforced inside writeNonItemSnapshotToDB's UPDATE itself.
                    long t0 = System.currentTimeMillis();
                    // PHASE 18: materialize the frozen snapshot on BG.
                    PlayerDataSnapshot snapshot = frozen.materialize();
                    // FIX DUP-DEATH: write ONLY non-item progression fields. Items are
                    // owned by the logout-save (post-drop / post-corpse). Writing the
                    // pre-death inventory here would dup with the corpse mod's items.
                    // Unused snapshot fields (backpackSnapshots, ssSnapshots, rs2DiskUuids,
                    // rs2Level, rs2Registry) are intentionally not persisted by the
                    // death-save: they are item-bearing and belong to the logout-save.
                    boolean persisted = writeNonItemSnapshotToDB(snapshot);
                    if (persisted) {
                        long dur = System.currentTimeMillis() - t0;
                        PlayerSync.LOGGER.info("Death-save (non-item) completed for player {} in {}ms", puuid, dur);
                        SyncLogger.saveCompleted(puuid, "DEATH", dur);
                    } else {
                        PlayerSync.LOGGER.warn("Death-save: core write blocked for {} — downstream skipped", puuid);
                        SyncLogger.saveSkipped(puuid, "DEATH", "core guard blocked");
                    }
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error death-saving player {}", puuid, e);
                } finally {
                    bgLock.unlock();
                }
            });
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting player {} on death", puuid, e);
        } finally {
            lock.unlock();
        }
    }
}
