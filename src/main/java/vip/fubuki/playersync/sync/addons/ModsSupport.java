package vip.fubuki.playersync.sync.addons;

import com.mojang.brigadier.exceptions.CommandSyntaxException;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.TagParser;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.neoforged.fml.ModList;
import top.theillusivec4.curios.api.CuriosApi;
import top.theillusivec4.curios.api.type.capability.ICuriosItemHandler;
import top.theillusivec4.curios.api.type.inventory.ICurioStacksHandler;
import top.theillusivec4.curios.api.type.inventory.IDynamicStackHandler;
import vip.fubuki.playersync.PlayerSync;
import vip.fubuki.playersync.sync.VanillaSync;
import vip.fubuki.playersync.util.JDBCsetUp;
import vip.fubuki.playersync.util.LocalJsonUtil;
import vip.fubuki.playersync.util.Tables;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Save / restore for the three mods that keep carried-container contents in a
 * world-level {@code SavedData} instead of inside the item itself:
 * Sophisticated Backpacks, Sophisticated Storage and Refined Storage 2 disks.
 * Curios slot contents (which DO live on the item) are handled here as well.
 *
 * <p>See {@link StorageOwnership} for the session model that makes an absent local
 * entry distinguishable from an empty one — the fix for the #238 duplication.
 */
public class ModsSupport {

    // =========================================================================
    // Sophisticated Backpacks — restore
    // =========================================================================

    public static void doBackPackRestore(Player player) {
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_BACKPACKS.get()) return;
        if (!ModList.get().isLoaded("sophisticatedbackpacks")) return;

        PlayerSync.LOGGER.debug("Restoring backpack data for player {}", player.getUUID());
        final String playerUuid = player.getUUID().toString();
        // PlayerInventoryProvider covers main inventory, armor, offhand and — when the
        // corresponding integration is installed — Curios slots.
        runOnBackpacksCompat(player,
                (ItemStack backpackItem, String handler, String identifier, int slot) -> {
                    restoreSingleBackpack(playerUuid, backpackItem);
                    return false;
                });
        // The ender chest is NOT part of PlayerInventoryProvider; the save side scans it,
        // so the restore side must too or the two go out of sync.
        for (int i = 0; i < player.getEnderChestInventory().getContainerSize(); i++) {
            ItemStack stack = player.getEnderChestInventory().getItem(i);
            if (!stack.isEmpty()) {
                restoreSingleBackpack(playerUuid, stack);
            }
        }
    }

    private static void restoreSingleBackpack(String playerUuid, ItemStack stack) {
        try {
            if (!isBackpackItem(stack)) return;

            net.p3pp3rf1y.sophisticatedbackpacks.backpack.wrapper.IBackpackWrapper backpackWrapper =
                    net.p3pp3rf1y.sophisticatedbackpacks.backpack.wrapper.BackpackWrapper.fromStack(stack);
            Optional<UUID> uuidOpt = backpackWrapper.getContentsUuid();
            if (uuidOpt.isEmpty()) return;
            final UUID contentsUuid = uuidOpt.get();

            final net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage store =
                    net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage.get();

            restoreStorageContents(contentsUuid,
                    (nbt) -> {
                        // BackpackStorage.setBackpackContents() upstream is a shallow MERGE, not a
                        // replace, when the UUID already exists (verified in 3.x bytecode: it copies
                        // every key of the incoming tag onto the existing one). On a server that
                        // previously loaded this backpack, old sub-tags would survive the "restore"
                        // and reappear as items. Removing first guarantees a clean replace.
                        boolean clearedViaApi = clearBackpackEntry(store, contentsUuid);
                        if (nbt.isEmpty()) {
                            // Explicit tombstone written by the save side: the container is empty.
                            // Clearing the local entry (done above) is the correct mirror; recreating
                            // it from the tag would resurrect nothing but noise.
                            StorageOwnership.mark(playerUuid, contentsUuid);
                            PlayerSync.LOGGER.debug("[restore-backpack] uuid={} empty tombstone — local entry cleared", contentsUuid);
                            return;
                        }
                        CompoundTag fresh = nbt.copy(); // never hand upstream a tag we still reference
                        store.setBackpackContents(contentsUuid, fresh);
                        StorageOwnership.mark(playerUuid, contentsUuid);
                        PlayerSync.LOGGER.debug("[restore-backpack] uuid={} nbt_keys={} cleared_via={}",
                                contentsUuid, fresh.getAllKeys().size(), clearedViaApi ? "api" : "reflection");
                    },
                    () -> {
                        // No row at all: this container has never been synced. Leave the local
                        // SavedData exactly as it is — wiping it would destroy a backpack filled
                        // between its creation and the first successful save.
                        //
                        // Deliberately NOT marked as owned: ownership means "we applied a
                        // database copy here", which is the only situation where a locally
                        // absent entry proves the container is empty. Claiming ownership on a
                        // missing row would let this server write an empty tombstone over a
                        // container whose only real contents live on the server that crashed
                        // before it could save them.
                        PlayerSync.LOGGER.debug("[restore-backpack] uuid={} absent from DB — keeping local contents untouched", contentsUuid);
                    });
        } catch (Exception e) {
            PlayerSync.LOGGER.error("[restore-backpack] unexpected error restoring backpack {}", stack, e);
        }
    }

    /**
     * Clears a {@code BackpackStorage} entry, preferring the public API and falling back
     * to a reflective map clear.
     *
     * @return true when the public API did the work, false when the reflective path was used
     */
    private static boolean clearBackpackEntry(
            net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage store, UUID contentsUuid) {
        try {
            store.removeBackpackContents(contentsUuid);
            return true;
        } catch (Throwable t) {
            PlayerSync.LOGGER.warn("Backpack removeBackpackContents failed for UUID {} ({}): falling back to reflection clear",
                    contentsUuid, t.getClass().getSimpleName());
            clearBackpackStorageReflective(store, contentsUuid);
            return false;
        }
    }

    /**
     * Reflection fallback that zeroes out the {@code BackpackStorage} entry for the
     * given UUID. Only used if the public {@code removeBackpackContents} call fails.
     */
    @SuppressWarnings("unchecked")
    private static void clearBackpackStorageReflective(
            net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage store, UUID uuid) {
        try {
            for (java.lang.reflect.Field f : store.getClass().getDeclaredFields()) {
                if (java.util.Map.class.isAssignableFrom(f.getType())) {
                    f.setAccessible(true);
                    Object map = f.get(store);
                    if (map instanceof java.util.Map<?, ?> m) {
                        ((java.util.Map<Object, Object>) m).remove(uuid);
                        ((java.util.Map<Object, Object>) m).remove(uuid.toString());
                    }
                }
            }
            store.setDirty();
        } catch (Throwable t) {
            PlayerSync.LOGGER.error("[restore-backpack] reflection clear failed for {}: {}", uuid, t.getMessage());
        }
    }

    // =========================================================================
    // Shared storage-blob plumbing (backpack_data table)
    // =========================================================================

    /**
     * Per-thread prefetch cache. A batch prefetch is performed at the start of
     * doPlayerJoin's apply phase, so each per-container restore reads from memory
     * instead of issuing its own round-trip on the main thread.
     */
    private static final ThreadLocal<java.util.Map<UUID, CompoundTag>> PREFETCH_CACHE = new ThreadLocal<>();

    /**
     * UUIDs the current prefetch actually queried. A UUID present here but absent from
     * {@link #PREFETCH_CACHE} is PROVEN to have no database row, so the restore path can
     * take its "missing" branch without a second SELECT. Before 2.1.6 every brand-new
     * container fell through to an individual query on the main thread.
     */
    private static final ThreadLocal<java.util.Set<UUID>> PREFETCH_SCOPE = new ThreadLocal<>();

    /** Installs a prefetched map for the current thread. Call {@link #clearStoragePrefetchCache} after. */
    public static void setStoragePrefetchCache(java.util.Map<UUID, CompoundTag> cache, java.util.Collection<UUID> queried) {
        PREFETCH_CACHE.set(cache);
        PREFETCH_SCOPE.set(queried == null ? java.util.Set.of() : new java.util.HashSet<>(queried));
    }

    /** Clears the per-thread prefetch cache. MUST be called from a finally block to avoid leaks. */
    public static void clearStoragePrefetchCache() {
        PREFETCH_CACHE.remove();
        PREFETCH_SCOPE.remove();
    }

    /**
     * Restores storage contents for a given UUID.
     *
     * @param callback  invoked with the stored tag when a row exists. The tag may be
     *                  EMPTY — that is the explicit "container is empty" tombstone.
     * @param onMissing invoked when the row provably does not exist, so callers can adopt
     *                  the local contents instead of wiping them.
     */
    private static void restoreStorageContents(UUID contentsUuid, StorageRestoreCallback callback, Runnable onMissing) {
        java.util.Map<UUID, CompoundTag> cache = PREFETCH_CACHE.get();
        if (cache != null) {
            CompoundTag cached = cache.get(contentsUuid);
            if (cached != null) {
                try {
                    callback.restore(cached);
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error applying cached storage for UUID {}", contentsUuid, e);
                }
                return;
            }
            java.util.Set<UUID> scope = PREFETCH_SCOPE.get();
            if (scope != null && scope.contains(contentsUuid)) {
                // Queried in the batch and not returned => no row exists. No SELECT needed.
                if (onMissing != null) onMissing.run();
                return;
            }
        }
        try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(
                "SELECT backpack_nbt FROM " + Tables.backpackData() + " WHERE uuid=?", contentsUuid.toString())) {
            ResultSet rs = qr.resultSet();
            if (!rs.next()) {
                if (onMissing != null) onMissing.run();
                return;
            }
            String serialized = rs.getString("backpack_nbt");
            if (serialized == null) {
                if (onMissing != null) onMissing.run();
                return;
            }
            callback.restore(deserializeStorageBlob(contentsUuid, serialized));
        } catch (SQLException e) {
            PlayerSync.LOGGER.error("Error restoring storage data for UUID {}", contentsUuid, e);
        } catch (CommandSyntaxException e) {
            PlayerSync.LOGGER.error("Error parsing storage NBT for UUID {}. Skipping.", contentsUuid, e);
        } catch (IOException e) {
            PlayerSync.LOGGER.error("Error reading binary storage NBT for UUID {}. Skipping.", contentsUuid, e);
        }
    }

    private static CompoundTag deserializeStorageBlob(UUID contentsUuid, String serialized)
            throws IOException, CommandSyntaxException {
        if (serialized.startsWith("BNBT:")) {
            return VanillaSync.deserializeBinaryBase64Tag(serialized);
        }
        String nbtString = VanillaSync.deserializeString(serialized);
        try {
            return TagParser.parseTag(nbtString);
        } catch (CommandSyntaxException ex) {
            PlayerSync.LOGGER.warn("TagParser failed for storage UUID {}, trying fallback", contentsUuid);
            return net.minecraft.nbt.NbtUtils.snbtToStructure(nbtString);
        }
    }

    @FunctionalInterface
    private interface StorageRestoreCallback {
        void restore(CompoundTag nbt);
    }

    /**
     * Batch-fetches storage contents (backpack / Sophisticated Storage / RS2 all share the
     * {@code backpack_data} table) for a list of UUIDs in ONE query. Called from the restore
     * path so a player carrying several containers costs a single round-trip.
     *
     * @return map {uuid -> deserialized CompoundTag}; missing UUIDs are absent from the map
     */
    public static java.util.Map<UUID, CompoundTag> prefetchStorageContents(java.util.Collection<UUID> uuids) {
        java.util.Map<UUID, CompoundTag> out = new java.util.HashMap<>();
        if (uuids == null || uuids.isEmpty()) return out;
        java.util.List<UUID> unique = new java.util.ArrayList<>(new java.util.LinkedHashSet<>(uuids));
        StringBuilder placeholders = new StringBuilder("?");
        for (int i = 1; i < unique.size(); i++) placeholders.append(",?");
        String sql = "SELECT uuid, backpack_nbt FROM " + Tables.backpackData() + " WHERE uuid IN (" + placeholders + ")";
        Object[] params = new Object[unique.size()];
        for (int i = 0; i < unique.size(); i++) params[i] = unique.get(i).toString();
        try (JDBCsetUp.QueryResult qr = JDBCsetUp.executePreparedQuery(sql, params)) {
            ResultSet rs = qr.resultSet();
            while (rs.next()) {
                String uuidStr = rs.getString("uuid");
                String serialized = rs.getString("backpack_nbt");
                if (serialized == null) continue;
                try {
                    out.put(UUID.fromString(uuidStr), deserializeStorageBlob(UUID.fromString(uuidStr), serialized));
                } catch (Exception e) {
                    PlayerSync.LOGGER.warn("[prefetch-storage] failed to parse NBT for {}: {}", uuidStr, e.getMessage());
                }
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("[prefetch-storage] batch SELECT failed for {} uuid(s)", unique.size(), e);
        }
        return out;
    }

    /**
     * Per-storage-UUID hash of the last successfully written blob. The core snapshot hash
     * in VanillaSync does NOT cover backpack / Sophisticated Storage / RS2 data, so without
     * this every effective auto-save rewrote every MEDIUMBLOB unconditionally — the dominant
     * write volume on container-heavy servers. Entries are written in BOTH modes (a logout
     * write primes the next session's skip) but only consulted when {@code skipUnchanged}
     * is true, so logout / shutdown / emergency paths keep their always-write semantics.
     *
     * <p>Entries are evicted with the player session ({@link #releaseSession}) so the map
     * cannot grow without bound on a long-running hub, and so a blob another server changed
     * while the player was away is never skipped on the strength of a stale local hash.
     */
    private static final java.util.concurrent.ConcurrentHashMap<UUID, Integer> lastWrittenStorageHash =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Releases every per-session structure held for a player: storage ownership handles and
     * the write-skip hashes of the containers they were carrying. Called when the logout save
     * finishes so nothing leaks across sessions.
     */
    public static void releaseSession(String playerUuid) {
        for (UUID storageUuid : StorageOwnership.view(playerUuid)) {
            lastWrittenStorageHash.remove(storageUuid);
        }
        StorageOwnership.clearPlayer(playerUuid);
    }

    /** Full reset — server shutdown only. */
    public static void releaseAllSessions() {
        lastWrittenStorageHash.clear();
        StorageOwnership.clearAll();
    }

    /** Saves pre-snapshotted storage blobs. Safe on a background thread (no entity access). */
    public static void saveBackpackSnapshots(Map<UUID, CompoundTag> snapshots) {
        saveBackpackSnapshots(snapshots, false);
    }

    /**
     * @param skipUnchanged when true (auto-save paths only), blobs whose serialized form
     *                      hashes identical to the last written one are skipped. Final-state
     *                      paths (logout / shutdown / emergency flush) MUST pass false.
     */
    public static void saveBackpackSnapshots(Map<UUID, CompoundTag> snapshots, boolean skipUnchanged) {
        // Every upsert goes into ONE transaction instead of N separate round-trips: with
        // 3 backpacks + 2 shulkers + 4 disks a logout save used to do 9 sequential commits.
        if (snapshots == null || snapshots.isEmpty()) return;
        List<Object[]> batch = new ArrayList<>(snapshots.size());
        List<UUID> batchUuids = new ArrayList<>(snapshots.size());
        List<Integer> batchHashes = new ArrayList<>(snapshots.size());
        int unchangedSkips = 0;
        for (Map.Entry<UUID, CompoundTag> entry : snapshots.entrySet()) {
            UUID uuid = entry.getKey();
            CompoundTag nbt = entry.getValue();
            if (nbt == null) continue;
            // NOTE: an EMPTY tag is written on purpose — it is the tombstone that tells the
            // next restore "this container is empty, clear your local copy". The pre-2.1.6
            // guard skipped it whenever the DB still held data, which is exactly how an
            // emptied backpack got its old contents re-injected on the next join (#238).
            // The snapshot side is the only place allowed to decide that a container is
            // genuinely empty; see StorageOwnership.
            try {
                String serialized = VanillaSync.serializeTagToBinaryBase64(nbt);
                int h = serialized.hashCode();
                if (skipUnchanged) {
                    Integer prev = lastWrittenStorageHash.get(uuid);
                    if (prev != null && prev == h) {
                        unchangedSkips++;
                        continue;
                    }
                }
                batch.add(new Object[]{
                        "INSERT INTO " + Tables.backpackData() + " (uuid, backpack_nbt) VALUES (?, ?)"
                                + " ON DUPLICATE KEY UPDATE backpack_nbt=VALUES(backpack_nbt)",
                        uuid.toString(), serialized});
                batchUuids.add(uuid);
                batchHashes.add(h);
            } catch (Exception e) {
                PlayerSync.LOGGER.error("Error preparing storage save for UUID {}", uuid, e);
            }
        }
        if (unchangedSkips > 0) {
            PlayerSync.LOGGER.debug("[save-storage] skipped {} unchanged blobs (per-UUID hash)", unchangedSkips);
        }
        if (batch.isEmpty()) return;
        try {
            JDBCsetUp.executeBatchTransaction(batch.toArray(new Object[0][]));
            for (int i = 0; i < batchUuids.size(); i++) {
                lastWrittenStorageHash.put(batchUuids.get(i), batchHashes.get(i));
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("[save-storage] batch transaction failed ({} entries)", batch.size(), e);
            // Fall back to per-entry writes so at least some survive
            for (int i = 0; i < batch.size(); i++) {
                Object[] stmt = batch.get(i);
                try {
                    JDBCsetUp.executePreparedUpdate((String) stmt[0], stmt[1], stmt[2]);
                    lastWrittenStorageHash.put(batchUuids.get(i), batchHashes.get(i));
                } catch (Exception e2) {
                    PlayerSync.LOGGER.error("[save-storage] fallback write failed for {}", stmt[1], e2);
                }
            }
        }
    }

    /**
     * Container blobs prepared for a write that is guarded by the player's {@code last_server}
     * and runs inside the same transaction as the player's core row.
     *
     * <p>2.2.0: container contents used to be committed in their own unguarded transaction
     * AFTER the core row. On logout the core row already said {@code online=0}, so a peer
     * could claim the player in between, read the previous container contents and keep the
     * items the player had just taken out of them: duplication. Written in the same commit,
     * a peer sees either both or neither.
     */
    public static final class StorageWrite {
        private final List<Object[]> statements = new ArrayList<>();
        private final List<UUID> uuids = new ArrayList<>();
        private final List<Integer> hashes = new ArrayList<>();

        public boolean isEmpty() { return statements.isEmpty(); }
        public int size() { return statements.size(); }
        public List<Object[]> statements() { return statements; }

        /** Records the written hashes once the enclosing transaction has committed. */
        public void markCommitted() {
            for (int i = 0; i < uuids.size(); i++) {
                lastWrittenStorageHash.put(uuids.get(i), hashes.get(i));
            }
        }

        /** Same as {@link #markCommitted} for the statement at {@code index} only. */
        public void markCommitted(int index) {
            lastWrittenStorageHash.put(uuids.get(index), hashes.get(index));
        }
    }

    /**
     * @param requireOnline also require {@code online=1}. Needed when the write runs alone
     *                      (an auto-save), not when it follows the logout's own core UPDATE
     *                      in the same transaction, which has just set {@code online=0}.
     */
    public static StorageWrite prepareStorageWrite(Map<UUID, CompoundTag> snapshots, boolean skipUnchanged,
                                                   String playerUuid, int serverId, boolean requireOnline) {
        StorageWrite write = new StorageWrite();
        if (snapshots == null || snapshots.isEmpty()) return write;
        String sql = "INSERT INTO " + Tables.backpackData() + " (uuid, backpack_nbt) SELECT ?, ? FROM "
                + Tables.playerData() + " WHERE uuid=? AND last_server=?" + (requireOnline ? " AND online=1" : "")
                + " ON DUPLICATE KEY UPDATE backpack_nbt=VALUES(backpack_nbt)";
        int unchangedSkips = 0;
        for (Map.Entry<UUID, CompoundTag> entry : snapshots.entrySet()) {
            UUID uuid = entry.getKey();
            CompoundTag nbt = entry.getValue();
            if (nbt == null) continue;
            // An EMPTY tag is written on purpose: it is the tombstone that tells the next
            // restore "this container is empty" (#238). See saveBackpackSnapshots.
            try {
                String serialized = VanillaSync.serializeTagToBinaryBase64(nbt);
                int h = serialized.hashCode();
                if (skipUnchanged) {
                    Integer prev = lastWrittenStorageHash.get(uuid);
                    if (prev != null && prev == h) {
                        unchangedSkips++;
                        continue;
                    }
                }
                write.statements.add(new Object[]{sql, uuid.toString(), serialized, playerUuid, serverId});
                write.uuids.add(uuid);
                write.hashes.add(h);
            } catch (Exception e) {
                PlayerSync.LOGGER.error("Error preparing storage save for UUID {}", uuid, e);
            }
        }
        if (unchangedSkips > 0) {
            PlayerSync.LOGGER.debug("[save-storage] skipped {} unchanged blobs (per-UUID hash)", unchangedSkips);
        }
        return write;
    }

    /** Sophisticated Storage shares the {@code backpack_data} table and the same writer. */
    public static void saveSSSnapshots(Map<UUID, CompoundTag> snapshots) {
        saveBackpackSnapshots(snapshots);
    }

    // =========================================================================
    // Non-creating existence probes
    // =========================================================================

    /**
     * {@code BackpackStorage#getOrCreateBackpackContents} is a {@code computeIfAbsent} that
     * INSERTS an empty tag and marks the SavedData dirty when the UUID is unknown (verified
     * in 3.x bytecode). Calling it just to look at a backpack therefore both pollutes the
     * mod's SavedData and makes "never loaded here" indistinguishable from "empty" — the
     * root of the #238 duplication. This probe reads the internal map without touching it.
     *
     * @return TRUE / FALSE when the answer is known, {@code null} when reflection is
     *         unavailable (upstream refactor) so callers fall back to the legacy behaviour
     *         instead of guessing.
     */
    private static volatile java.lang.reflect.Field bpContentsField;
    private static volatile boolean bpContentsFieldResolved = false;

    private static java.lang.reflect.Field resolveBackpackContentsField(Object store) {
        if (bpContentsFieldResolved) return bpContentsField;
        synchronized (ModsSupport.class) {
            if (bpContentsFieldResolved) return bpContentsField;
            for (java.lang.reflect.Field candidate : store.getClass().getDeclaredFields()) {
                if (!java.util.Map.class.isAssignableFrom(candidate.getType())) continue;
                // backpackContents is Map<UUID, CompoundTag>; accessLogRecords is
                // Map<UUID, AccessLogRecord>. Match on the VALUE type so we never probe the
                // wrong map after an upstream field rename.
                if (candidate.getGenericType() instanceof java.lang.reflect.ParameterizedType pt) {
                    java.lang.reflect.Type[] args = pt.getActualTypeArguments();
                    if (args.length == 2 && args[1] == CompoundTag.class) {
                        candidate.setAccessible(true);
                        bpContentsField = candidate;
                        break;
                    }
                }
            }
            bpContentsFieldResolved = true;
            if (bpContentsField == null) {
                PlayerSync.LOGGER.warn("[storage-probe] BackpackStorage contents map not found — falling back"
                        + " to legacy empty-tag handling for backpacks (upstream layout changed?)");
            }
            return bpContentsField;
        }
    }

    /**
     * Reads a backpack entry WITHOUT creating it.
     *
     * @return the live tag, or {@code null} when no entry exists (distinct from an empty
     *         tag, which means "exists and is empty")
     */
    private static CompoundTag peekBackpackContents(
            net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage store, UUID uuid) {
        java.lang.reflect.Field f = resolveBackpackContentsField(store);
        if (f == null) {
            // Legacy path: may create an empty entry upstream, same as pre-2.1.6.
            return store.getOrCreateBackpackContents(uuid);
        }
        try {
            Object map = f.get(store);
            if (map instanceof java.util.Map<?, ?> m) {
                Object v = m.get(uuid);
                return v instanceof CompoundTag ct ? ct : null;
            }
        } catch (Throwable ignored) {}
        return store.getOrCreateBackpackContents(uuid);
    }

    // =========================================================================
    // Sophisticated Backpacks — snapshot (main thread)
    // =========================================================================

    /**
     * Collects Sophisticated Backpack UUIDs AND snapshots their contents. MUST run on the
     * MAIN THREAD: it reads the player inventory and the mod's SavedData, neither of which
     * is thread-safe.
     *
     * <p>Reading the SavedData here (rather than on the writer thread) also closes the race
     * where another player viewing the same backpack modified it between the main-thread
     * scan and an asynchronous read.
     */
    public static Map<UUID, CompoundTag> snapshotBackpackData(Player player) {
        Map<UUID, CompoundTag> data = new HashMap<>();
        if (!ModList.get().isLoaded("sophisticatedbackpacks")) return data;
        // Same toggle as the restore side. Saving while the restore is disabled would leave
        // the two halves asymmetric: this server would keep publishing blobs it never reads
        // back, so its stale local copy would eventually overwrite everyone else's.
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_BACKPACKS.get()) return data;
        final String playerUuid = player.getUUID().toString();
        try {
            net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage store =
                    net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage.get();
            runOnBackpacksCompat(player,
                    (ItemStack backpackItem, String handler, String identifier, int slot) -> {
                        snapshotSingleBackpack(playerUuid, store, backpackItem, data);
                        return false;
                    });
            // PlayerInventoryProvider does NOT include the ender chest.
            for (int i = 0; i < player.getEnderChestInventory().getContainerSize(); i++) {
                ItemStack stack = player.getEnderChestInventory().getItem(i);
                if (stack.isEmpty()) continue;
                snapshotSingleBackpack(playerUuid, store, stack, data);
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting backpack data for player {}", player.getUUID(), e);
        }
        return data;
    }

    private static void snapshotSingleBackpack(String playerUuid,
                                               net.p3pp3rf1y.sophisticatedbackpacks.backpack.BackpackStorage store,
                                               ItemStack stack, Map<UUID, CompoundTag> data) {
        try {
            if (!isBackpackItem(stack)) return;

            net.p3pp3rf1y.sophisticatedbackpacks.backpack.wrapper.IBackpackWrapper wrapper =
                    net.p3pp3rf1y.sophisticatedbackpacks.backpack.wrapper.BackpackWrapper.fromStack(stack);
            // Resets the IO handler cache and runs the change callbacks so the live tag in
            // BackpackStorage reflects any pending upgrade-driven movement.
            try { wrapper.refreshInventoryForInputOutput(); } catch (Exception ignored) {}

            Optional<UUID> uuidOpt = wrapper.getContentsUuid();
            if (uuidOpt.isEmpty()) return;
            UUID uuid = uuidOpt.get();

            CompoundTag live = peekBackpackContents(store, uuid);
            if (live != null && !live.isEmpty()) {
                data.put(uuid, live.copy()); // .copy() freezes the state for the writer thread
                return;
            }
            // Nothing (or nothing meaningful) stored locally. Whether that means "empty" or
            // "this server never loaded it" is exactly what StorageOwnership answers.
            if (StorageOwnership.isOwned(playerUuid, uuid)) {
                // We established this container this session, so an empty local entry is the
                // truth: write the tombstone and let the next restore clear its copy.
                data.put(uuid, new CompoundTag());
            } else {
                // Never restored here — the database copy is authoritative, leave it alone.
                PlayerSync.LOGGER.debug("[snapshot-backpack] uuid={} not established this session — preserving DB row", uuid);
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.debug("[snapshot-backpack] skipped a backpack: {}", e.toString());
        }
    }

    private static volatile java.lang.reflect.Method runOnBackpacksMethod;

    /**
     * Calls PlayerInventoryProvider.runOnBackpacks(Player, BackpackInventorySlotConsumer)
     * reflectively. Newer Sophisticated Backpacks releases changed the return type from
     * void to boolean, which breaks a direct call compiled against the older API with a
     * NoSuchMethodError. Resolving by name and parameter types works with both.
     */
    private static void runOnBackpacksCompat(Player player,
            net.p3pp3rf1y.sophisticatedbackpacks.util.PlayerInventoryProvider.BackpackInventorySlotConsumer consumer) {
        try {
            java.lang.reflect.Method m = runOnBackpacksMethod;
            if (m == null) {
                m = net.p3pp3rf1y.sophisticatedbackpacks.util.PlayerInventoryProvider.class.getMethod("runOnBackpacks",
                        Player.class,
                        net.p3pp3rf1y.sophisticatedbackpacks.util.PlayerInventoryProvider.BackpackInventorySlotConsumer.class);
                runOnBackpacksMethod = m;
            }
            m.invoke(net.p3pp3rf1y.sophisticatedbackpacks.util.PlayerInventoryProvider.get(), player, consumer);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IllegalStateException(cause);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Sophisticated Backpacks runOnBackpacks API not found", e);
        }
    }

    private static boolean isBackpackItem(ItemStack stack) {
        if (stack == null || stack.isEmpty()) return false;
        net.minecraft.resources.ResourceLocation loc =
                net.minecraft.core.registries.BuiltInRegistries.ITEM.getKey(stack.getItem());
        return loc != null && loc.getNamespace().equals("sophisticatedbackpacks");
    }

    /**
     * Backpack UUID collection without a snapshot. Used by the restore path to prefetch
     * storage contents in bulk.
     */
    public static java.util.List<UUID> collectBackpackUuids(Player player, boolean includeEnderChest) {
        java.util.List<UUID> uuids = new java.util.ArrayList<>();
        if (!ModList.get().isLoaded("sophisticatedbackpacks")) return uuids;
        try {
            runOnBackpacksCompat(player,
                    (ItemStack stack, String handler, String identifier, int slot) -> {
                        addBackpackUuid(stack, uuids);
                        return false;
                    });
            if (includeEnderChest) {
                for (int i = 0; i < player.getEnderChestInventory().getContainerSize(); i++) {
                    addBackpackUuid(player.getEnderChestInventory().getItem(i), uuids);
                }
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.warn("[collect-backpack-uuids] scan failed: {}", e.getMessage());
        }
        return uuids;
    }

    private static void addBackpackUuid(ItemStack stack, java.util.List<UUID> out) {
        try {
            if (!isBackpackItem(stack)) return;
            net.p3pp3rf1y.sophisticatedbackpacks.backpack.wrapper.IBackpackWrapper wrapper =
                    net.p3pp3rf1y.sophisticatedbackpacks.backpack.wrapper.BackpackWrapper.fromStack(stack);
            wrapper.getContentsUuid().ifPresent(out::add);
        } catch (Exception ignored) {}
    }

    // =========================================================================
    // Curios
    // =========================================================================

    /**
     * Snapshots Curios data into a serialized string on the main thread (no DB write).
     * Returns null when the data must NOT be written (handler unavailable), so the writer
     * preserves whatever is already stored rather than wiping a real record.
     */
    public static String snapshotCuriosData(Player player) {
        return ModCompatSync.serializeSlots(captureCurios(player));
    }

    /**
     * Main-thread half of the Curios snapshot: frozen copies of the occupied slots,
     * serialized on the writer thread. {@code null} means "do not write": handler
     * unavailable, or {@code sync_curios} off on this server (2.2.0: the toggle used to gate
     * the restore only, so a server with it off kept publishing curios it never read back).
     */
    public static Map<String, ItemStack> captureCurios(Player player) {
        if (!ModList.get().isLoaded("curios")) return null;
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_CURIOS.get()) return null;
        Optional<ICuriosItemHandler> handlerOpt = CuriosApi.getCuriosInventory(player);
        // If the handler could not be resolved (capability not yet attached, or a Curios
        // issue), return null so the writer SKIPS the write and preserves the DB. Returning
        // "{}" here would overwrite a legitimate record with an empty one.
        if (handlerOpt.isEmpty()) {
            PlayerSync.LOGGER.warn("Curios handler unavailable while snapshotting {} — skipping curios write", player.getUUID());
            return null;
        }
        Map<String, ItemStack> slots = new HashMap<>();
        ICuriosItemHandler handler = handlerOpt.get();
        // BOTH functional and cosmetic stacks are captured. Cosmetic slots are identified by
        // the "cos:" prefix in the composite key so apply/clear can tell them apart without
        // a schema change.
        handler.getCurios().forEach((slotType, stacksHandler) -> {
            IDynamicStackHandler dynStacks = stacksHandler.getStacks();
            for (int i = 0; i < dynStacks.getSlots(); i++) {
                ItemStack stack = dynStacks.getStackInSlot(i);
                if (!stack.isEmpty()) {
                    slots.put(slotType + ":" + i, stack.copy());
                }
            }
            IDynamicStackHandler cosStacks = stacksHandler.getCosmeticStacks();
            for (int i = 0; i < cosStacks.getSlots(); i++) {
                ItemStack stack = cosStacks.getStackInSlot(i);
                if (!stack.isEmpty()) {
                    slots.put("cos:" + slotType + ":" + i, stack.copy());
                }
            }
        });
        return slots;
    }

    /** Applies pre-read curios data to the player entity (NO DB access). */
    public static void applyCuriosFromData(Player player, String curiosData) {
        if (!ModList.get().isLoaded("curios")) return;
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_CURIOS.get()) return;

        Optional<ICuriosItemHandler> handlerOpt = CuriosApi.getCuriosInventory(player);
        if (handlerOpt.isEmpty()) {
            PlayerSync.LOGGER.warn("Could not get Curios handler for player {} during apply", player.getUUID());
            return;
        }

        ICuriosItemHandler handler = handlerOpt.get();

        // Clear BOTH functional and cosmetic stacks first, even when the DB data is empty.
        // Without this, stale curios loaded from the .dat persist when the DB has no entry
        // and duplicate across servers.
        for (Map.Entry<String, ICurioStacksHandler> entry : handler.getCurios().entrySet()) {
            IDynamicStackHandler stacks = entry.getValue().getStacks();
            for (int i = 0; i < stacks.getSlots(); i++) {
                stacks.setStackInSlot(i, ItemStack.EMPTY);
            }
            IDynamicStackHandler cos = entry.getValue().getCosmeticStacks();
            for (int i = 0; i < cos.getSlots(); i++) {
                cos.setStackInSlot(i, ItemStack.EMPTY);
            }
        }

        if (curiosData == null || curiosData.length() <= 2) return;

        Map<String, String> storedMap = LocalJsonUtil.StringToMap(curiosData);
        if (storedMap.isEmpty()) return;

        for (Map.Entry<String, String> entry : storedMap.entrySet()) {
            String compositeKey = entry.getKey();
            boolean cosmetic = compositeKey.startsWith("cos:");
            String remaining = cosmetic ? compositeKey.substring(4) : compositeKey;
            int lastColon = remaining.lastIndexOf(':');
            if (lastColon < 0) continue;
            String slotType = remaining.substring(0, lastColon);
            int slotIndex;
            try { slotIndex = Integer.parseInt(remaining.substring(lastColon + 1)); }
            catch (NumberFormatException e) { continue; }

            try {
                ItemStack stack = VanillaSync.deserializeAndCreatePlaceholderIfNeeded(entry.getValue());
                ICurioStacksHandler stacksHandler = handler.getCurios().get(slotType);
                if (stacksHandler != null) {
                    IDynamicStackHandler stacks = cosmetic
                            ? stacksHandler.getCosmeticStacks()
                            : stacksHandler.getStacks();
                    if (slotIndex < stacks.getSlots()) {
                        stacks.setStackInSlot(slotIndex, stack);
                    }
                }
            } catch (Exception e) {
                PlayerSync.LOGGER.error("Error applying curios slot {} ({}:{})", compositeKey, slotType, slotIndex, e);
            }
        }
        PlayerSync.LOGGER.debug("Applied curios data for player {} from pre-read data", player.getUUID());
    }

    /**
     * Feeds every ItemStack currently equipped in a Curios slot to the consumer.
     *
     * <p>Sophisticated Storage shulkers and RS2 disks are routinely carried in Curios slots
     * (belt / back / charm). Before 2.1.6 the collectors only walked the vanilla inventory
     * and the ender chest, so those containers were neither saved nor restored and their
     * contents silently diverged between servers.
     */
    private static void forEachCuriosStack(Player player, Consumer<ItemStack> consumer) {
        if (!ModList.get().isLoaded("curios")) return;
        try {
            Optional<ICuriosItemHandler> handlerOpt = CuriosApi.getCuriosInventory(player);
            if (handlerOpt.isEmpty()) return;
            handlerOpt.get().getCurios().forEach((slotType, stacksHandler) -> {
                IDynamicStackHandler stacks = stacksHandler.getStacks();
                for (int i = 0; i < stacks.getSlots(); i++) {
                    ItemStack stack = stacks.getStackInSlot(i);
                    if (!stack.isEmpty()) consumer.accept(stack);
                }
                IDynamicStackHandler cos = stacksHandler.getCosmeticStacks();
                for (int i = 0; i < cos.getSlots(); i++) {
                    ItemStack stack = cos.getStackInSlot(i);
                    if (!stack.isEmpty()) consumer.accept(stack);
                }
            });
        } catch (Exception e) {
            PlayerSync.LOGGER.debug("[curios-scan] unavailable: {}", e.toString());
        }
    }

    /**
     * Feeds every ItemStack the player carries (inventory incl. armor + offhand, ender chest
     * and Curios slots) to the consumer. Single definition so the save and restore sides can
     * never drift apart again.
     */
    private static void forEachCarriedStack(Player player, Consumer<ItemStack> consumer) {
        net.minecraft.world.entity.player.Inventory inv = player.getInventory();
        for (int i = 0; i < inv.getContainerSize(); i++) {
            ItemStack stack = inv.getItem(i);
            if (!stack.isEmpty()) consumer.accept(stack);
        }
        for (int i = 0; i < player.getEnderChestInventory().getContainerSize(); i++) {
            ItemStack stack = player.getEnderChestInventory().getItem(i);
            if (!stack.isEmpty()) consumer.accept(stack);
        }
        forEachCuriosStack(player, consumer);
    }

    // =========================================================================
    // Sophisticated Storage (barrels, shulkers, chests carried as items)
    // =========================================================================

    /** Restores packed Sophisticated Storage items' contents from the database. */
    public static void restoreSophisticatedStorageItems(Player player) {
        if (!ModList.get().isLoaded("sophisticatedstorage")) return;
        // Sophisticated Storage shares the sync_backpacks toggle (same storage table).
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_BACKPACKS.get()) return;
        PlayerSync.LOGGER.debug("Restoring Sophisticated Storage items for player {}", player.getUUID());
        final String playerUuid = player.getUUID().toString();
        forEachCarriedStack(player, stack -> restoreSingleSophisticatedStorageItem(playerUuid, stack));
    }

    private static void restoreSingleSophisticatedStorageItem(String playerUuid, ItemStack stack) {
        if (!isSophisticatedStorageItem(stack)) return;

        try {
            Optional<UUID> uuidOpt = ssContentsUuid(stack);
            if (uuidOpt.isEmpty()) return;
            final UUID finalUuid = uuidOpt.get();
            final var store = net.p3pp3rf1y.sophisticatedstorage.block.ItemContentsStorage.get();

            restoreStorageContents(finalUuid,
                    (nbt) -> {
                        try {
                            // ItemContentsStorage merges on setStorageContents when the UUID is
                            // already present — same upstream shape as BackpackStorage — so clear first.
                            clearSSStorageContents(store, finalUuid);
                            if (nbt.isEmpty()) {
                                StorageOwnership.mark(playerUuid, finalUuid);
                                PlayerSync.LOGGER.debug("[restore-ss] uuid={} empty tombstone — local entry cleared", finalUuid);
                                return;
                            }
                            CompoundTag fresh = nbt.copy();
                            store.setStorageContents(finalUuid, fresh);
                            StorageOwnership.mark(playerUuid, finalUuid);
                            PlayerSync.LOGGER.debug("[restore-ss] uuid={} nbt_keys={}", finalUuid, fresh.getAllKeys().size());
                        } catch (Exception e) {
                            PlayerSync.LOGGER.error("Error restoring Sophisticated Storage data for UUID {}", finalUuid, e);
                        }
                    },
                    // No row: never synced. Keep the local contents and stay un-owned so this
                    // server can never tombstone a container it has not actually loaded.
                    () -> PlayerSync.LOGGER.debug("[restore-ss] uuid={} absent from DB — keeping local contents", finalUuid));
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error restoring Sophisticated Storage item", e);
        }
    }

    private static Optional<UUID> ssContentsUuid(ItemStack stack) {
        try {
            // The UUID is a proper DataComponent managed by ModCoreDataComponents in 1.21.1,
            // not an NBT tag in CustomData — go through the wrapper API.
            net.p3pp3rf1y.sophisticatedstorage.item.StackStorageWrapper wrapper =
                    net.p3pp3rf1y.sophisticatedstorage.item.StackStorageWrapper.fromStack(
                            net.neoforged.neoforge.server.ServerLifecycleHooks.getCurrentServer().registryAccess(), stack);
            return wrapper.getContentsUuid();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Clears a Sophisticated Storage entry from the ItemContentsStorage SavedData. Tries the
     * public {@code removeStorageContents} first, then reflection.
     *
     * <p>The reflection result is cached the first time a given concrete class is seen —
     * without the cache every restored item re-walked {@code getDeclaredFields()}, which
     * showed up on the main thread during mass joins.
     */
    private static volatile Class<?> ssCachedClass;
    private static volatile java.lang.reflect.Method ssRemoveMethod;
    private static volatile java.lang.reflect.Field[] ssMapFields;

    @SuppressWarnings("unchecked")
    private static void clearSSStorageContents(
            net.p3pp3rf1y.sophisticatedstorage.block.ItemContentsStorage store, UUID uuid) {
        try {
            Class<?> klass = store.getClass();
            if (ssCachedClass != klass) {
                synchronized (ModsSupport.class) {
                    if (ssCachedClass != klass) {
                        java.lang.reflect.Method m = null;
                        try {
                            m = klass.getMethod("removeStorageContents", UUID.class);
                        } catch (NoSuchMethodException ignored) {}
                        java.util.List<java.lang.reflect.Field> maps = new java.util.ArrayList<>();
                        for (java.lang.reflect.Field f : klass.getDeclaredFields()) {
                            if (java.util.Map.class.isAssignableFrom(f.getType())) {
                                f.setAccessible(true);
                                maps.add(f);
                            }
                        }
                        ssRemoveMethod = m;
                        ssMapFields = maps.toArray(new java.lang.reflect.Field[0]);
                        ssCachedClass = klass;
                    }
                }
            }
            if (ssRemoveMethod != null) {
                ssRemoveMethod.invoke(store, uuid);
                return;
            }
            for (java.lang.reflect.Field f : ssMapFields) {
                Object map = f.get(store);
                if (map instanceof java.util.Map<?, ?> m) {
                    ((java.util.Map<Object, Object>) m).remove(uuid);
                    ((java.util.Map<Object, Object>) m).remove(uuid.toString());
                }
            }
            store.setDirty();
        } catch (Throwable t) {
            PlayerSync.LOGGER.warn("[clear-ss] unable to clear SS storage for {}: {}", uuid, t.getMessage());
        }
    }

    /** Checks if an item comes from the Sophisticated Storage mod. */
    private static boolean isSophisticatedStorageItem(ItemStack stack) {
        try {
            if (stack == null || stack.isEmpty()) return false;
            net.minecraft.resources.ResourceLocation loc =
                    net.minecraft.core.registries.BuiltInRegistries.ITEM.getKey(stack.getItem());
            return loc != null && loc.getNamespace().equals("sophisticatedstorage");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Collects Sophisticated Storage item UUIDs from everything the player carries.
     * MUST run on the MAIN THREAD (reads inventory items).
     */
    public static List<UUID> collectSSUuids(Player player) {
        List<UUID> uuids = new ArrayList<>();
        if (!ModList.get().isLoaded("sophisticatedstorage")) return uuids;
        try {
            forEachCarriedStack(player, stack -> {
                if (!isSophisticatedStorageItem(stack)) return;
                ssContentsUuid(stack).ifPresent(uuids::add);
            });
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error collecting SS UUIDs for player {}", player.getUUID(), e);
        }
        return uuids;
    }

    /**
     * Captures Sophisticated Storage CompoundTags on the MAIN thread by copying the SavedData
     * entries. Reading {@code ItemContentsStorage} from a background thread would race with
     * main-thread modifications of a non-thread-safe HashMap and risk torn reads.
     */
    public static Map<UUID, CompoundTag> snapshotSSData(Player player) {
        Map<UUID, CompoundTag> out = new HashMap<>();
        if (!ModList.get().isLoaded("sophisticatedstorage")) return out;
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_BACKPACKS.get()) return out;
        List<UUID> uuids = collectSSUuids(player);
        if (uuids.isEmpty()) return out;
        final String playerUuid = player.getUUID().toString();
        try {
            net.p3pp3rf1y.sophisticatedstorage.block.ItemContentsStorage store =
                    net.p3pp3rf1y.sophisticatedstorage.block.ItemContentsStorage.get();
            for (UUID uuid : uuids) {
                try {
                    // ItemContentsStorage exposes a public non-creating probe, so unlike the
                    // backpack path no reflection is needed to tell absent from empty.
                    boolean present;
                    try {
                        present = store.has(uuid);
                    } catch (Throwable t) {
                        present = true; // probe unavailable: fall back to the creating read
                    }
                    if (present) {
                        CompoundTag live = store.getOrCreateStorageContents(uuid);
                        if (live != null && !live.isEmpty()) {
                            out.put(uuid, live.copy());
                            continue;
                        }
                    }
                    if (StorageOwnership.isOwned(playerUuid, uuid)) {
                        out.put(uuid, new CompoundTag()); // authoritative empty
                    }
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error snapshotting SS contents for UUID {}", uuid, e);
                }
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error reading ItemContentsStorage for snapshot", e);
        }
        return out;
    }

    // =========================================================================
    // Refined Storage 2 disks
    // =========================================================================

    /**
     * Cached handle on {@code StorageRepositoryImpl.createCodec(Runnable)}. Only the METHOD
     * is cached, never the resulting codec.
     *
     * <p>That Runnable is the change listener baked into every storage the codec decodes
     * (verified in bytecode: {@code createCodec} forwards it to
     * {@code StorageType#getMapCodec(Runnable)}, and the repository's own constructor passes
     * {@code this::markAsChanged}). PlayerSync used to pass a no-op {@code () -> {}} and
     * cache the codec globally, so every disk it restored came back with a dead listener:
     * RS2 never marked its SavedData dirty when the player subsequently changed that disk,
     * the world save skipped the file, and everything written since the transfer was gone
     * after the next restart. Binding the listener to the live repository fixes it.
     */
    private static volatile java.lang.reflect.Method rs2CreateCodecMethod;
    private static volatile boolean rs2CodecUnavailable = false;

    @SuppressWarnings("rawtypes")
    private static com.mojang.serialization.Codec getRS2MapCodec(Object repo) {
        if (rs2CodecUnavailable) return null;
        try {
            java.lang.reflect.Method m = rs2CreateCodecMethod;
            if (m == null) {
                synchronized (ModsSupport.class) {
                    m = rs2CreateCodecMethod;
                    if (m == null) {
                        m = repo.getClass().getDeclaredMethod("createCodec", Runnable.class);
                        m.setAccessible(true);
                        rs2CreateCodecMethod = m;
                    }
                }
            }
            final Object repoRef = repo;
            Runnable listener = () -> {
                try {
                    ((com.refinedmods.refinedstorage.common.api.storage.StorageRepository) repoRef).markAsChanged();
                } catch (Throwable ignored) {}
            };
            return (com.mojang.serialization.Codec) m.invoke(null, listener);
        } catch (Throwable t) {
            rs2CodecUnavailable = true;
            PlayerSync.LOGGER.error("[rs2] cannot resolve the storage map codec — disk sync disabled for this session", t);
            return null;
        }
    }

    /**
     * Encodes the RS2 disks the player carries, on the MAIN THREAD.
     *
     * <p>Encoding used to happen on the background writer thread, reading the live
     * {@code StorageRepository} while the main thread could be mutating it. A failed encode
     * was logged and skipped, which meant the disk simply never reached the database — the
     * player then arrived on the next server with an empty disk. Snapshotting here matches
     * what the backpack and Sophisticated Storage paths already do.
     *
     * <p>Only the disks in the player's own containers are touched: cost is O(player disks),
     * not O(world disks) as the old full {@code SavedData.save()} scan was.
     *
     * @return map {disk uuid -> encoded tag}; an EMPTY tag is the tombstone for a disk this
     *         session established and that no longer has a repository entry
     */
    public static Map<UUID, CompoundTag> snapshotRS2Disks(Player player) {
        Map<UUID, CompoundTag> out = new HashMap<>();
        if (!ModList.get().isLoaded("refinedstorage")) return out;
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_REFINED_STORAGE.get()) return out;
        if (!(player instanceof net.minecraft.server.level.ServerPlayer sp)) return out;
        List<UUID> diskUuids = collectRS2DiskUuids(player);
        if (diskUuids.isEmpty()) return out;

        final String playerUuid = sp.getUUID().toString();
        try {
            com.refinedmods.refinedstorage.common.api.storage.StorageRepository repo =
                    com.refinedmods.refinedstorage.common.api.RefinedStorageApi.INSTANCE.getStorageRepository(sp.serverLevel());
            if (repo == null) return out;

            @SuppressWarnings("rawtypes")
            com.mojang.serialization.Codec mapCodec = getRS2MapCodec(repo);
            if (mapCodec == null) return out;

            var ops = sp.getServer().registryAccess().createSerializationContext(net.minecraft.nbt.NbtOps.INSTANCE);
            for (UUID uuid : diskUuids) {
                try {
                    Optional<com.refinedmods.refinedstorage.common.api.storage.SerializableStorage> diskOpt = repo.get(uuid);
                    if (diskOpt.isEmpty()) {
                        // The item exists but the repository has no entry. Only a disk this
                        // session established may be tombstoned; otherwise the database copy
                        // is authoritative and must survive untouched.
                        if (StorageOwnership.isOwned(playerUuid, uuid)) {
                            out.put(uuid, new CompoundTag());
                        }
                        continue;
                    }
                    // Encode a single-entry map with the same codec RS2 uses for its full save.
                    // The output is {"uuid-string": {type, capacity, resources}}; we store ONLY
                    // the inner object, which is what the restore path expects.
                    Map<UUID, com.refinedmods.refinedstorage.common.api.storage.SerializableStorage> singleMap =
                            java.util.Collections.singletonMap(uuid, diskOpt.get());
                    @SuppressWarnings("unchecked")
                    com.mojang.serialization.DataResult<net.minecraft.nbt.Tag> enc = mapCodec.encodeStart(ops, singleMap);
                    Optional<net.minecraft.nbt.Tag> tagOpt = enc.result();
                    if (tagOpt.isEmpty()) {
                        PlayerSync.LOGGER.error("[rs2-save] codec encode failed for disk {} ({}) — the disk was NOT persisted",
                                uuid, enc.error().map(Object::toString).orElse("no error detail"));
                        continue;
                    }
                    if (!(tagOpt.get() instanceof CompoundTag wrapped)) continue;
                    CompoundTag inner = wrapped.getCompound(uuid.toString());
                    if (inner != null && !inner.isEmpty()) {
                        out.put(uuid, inner);
                    }
                } catch (Throwable t) {
                    PlayerSync.LOGGER.error("[rs2-save] encode threw for disk {} — the disk was NOT persisted", uuid, t);
                }
            }
            if (!out.isEmpty()) {
                PlayerSync.LOGGER.debug("Snapshotted {} RS2 disk(s) for {}", out.size(), playerUuid);
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting RS2 disks for player {}", player.getUUID(), e);
        }
        return out;
    }

    /**
     * Restores RS2 disk storage: decodes the stored entry with the same map codec used at
     * save time and injects it into the repository.
     */
    public static void restoreRefinedStorageDisks(Player player) {
        if (!ModList.get().isLoaded("refinedstorage")) return;
        if (!vip.fubuki.playersync.config.JdbcConfig.SYNC_REFINED_STORAGE.get()) return;
        if (!(player instanceof net.minecraft.server.level.ServerPlayer sp)) return;

        List<UUID> diskUuids = collectRS2DiskUuids(player);
        if (diskUuids.isEmpty()) return;

        final String playerUuid = sp.getUUID().toString();
        try {
            com.refinedmods.refinedstorage.common.api.storage.StorageRepository repo =
                    com.refinedmods.refinedstorage.common.api.RefinedStorageApi.INSTANCE.getStorageRepository(sp.serverLevel());
            if (repo == null) return;

            @SuppressWarnings("rawtypes")
            final com.mojang.serialization.Codec mapCodec = getRS2MapCodec(repo);
            if (mapCodec == null) {
                PlayerSync.LOGGER.error("Cannot get RS2 map codec — disk restore skipped for {}", playerUuid);
                return;
            }

            var ops = sp.getServer().registryAccess().createSerializationContext(net.minecraft.nbt.NbtOps.INSTANCE);

            for (UUID uuid : diskUuids) {
                restoreStorageContents(uuid,
                        (storedNbt) -> applyRS2Disk(repo, mapCodec, ops, uuid, storedNbt, playerUuid),
                        // No row: never synced. Keep whatever this server's repository holds and
                        // stay un-owned, so the save side cannot tombstone a disk it never loaded.
                        () -> PlayerSync.LOGGER.debug("[restore-rs2] uuid={} absent from DB — keeping local entry", uuid));
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error restoring RS2 disk data for player {}", player.getUUID(), e);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void applyRS2Disk(com.refinedmods.refinedstorage.common.api.storage.StorageRepository repo,
                                     com.mojang.serialization.Codec mapCodec,
                                     com.mojang.serialization.DynamicOps<net.minecraft.nbt.Tag> ops,
                                     UUID uuid, CompoundTag storedNbt, String playerUuid) {
        // Keep the current entry so a failed injection can be rolled back instead of
        // leaving the player with a disk that has no repository entry at all.
        Optional<com.refinedmods.refinedstorage.common.api.storage.SerializableStorage> previous;
        try {
            previous = repo.get(uuid);
        } catch (Throwable t) {
            previous = Optional.empty();
        }

        if (storedNbt.isEmpty()) {
            // Explicit tombstone: the disk was empty when it was last saved.
            try { repo.remove(uuid); } catch (Exception ignored) {}
            StorageOwnership.mark(playerUuid, uuid);
            PlayerSync.LOGGER.debug("[restore-rs2] uuid={} empty tombstone — repository entry cleared", uuid);
            return;
        }

        try {
            // storedNbt is the INNER object ({type, capacity, resources}); the map codec
            // expects {uuid-string: {...}}, so wrap it back before decoding.
            CompoundTag wrapped = new CompoundTag();
            wrapped.put(uuid.toString(), storedNbt);

            com.mojang.serialization.DataResult<?> dataResult = mapCodec.decode(ops, wrapped);
            Optional<?> opt = dataResult.result();
            if (opt.isEmpty()) {
                PlayerSync.LOGGER.error("[restore-rs2] decode failed for disk {} ({}) — keeping the existing entry",
                        uuid, dataResult.error().map(Object::toString).orElse("no error detail"));
                return;
            }
            com.mojang.datafixers.util.Pair<?, ?> pair = (com.mojang.datafixers.util.Pair<?, ?>) opt.get();
            java.util.Map<UUID, ?> decoded = (java.util.Map<UUID, ?>) pair.getFirst();
            for (java.util.Map.Entry<UUID, ?> entry : decoded.entrySet()) {
                // repo.set() throws IllegalArgumentException when the UUID is already present,
                // so remove first. Both calls mark the SavedData dirty upstream.
                try { repo.remove(entry.getKey()); } catch (Exception ignored) {}
                try {
                    repo.set(entry.getKey(),
                            (com.refinedmods.refinedstorage.common.api.storage.SerializableStorage) entry.getValue());
                } catch (Exception setEx) {
                    PlayerSync.LOGGER.warn("[restore-rs2] repo.set failed for {} — rolling back to the previous entry", entry.getKey(), setEx);
                    if (previous.isPresent()) {
                        try { repo.set(entry.getKey(), previous.get()); } catch (Exception ignored) {}
                    }
                    continue;
                }
                StorageOwnership.mark(playerUuid, entry.getKey());
                PlayerSync.LOGGER.debug("[restore-rs2] uuid={} restored", entry.getKey());
            }
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error restoring RS2 disk data for UUID {}", uuid, e);
        }
    }

    /** Collects RS2 / ExtraDisks storage-reference UUIDs from everything the player carries. */
    public static List<UUID> collectRS2DiskUuids(Player player) {
        List<UUID> uuids = new ArrayList<>();
        forEachCarriedStack(player, stack -> {
            UUID ref = getRS2StorageReference(stack);
            if (ref != null) uuids.add(ref);
        });
        return uuids;
    }

    /**
     * Extracts the storageReference UUID from an RS2 disk item.
     * Returns null if the item is not an RS2 disk or has no storage reference.
     */
    private static UUID getRS2StorageReference(ItemStack stack) {
        try {
            if (stack == null || stack.isEmpty()) return null;
            net.minecraft.resources.ResourceLocation loc =
                    net.minecraft.core.registries.BuiltInRegistries.ITEM.getKey(stack.getItem());
            if (loc == null) return null;
            if (!loc.getNamespace().equals("refinedstorage") && !loc.getNamespace().equals("extradisks")) {
                return null;
            }
            net.minecraft.core.component.DataComponentType<UUID> storageRefType =
                    com.refinedmods.refinedstorage.common.content.DataComponents.INSTANCE.getStorageReference();
            return stack.get(storageRefType);
        } catch (Exception e) {
            return null;
        }
    }
}
