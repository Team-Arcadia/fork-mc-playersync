package vip.fubuki.playersync.sync.addons;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.neoforged.fml.ModList;
import vip.fubuki.playersync.PlayerSync;
import vip.fubuki.playersync.config.JdbcConfig;
import vip.fubuki.playersync.sync.VanillaSync;
import vip.fubuki.playersync.util.LocalJsonUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Player-data compatibility handlers for mods that keep their state outside the vanilla
 * inventory:
 * <ul>
 *   <li><b>Accessories API</b> — extra equipment slots (The Aether and others)</li>
 *   <li><b>Cosmetic Armor Reworked</b> — 4 cosmetic equipment slots</li>
 *   <li><b>NeoForge attachments</b> — the modern per-player storage used by Ars Nouveau,
 *       Iron's Spellbooks, Pehkui, Spice of Life and many more</li>
 *   <li><b>Player persistent data</b> — the legacy {@code NeoForgeData / PlayerPersisted}
 *       scratchpad, still used by Corail Tombstone (knowledge, alignment, perks, watcher
 *       knowledge) and a long tail of other mods</li>
 * </ul>
 *
 * <p>Item DataComponents (Apotheosis affixes, AE2 storage-cell contents, …) need no handler
 * here: they live on the ItemStack and travel with the inventory serialization.
 */
public class ModCompatSync {

    // Cache the reflective AttachmentHolder lookups once. Resolving them per snapshot meant
    // thousands of reflective lookups an hour on a populated server.
    private static final java.lang.reflect.Method SERIALIZE_ATTACHMENTS;
    private static final java.lang.reflect.Method DESERIALIZE_ATTACHMENTS;
    static {
        java.lang.reflect.Method ser = null, des = null;
        try {
            ser = net.neoforged.neoforge.attachment.AttachmentHolder.class
                    .getDeclaredMethod("serializeAttachments", net.minecraft.core.HolderLookup.Provider.class);
            ser.setAccessible(true);
            des = net.neoforged.neoforge.attachment.AttachmentHolder.class
                    .getDeclaredMethod("deserializeAttachments",
                            net.minecraft.core.HolderLookup.Provider.class,
                            net.minecraft.nbt.CompoundTag.class);
            des.setAccessible(true);
        } catch (NoSuchMethodException e) {
            PlayerSync.LOGGER.error("[PlayerSync] Could not cache AttachmentHolder reflection methods; NeoForge attachment sync will be disabled.", e);
        }
        SERIALIZE_ATTACHMENTS = ser;
        DESERIALIZE_ATTACHMENTS = des;
    }

    // ============================
    // Accessories API (Aether slots)
    // ============================

    /** Applies pre-read Accessories data to the player entity (NO DB access). */
    public static void applyAccessoriesFromData(Player player, String accessoriesData) {
        if (!ModList.get().isLoaded("accessories")) return;
        if (!JdbcConfig.SYNC_ACCESSORIES.get()) return;
        try {
            io.wispforest.accessories.api.AccessoriesCapability cap =
                    io.wispforest.accessories.api.AccessoriesCapability.get(player);
            if (cap == null) return;

            Map<String, io.wispforest.accessories.api.AccessoriesContainer> containers = cap.getContainers();

            // ALWAYS clear the slots first to wipe stale data from the .dat file, then only
            // restore if the DB has valid data. Without the clear, a player whose DB record
            // is empty keeps whatever the local .dat carried — duplication across servers.
            for (io.wispforest.accessories.api.AccessoriesContainer container : containers.values()) {
                var accessories = container.getAccessories();
                for (int i = 0; i < accessories.getContainerSize(); i++) {
                    accessories.setItem(i, ItemStack.EMPTY);
                }
            }

            if (accessoriesData == null || accessoriesData.length() <= 2) return;

            Map<String, String> storedMap = LocalJsonUtil.StringToMap(accessoriesData);
            if (storedMap.isEmpty()) return;

            for (Map.Entry<String, String> entry : storedMap.entrySet()) {
                String compositeKey = entry.getKey();
                int lastColon = compositeKey.lastIndexOf(':');
                if (lastColon < 0) continue;
                String slotType = compositeKey.substring(0, lastColon);
                int slotIndex;
                try { slotIndex = Integer.parseInt(compositeKey.substring(lastColon + 1)); }
                catch (NumberFormatException ex) { continue; }

                try {
                    ItemStack stack = VanillaSync.deserializeAndCreatePlaceholderIfNeeded(entry.getValue());
                    var container = containers.get(slotType);
                    if (container != null) {
                        var acc = container.getAccessories();
                        if (slotIndex < acc.getContainerSize()) {
                            acc.setItem(slotIndex, stack);
                        }
                    }
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error applying Accessories data for key {}", compositeKey, e);
                }
            }
            PlayerSync.LOGGER.debug("Applied Accessories data for player {}", player.getUUID());
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error applying Accessories data for player {}", player.getUUID(), e);
        }
    }

    /**
     * Captures Accessories slot data on the main thread.
     *
     * @return the serialized slots, or {@code null} when the capability is unavailable so
     *         the writer preserves the existing record instead of wiping it
     */
    public static String snapshotAccessories(Player player) {
        if (!ModList.get().isLoaded("accessories")) return null;
        try {
            io.wispforest.accessories.api.AccessoriesCapability cap =
                    io.wispforest.accessories.api.AccessoriesCapability.get(player);
            if (cap == null) {
                vip.fubuki.playersync.util.SyncLogger.modCompatSkip(
                        player.getUUID().toString(), "accessories",
                        "capability unavailable — skipping write to preserve DB");
                return null;
            }
            Map<String, String> flatMap = new HashMap<>();
            for (Map.Entry<String, io.wispforest.accessories.api.AccessoriesContainer> entry : cap.getContainers().entrySet()) {
                String slotType = entry.getKey();
                var accessories = entry.getValue().getAccessories();
                for (int i = 0; i < accessories.getContainerSize(); i++) {
                    ItemStack stack = accessories.getItem(i);
                    if (!stack.isEmpty()) {
                        flatMap.put(slotType + ":" + i, VanillaSync.getNbtForStorage(stack));
                    }
                }
            }
            // Capability read OK — "{}" is intentional for truly empty slots so the apply
            // side clears stale .dat contents.
            return flatMap.toString();
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting Accessories for player {}", player.getUUID(), e);
            return null;
        }
    }

    // ============================
    // Cosmetic Armor Reworked
    // ============================

    /** Applies pre-read CosmeticArmor data to the player entity (NO DB access). */
    public static void applyCosmeticArmorFromData(Player player, String cosmeticArmorData) {
        if (!ModList.get().isLoaded("cosmeticarmorreworked")) return;
        if (!JdbcConfig.SYNC_COSMETIC_ARMOR.get()) return;
        try {
            lain.mods.cos.impl.inventory.InventoryCosArmor cosInv =
                    lain.mods.cos.impl.ModObjects.invMan.getCosArmorInventory(player.getUUID());
            if (cosInv == null) return;

            for (int i = 0; i < cosInv.getContainerSize(); i++) {
                cosInv.setItem(i, ItemStack.EMPTY);
            }

            if (cosmeticArmorData == null || cosmeticArmorData.length() <= 2) {
                cosInv.setChanged();
                return;
            }

            Map<Integer, String> storedMap = LocalJsonUtil.StringToEntryMap(cosmeticArmorData);
            if (storedMap.isEmpty()) {
                cosInv.setChanged();
                return;
            }

            for (Map.Entry<Integer, String> entry : storedMap.entrySet()) {
                int slot = entry.getKey();
                try {
                    ItemStack stack = VanillaSync.deserializeAndCreatePlaceholderIfNeeded(entry.getValue());
                    if (slot < cosInv.getContainerSize()) {
                        cosInv.setItem(slot, stack);
                    }
                } catch (Exception e) {
                    PlayerSync.LOGGER.error("Error applying CosmeticArmor slot {}", slot, e);
                }
            }
            cosInv.setChanged();
            PlayerSync.LOGGER.debug("Applied CosmeticArmor data for player {}", player.getUUID());
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error applying CosmeticArmor data for player {}", player.getUUID(), e);
        }
    }

    /**
     * Captures Cosmetic Armor slot data on the main thread.
     *
     * @return the serialized slots, or {@code null} when the inventory manager is
     *         unavailable so the writer preserves the existing record
     */
    public static String snapshotCosmeticArmor(Player player) {
        if (!ModList.get().isLoaded("cosmeticarmorreworked")) return null;
        try {
            lain.mods.cos.impl.inventory.InventoryCosArmor cosInv =
                    lain.mods.cos.impl.ModObjects.invMan.getCosArmorInventory(player.getUUID());
            if (cosInv == null) return null;
            Map<Integer, String> flatMap = new HashMap<>();
            for (int i = 0; i < cosInv.getContainerSize(); i++) {
                ItemStack stack = cosInv.getItem(i);
                if (!stack.isEmpty()) {
                    flatMap.put(i, VanillaSync.getNbtForStorage(stack));
                }
            }
            return flatMap.toString();
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting CosmeticArmor for player {}", player.getUUID(), e);
            return null;
        }
    }

    // ============================
    // NeoForge attachments
    // ============================

    /**
     * Captures NeoForge attachment data on the main thread.
     * Returns a BNBT-serialized string, or null when there is nothing to store.
     */
    public static String snapshotAttachments(Player player) {
        if (SERIALIZE_ATTACHMENTS == null) return null;
        try {
            if (!(player instanceof net.minecraft.server.level.ServerPlayer serverPlayer)) return null;
            CompoundTag attachments = (CompoundTag)
                    SERIALIZE_ATTACHMENTS.invoke(player, serverPlayer.getServer().registryAccess());
            if (attachments == null || attachments.isEmpty()) return null;
            return VanillaSync.serializeTagToBinaryBase64(attachments);
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting NeoForge attachments for player {}", player.getUUID(), e);
            return null;
        }
    }

    /**
     * Applies pre-read NeoForge attachment data to the player entity (NO DB access).
     * Goes through NeoForge's own deserialization so the result is identical to a normal
     * player load.
     */
    public static void applyAttachmentsFromData(Player player, String serialized) {
        if (serialized == null || !serialized.startsWith("BNBT:")) return;
        if (DESERIALIZE_ATTACHMENTS == null) return;
        try {
            if (!(player instanceof net.minecraft.server.level.ServerPlayer serverPlayer)) return;

            CompoundTag attachments = VanillaSync.deserializeBinaryBase64Tag(serialized);
            if (attachments.isEmpty()) return;

            CompoundTag wrapper = new CompoundTag();
            wrapper.put("neoforge:attachments", attachments);

            DESERIALIZE_ATTACHMENTS.invoke(player, serverPlayer.getServer().registryAccess(), wrapper);

            PlayerSync.LOGGER.debug("Applied NeoForge attachments for player {} ({} keys)",
                    player.getUUID(), attachments.getAllKeys().size());
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error applying NeoForge attachments for player {}", player.getUUID(), e);
        }
    }

    // ============================
    // Player persistent data (legacy NeoForgeData scratchpad)
    // ============================

    /**
     * Mirrors {@code Player#getPersistentData()} across servers.
     *
     * <p>NeoForge attachments are the modern mechanism, but a large number of mods still
     * write straight into the persistent-data tag. <b>Corail Tombstone</b> is the notable
     * one for this mod's users: {@code TBPlayerCapabilityHandler} stores knowledge,
     * alignment, perks and watcher knowledge under
     * {@code getPersistentData() -> "PlayerPersisted" -> "tb_player_tag"} and writes through
     * on every setter (verified in the 1.21.1 NeoForge build). Without this handler a player
     * switching servers arrived with their whole Tombstone progression reset.
     *
     * @return BNBT-serialized tag, or {@code null} when there is nothing to store or the
     *         feature is disabled
     */
    public static String snapshotPersistentData(Player player) {
        if (!JdbcConfig.SYNC_PERSISTENT_DATA.get()) return null;
        try {
            CompoundTag persistent = player.getPersistentData();
            if (persistent == null || persistent.isEmpty()) return null;
            CompoundTag filtered = filterPersistentData(persistent);
            if (filtered.isEmpty()) return null;
            return VanillaSync.serializeTagToBinaryBase64(filtered);
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error snapshotting persistent data for player {}", player.getUUID(), e);
            return null;
        }
    }

    /** Applies pre-read persistent data to the player entity (NO DB access). */
    public static void applyPersistentDataFromData(Player player, String serialized) {
        if (!JdbcConfig.SYNC_PERSISTENT_DATA.get()) return;
        if (serialized == null || !serialized.startsWith("BNBT:")) return;
        try {
            CompoundTag stored = VanillaSync.deserializeBinaryBase64Tag(serialized);
            if (stored.isEmpty()) return;
            CompoundTag target = player.getPersistentData();

            // Replace only the keys we actually manage. Blacklisted keys keep whatever the
            // destination server had, which is what an admin excluding a world-bound key
            // (a saved home, a per-world cooldown) expects.
            CompoundTag incoming = filterPersistentData(stored);

            // Nested entries ("PlayerPersisted.WaystonesData") live inside a key we do
            // replace, so remember the destination's own values before the swap and put
            // them back afterwards.
            java.util.List<String[]> nestedPaths = nestedBlacklistPaths();
            java.util.Map<String[], net.minecraft.nbt.Tag> localNested = new java.util.HashMap<>();
            for (String[] path : nestedPaths) {
                net.minecraft.nbt.Tag local = getAtPath(target, path);
                localNested.put(path, local == null ? null : local.copy());
            }

            for (String key : new java.util.ArrayList<>(target.getAllKeys())) {
                if (isPersistentKeyBlacklisted(key)) continue;
                target.remove(key);
            }
            for (String key : incoming.getAllKeys()) {
                target.put(key, incoming.get(key).copy());
            }

            for (String[] path : nestedPaths) {
                removeAtPath(target, path);
                net.minecraft.nbt.Tag local = localNested.get(path);
                if (local != null) putAtPath(target, path, local);
            }
            PlayerSync.LOGGER.debug("Applied persistent data for player {} ({} keys)",
                    player.getUUID(), incoming.getAllKeys().size());
        } catch (Exception e) {
            PlayerSync.LOGGER.error("Error applying persistent data for player {}", player.getUUID(), e);
        }
    }

    private static CompoundTag filterPersistentData(CompoundTag source) {
        CompoundTag out = new CompoundTag();
        for (String key : source.getAllKeys()) {
            if (isPersistentKeyBlacklisted(key)) continue;
            net.minecraft.nbt.Tag value = source.get(key);
            if (value != null) out.put(key, value.copy());
        }
        for (String[] path : nestedBlacklistPaths()) {
            removeAtPath(out, path);
        }
        return out;
    }

    private static boolean isPersistentKeyBlacklisted(String key) {
        try {
            java.util.List<?> blacklist = JdbcConfig.PERSISTENT_DATA_BLACKLIST.get();
            if (blacklist == null || blacklist.isEmpty()) return false;
            for (Object entry : blacklist) {
                if (entry != null && entry.toString().equals(key)) return true;
            }
            return false;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * Blacklist entries containing a dot name a key nested inside compounds, e.g.
     * {@code "PlayerPersisted.WaystonesData"}. Balm mods (Waystones, NetherPortalFix)
     * all share the top-level {@code PlayerPersisted} compound with data that must keep
     * syncing (Corail Tombstone), so only their own sub-key can be excluded.
     */
    private static java.util.List<String[]> nestedBlacklistPaths() {
        java.util.List<String[]> paths = new java.util.ArrayList<>();
        try {
            java.util.List<?> blacklist = JdbcConfig.PERSISTENT_DATA_BLACKLIST.get();
            if (blacklist == null) return paths;
            for (Object entry : blacklist) {
                if (entry == null) continue;
                String s = entry.toString();
                if (s.indexOf('.') <= 0 || s.endsWith(".")) continue;
                paths.add(s.split("\\."));
            }
        } catch (Throwable ignored) {
        }
        return paths;
    }

    private static CompoundTag getParent(CompoundTag root, String[] path, boolean create) {
        CompoundTag current = root;
        for (int i = 0; i < path.length - 1; i++) {
            if (current.contains(path[i], net.minecraft.nbt.Tag.TAG_COMPOUND)) {
                current = current.getCompound(path[i]);
            } else if (create && !current.contains(path[i])) {
                CompoundTag child = new CompoundTag();
                current.put(path[i], child);
                current = child;
            } else {
                return null;
            }
        }
        return current;
    }

    private static net.minecraft.nbt.Tag getAtPath(CompoundTag root, String[] path) {
        CompoundTag parent = getParent(root, path, false);
        return parent == null ? null : parent.get(path[path.length - 1]);
    }

    private static void removeAtPath(CompoundTag root, String[] path) {
        CompoundTag parent = getParent(root, path, false);
        if (parent != null) parent.remove(path[path.length - 1]);
    }

    private static void putAtPath(CompoundTag root, String[] path, net.minecraft.nbt.Tag value) {
        CompoundTag parent = getParent(root, path, true);
        if (parent != null) parent.put(path[path.length - 1], value);
    }
}
