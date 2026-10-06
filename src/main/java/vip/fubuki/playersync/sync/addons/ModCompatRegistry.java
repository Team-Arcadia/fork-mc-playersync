package vip.fubuki.playersync.sync.addons;

import net.neoforged.fml.ModList;
import vip.fubuki.playersync.PlayerSync;
import vip.fubuki.playersync.config.JdbcConfig;
import vip.fubuki.playersync.util.SyncLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Single source of truth for "which mods does PlayerSync understand, and how".
 *
 * <p>Every integration is declared once here with the mechanism it relies on and the toggle
 * that governs it, so the startup report, the {@code /playersync compat} command and the
 * documentation can never drift apart from the code. Adding an integration without adding
 * its row here is the mistake this class exists to make obvious.
 */
public final class ModCompatRegistry {

    private ModCompatRegistry() {}

    /** How a mod's player data reaches the database. */
    public enum Mechanism {
        /** Contents live on the ItemStack, so the inventory serialization already carries them. */
        ITEM_COMPONENTS("item components — carried by the inventory sync"),
        /** Dedicated handler reading the mod's API on the main thread. */
        DEDICATED("dedicated handler"),
        /** Contents live in a world SavedData keyed by a container UUID. */
        EXTERNAL_STORAGE("external container storage (backpack_data)"),
        /** Covered by the generic NeoForge attachment sync. */
        ATTACHMENTS("NeoForge attachments (generic)"),
        /** Covered by the generic persistent-data sync. */
        PERSISTENT_DATA("player persistent data (generic)"),
        /** PlayerSync steps aside so the mod's own death handling stays authoritative. */
        DEATH_INTEROP("death interop — PlayerSync yields to the mod");

        public final String label;
        Mechanism(String label) { this.label = label; }
    }

    public record Entry(String modId, String displayName, Mechanism mechanism,
                        String toggleName, BooleanSupplier enabled, String note) {
        public boolean loaded() {
            return ModList.get().isLoaded(modId);
        }
        public boolean active() {
            try {
                return enabled == null || enabled.getAsBoolean();
            } catch (Throwable t) {
                return true;
            }
        }
    }

    private static final List<Entry> ENTRIES = List.of(
            new Entry("curios", "Curios API", Mechanism.DEDICATED,
                    "sync_curios", () -> JdbcConfig.SYNC_CURIOS.get(),
                    "functional + cosmetic slots; slots are cleared before restore so stale .dat data cannot duplicate"),
            new Entry("accessories", "Accessories API", Mechanism.DEDICATED,
                    "sync_accessories", () -> JdbcConfig.SYNC_ACCESSORIES.get(),
                    "used by The Aether; a missing capability skips the write instead of wiping the record"),
            new Entry("cosmeticarmorreworked", "Cosmetic Armor Reworked", Mechanism.DEDICATED,
                    "sync_cosmetic_armor", () -> JdbcConfig.SYNC_COSMETIC_ARMOR.get(),
                    "4 cosmetic slots"),
            new Entry("sophisticatedbackpacks", "Sophisticated Backpacks", Mechanism.EXTERNAL_STORAGE,
                    "sync_backpacks", () -> JdbcConfig.SYNC_BACKPACKS.get(),
                    "inventory, armor, offhand, Curios slots and ender chest are scanned; empty containers are tombstoned"),
            new Entry("sophisticatedstorage", "Sophisticated Storage", Mechanism.EXTERNAL_STORAGE,
                    "sync_backpacks", () -> JdbcConfig.SYNC_BACKPACKS.get(),
                    "packed barrels / shulkers / chests carried as items"),
            new Entry("refinedstorage", "Refined Storage 2", Mechanism.EXTERNAL_STORAGE,
                    "sync_refined_storage", () -> JdbcConfig.SYNC_REFINED_STORAGE.get(),
                    "disks are encoded on the main thread and restored with a live dirty-listener"),
            new Entry("extradisks", "Extra Disks", Mechanism.EXTERNAL_STORAGE,
                    "sync_refined_storage", () -> JdbcConfig.SYNC_REFINED_STORAGE.get(),
                    "handled by the Refined Storage 2 disk path"),
            new Entry("ae2", "Applied Energistics 2", Mechanism.ITEM_COMPONENTS,
                    "sync_inventory", () -> JdbcConfig.SYNC_INVENTORY.get(),
                    "storage cells keep their contents in item components (ae2:storage_cell_inv); "
                            + "spatial cells and wireless links stay bound to the world that owns them"),
            new Entry("apotheosis", "Apotheosis", Mechanism.ITEM_COMPONENTS,
                    "sync_inventory", () -> JdbcConfig.SYNC_INVENTORY.get(),
                    "affixes are item components; world tier rides the attachment sync"),
            new Entry("tombstone", "Corail Tombstone", Mechanism.PERSISTENT_DATA,
                    "sync_persistent_data", () -> JdbcConfig.SYNC_PERSISTENT_DATA.get(),
                    "knowledge / alignment / perks / watcher knowledge live in the persistent-data tag; "
                            + "graves stay in the world where the player died"),
            new Entry("corpse", "Corpse", Mechanism.DEATH_INTEROP,
                    null, null,
                    "the corpse owns the items after a death; PlayerSync clears its item columns so nothing duplicates"),
            new Entry("revive_me", "Revive Me", Mechanism.DEATH_INTEROP,
                    null, null,
                    "a fallen player keeps their .dat inventory: the DB apply is skipped on join and the item columns are cleared on logout"),
            // 2.2.0: the attachment rows now name the toggle that actually governs them
            // (sync_attachments) and Ars Nouveau's real mod id (ars_nouveau, not arsnouveau).
            new Entry("ars_nouveau", "Ars Nouveau", Mechanism.ATTACHMENTS,
                    "sync_attachments", () -> JdbcConfig.SYNC_ATTACHMENTS.get(), "mana and glyph knowledge"),
            new Entry("irons_spellbooks", "Iron's Spells 'n Spellbooks", Mechanism.ATTACHMENTS,
                    "sync_attachments", () -> JdbcConfig.SYNC_ATTACHMENTS.get(), "mana and learned spells"),
            new Entry("pehkui", "Pehkui", Mechanism.ATTACHMENTS,
                    "sync_attachments", () -> JdbcConfig.SYNC_ATTACHMENTS.get(), "player scale"),
            new Entry("solonion", "Spice of Life: Onion", Mechanism.ATTACHMENTS,
                    "sync_attachments", () -> JdbcConfig.SYNC_ATTACHMENTS.get(), "food diversity history")
    );

    public static List<Entry> entries() {
        return ENTRIES;
    }

    /** Entries whose mod is present on this server. */
    public static List<Entry> loadedEntries() {
        List<Entry> out = new ArrayList<>();
        for (Entry e : ENTRIES) {
            if (e.loaded()) out.add(e);
        }
        return out;
    }

    /**
     * Logs one line per detected integration at server start, plus a warning for any mod
     * that is installed while its sync toggle is off — the single most common cause of
     * "PlayerSync is not syncing my X" reports.
     */
    public static void logStartupReport() {
        List<Entry> loaded = loadedEntries();
        if (loaded.isEmpty()) {
            PlayerSync.LOGGER.info("[compat] no optional integration detected — vanilla player data only");
            return;
        }
        PlayerSync.LOGGER.info("[compat] {} integration(s) detected:", loaded.size());
        for (Entry e : loaded) {
            boolean active = e.active();
            PlayerSync.LOGGER.info("[compat]   {} ({}) — {}{}",
                    e.displayName(), e.modId(), e.mechanism().label,
                    active ? "" : "  [DISABLED by " + e.toggleName() + "=false]");
            if (!active) {
                PlayerSync.LOGGER.warn("[compat] {} is installed but '{}' is false — its data will NOT be synchronized",
                        e.displayName(), e.toggleName());
                SyncLogger.modCompatSkip("-", e.modId(), "installed but " + e.toggleName() + "=false");
            }
        }
        // Sophisticated Storage without its Backpacks sibling still works, but the shared
        // toggle name is confusing enough to be worth calling out once.
        if (ModList.get().isLoaded("sophisticatedstorage") && !ModList.get().isLoaded("sophisticatedbackpacks")) {
            PlayerSync.LOGGER.info("[compat] Sophisticated Storage is governed by the 'sync_backpacks' toggle (shared storage table)");
        }
    }
}
