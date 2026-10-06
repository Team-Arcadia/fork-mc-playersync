package vip.fubuki.playersync.sync;

import net.minecraft.network.chat.Component;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.neoforged.bus.api.EventPriority;
import net.neoforged.bus.api.SubscribeEvent;
import net.neoforged.fml.common.EventBusSubscriber;
import net.neoforged.neoforge.common.util.TriState;
import net.neoforged.neoforge.event.entity.item.ItemTossEvent;
import net.neoforged.neoforge.event.entity.living.LivingIncomingDamageEvent;
import net.neoforged.neoforge.event.entity.player.AttackEntityEvent;
import net.neoforged.neoforge.event.entity.player.ItemEntityPickupEvent;
import net.neoforged.neoforge.event.entity.player.PlayerContainerEvent;
import net.neoforged.neoforge.event.entity.player.PlayerInteractEvent;
import net.neoforged.neoforge.event.entity.player.PlayerXpEvent;
import net.neoforged.neoforge.event.level.BlockEvent;
import vip.fubuki.playersync.PlayerSync;

/**
 * Freezes a player's interactions with the world while their restore is pending.
 *
 * <p>Between login and the restore, the player stands in the world holding the stale
 * inventory of their local {@code .dat} file — for under a second normally, up to a minute
 * while the previous server finishes its save. Whatever they threw, stored or dropped in
 * that window stayed in the world, and the restore then handed them the database copy of
 * the same items: duplication. Whatever they picked up was erased by the clear-before-restore
 * instead. Until the restore completes (see {@link VanillaSync#syncNotCompletedPlayer}) the
 * player can therefore not toss, pick up, use, place, break, attack, open a container or be
 * hurt. Everything resumes the moment the restore lands.
 *
 * @author vyrriox
 */
@EventBusSubscriber(modid = PlayerSync.MODID)
public final class SyncPendingGuard {

    private SyncPendingGuard() {}

    private static boolean isPending(Entity entity) {
        return entity instanceof ServerPlayer player
                && VanillaSync.syncNotCompletedPlayer.contains(player.getUUID().toString());
    }

    private static void notifyPending(Player player) {
        player.displayClientMessage(Component.translatableWithFallback("playersync.sync_pending",
                "Loading your data, please wait..."), true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onToss(ItemTossEvent event) {
        Player player = event.getPlayer();
        if (!isPending(player)) return;
        // The stack has already left the inventory when this fires; canceling only stops
        // the item entity from spawning, so hand the stack back.
        ItemStack stack = event.getEntity().getItem();
        event.setCanceled(true);
        if (!stack.isEmpty()) player.getInventory().placeItemBackInInventory(stack);
        notifyPending(player);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onPickup(ItemEntityPickupEvent.Pre event) {
        if (isPending(event.getPlayer())) event.setCanPickup(TriState.FALSE);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onXpPickup(PlayerXpEvent.PickupXp event) {
        if (isPending(event.getEntity())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onRightClickBlock(PlayerInteractEvent.RightClickBlock event) {
        if (isPending(event.getEntity())) {
            event.setCanceled(true);
            notifyPending(event.getEntity());
        }
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onRightClickItem(PlayerInteractEvent.RightClickItem event) {
        if (isPending(event.getEntity())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onLeftClickBlock(PlayerInteractEvent.LeftClickBlock event) {
        if (isPending(event.getEntity())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onEntityInteract(PlayerInteractEvent.EntityInteract event) {
        if (isPending(event.getEntity())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onEntityInteractSpecific(PlayerInteractEvent.EntityInteractSpecific event) {
        if (isPending(event.getEntity())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onAttack(AttackEntityEvent event) {
        if (isPending(event.getEntity())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onBreak(BlockEvent.BreakEvent event) {
        if (isPending(event.getPlayer())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onPlace(BlockEvent.EntityPlaceEvent event) {
        if (isPending(event.getEntity())) event.setCanceled(true);
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onContainerOpen(PlayerContainerEvent.Open event) {
        // Not cancelable: close it straight away. Covers keybind-opened menus too
        // (backpacks, Curios, Accessories), which never go through an interact event.
        if (event.getEntity() instanceof ServerPlayer player && isPending(player)
                && player.containerMenu != player.inventoryMenu) {
            player.closeContainer();
            notifyPending(player);
        }
    }

    @SubscribeEvent(priority = EventPriority.HIGHEST)
    public static void onIncomingDamage(LivingIncomingDamageEvent event) {
        // A death in this window would drop the stale inventory into a corpse right before
        // the database copy is restored.
        if (isPending(event.getEntity())) event.setCanceled(true);
    }
}
