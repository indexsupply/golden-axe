use async_trait::async_trait;
use eyre::Result;
use shared::jrpc;
use tokio_postgres::Transaction;

/// A hook for deriving additional state from synced blocks and logs.
///
/// `on_block` runs inside the same transaction that writes blocks/txs/logs, so
/// any derived state a handler writes commits atomically with the canonical
/// data. `on_rollback` mirrors a reorg delete: it is called with the first
/// block number being removed so the handler can revert its derived state in
/// lockstep.
///
/// This is intentionally chain-data-agnostic — golden-axe knows nothing about
/// what a handler derives (e.g. app-specific projections). Handlers receive the same
/// decoded blocks/logs golden-axe persists.
#[async_trait]
pub trait BlockHandler: Send + Sync {
    async fn on_block(
        &self,
        tx: &Transaction<'_>,
        chain: u64,
        blocks: &[jrpc::Block],
        logs: &[jrpc::Log],
    ) -> Result<()>;

    async fn on_rollback(&self, tx: &Transaction<'_>, chain: u64, from_block: u64) -> Result<()>;
}
