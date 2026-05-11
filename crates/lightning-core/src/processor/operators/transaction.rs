use crate::planner::binder::BoundTransactionAction;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use std::collections::HashMap;

#[derive(Clone)]
pub struct PhysicalTransaction {
    action: BoundTransactionAction,
    executed: bool,
}

impl PhysicalTransaction {
    pub fn new(action: BoundTransactionAction) -> Self {
        Self {
            action,
            executed: false,
        }
    }
}

impl PhysicalOperator for PhysicalTransaction {
    fn get_next(
        &mut self,
        _database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        // Transaction actions are usually handled by the higher-level execution context (e.g. Session/Client)
        // that manages the lifecycle of the transaction object.
        // For the purpose of the physical operator, we can return success but the actual
        // commit/rollback logic must be integrated with the system's TransactionManager
        // at the boundaries of query execution.

        // In this implementation, we simply signify the action.
        // The calling code (e.g. in Database::query) will see this operator and act accordingly.

        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(self.clone())
    }
}
