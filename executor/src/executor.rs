// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};
use anyhow::Result;
use starcoin_types::transaction::{SignedUserTransaction, Transaction, TransactionOutput};
use starcoin_vm_types::identifier::Identifier;
use starcoin_vm_types::language_storage::{ModuleId, TypeTag};
use starcoin_vm_types::{state_view::StateView, vm_status::VMStatus};
use vm_runtime::metrics::VMMetrics;
use vm_runtime::starcoin_vm::StarcoinVM;

pub fn execute_transactions(
    chain_state: &dyn StateView,
    txns: Vec<Transaction>,
    metrics: Option<VMMetrics>,
    vm: &Arc<Mutex<StarcoinVM>>,
) -> Result<Vec<TransactionOutput>> {
    do_execute_block_transactions(chain_state, txns, None, metrics, vm)
}

/// Execute a block transactions with gas_limit,
/// if gas is used up when executing some txn, only return the outputs of previous succeed txns.
pub fn execute_block_transactions(
    chain_state: &dyn StateView,
    txns: Vec<Transaction>,
    block_gas_limit: u64,
    metrics: Option<VMMetrics>,
    vm: &Arc<Mutex<StarcoinVM>>
) -> Result<Vec<TransactionOutput>> {
    do_execute_block_transactions(chain_state, txns, Some(block_gas_limit), metrics, vm)
}

fn do_execute_block_transactions(
    chain_state: &dyn StateView,
    txns: Vec<Transaction>,
    block_gas_limit: Option<u64>,
    metrics: Option<VMMetrics>,
    vm: &Arc<Mutex<StarcoinVM>>,
) -> Result<Vec<TransactionOutput>> {
   // let mut vm = StarcoinVM::new(metrics);
    let mut vm = vm.lock().unwrap();
    vm.add_metrics(metrics);
    let result = vm
        .execute_block_transactions(chain_state, txns, block_gas_limit)?
        .into_iter()
        .map(|(_, output)| {
            debug! {"{:?}", output};
            output
        })
        .collect();
    Ok(result)
}

pub fn validate_transaction(
    chain_state: &dyn StateView,
    txn: SignedUserTransaction,
    metrics: Option<VMMetrics>,
    vm: &Arc<Mutex<StarcoinVM>>,
) -> Option<VMStatus> {
    //let mut vm = StarcoinVM::new(metrics);
    let mut vm = vm.lock().unwrap();
    vm.add_metrics(metrics);
    vm.verify_transaction(chain_state, txn)
}

pub fn execute_readonly_function(
    chain_state: &dyn StateView,
    module: &ModuleId,
    function_name: &Identifier,
    type_params: Vec<TypeTag>,
    args: Vec<Vec<u8>>,
    metrics: Option<VMMetrics>,
) -> Result<Vec<Vec<u8>>, VMStatus> {
    let mut vm = StarcoinVM::new(metrics);
    vm.execute_readonly_function(chain_state, module, function_name, type_params, args)
}
