use crate::cases::ratio_to_gas_signed;
use crate::testbed_runners::{end_count, start_count, GasMetric};
use crate::vm_estimator::{create_context, least_squares_method, measure_contract};
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::{CompiledContractCache, ProtocolVersion};
use near_store::{create_store, StoreCompiledContractCache};
use near_test_contracts::{many_functions_contract, many_functions_contract_with_repeats};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_runner::prepare::prepare_contract;
use near_vm_runner::{precompile_contract_vm, run_vm, MockCompiledContractCache, VMKind};
use nearcore::get_store_path;
use num_rational::Ratio;
use std::fmt::Write;
use std::sync::Arc;

const REPEATS: u64 = 1;

#[allow(dead_code)]
fn test_function_call(metric: GasMetric, vm_kind: VMKind) {
    let mut xs = vec![];
    let mut ys = vec![];
    for method_count in vec![5, 20, 30, 50, 100, 200, 1000] {
        let contract = make_many_methods_contract(method_count);
        let cost = compute_function_call_cost(metric, vm_kind, REPEATS, &contract);
        println!("{:?} {:?} {} {}", vm_kind, metric, method_count, cost / REPEATS);
        xs.push(contract.code().len() as u64);
        ys.push(cost / REPEATS);
    }

    // Regression analysis only makes sense for additive metrics.
    if metric == GasMetric::Time {
        return;
    }

    let (cost_base, cost_byte, _) = least_squares_method(&xs, &ys);

    println!(
        "{:?} {:?} function call base {} gas, per byte {} gas",
        vm_kind,
        metric,
        ratio_to_gas_signed(metric, cost_base),
        ratio_to_gas_signed(metric, cost_byte),
    );
}

#[allow(dead_code)]
fn test_prepare_contract(metric: GasMetric, vm_kind: VMKind) {
    for (method_count, body_repeat) in
        // vec![(2, 1), (5, 1), (10, 1), (100, 1), (1000, 1), (10000, 1)].iter().cloned()
        vec![(9990, 1), (9990, 10), (9990, 100), (20010, 10), (50010, 1), (100010, 1)]
                .iter()
                .cloned()
    // vec![(0, 0)].iter().cloned()
    {
        // let code = many_functions_contract(method_count);
        let code = many_functions_contract_with_repeats(method_count, body_repeat);
        let contract = ContractCode::new(code.clone(), None);
        let store = RuntimeConfigStore::new(None);
        let config = store.get_config(ProtocolVersion::MAX);
        let vm_config = &config.wasm_config;
        let cache_store = Arc::new(MockCompiledContractCache::default());
        let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());

        let start = start_count(metric);
        for i in 0..REPEATS {
            print!("{} ", i);
            // let result = prepare_contract(&code, vm_config);
            // measure_contract(vm_kind, metric, &contract, cache);
            let _ = precompile_contract_vm(vm_kind, &contract, &vm_config, cache).unwrap();
            // if method_count < 10000 {
            //     assert!(result.is_ok());
            // } else {
            //     assert!(result.is_err());
            // }
        }
        let total_raw = end_count(metric, &start) as i128;

        println!(
            "total cost = {}, average gas cost = {}, len = {}",
            total_raw,
            ratio_to_gas_signed(metric, Ratio::new(total_raw as i128, REPEATS as i128)),
            code.len()
        );
    }
}

#[test]
fn test_prepare_contract_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh \
    // cargo test --release --features no_cpu_compatibility_checks,required  \
    // --lib function_call::test_prepare_contract_icount -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    test_prepare_contract(GasMetric::ICount, VMKind::Wasmer2);
}

#[test]
fn test_function_call_time() {
    // Run with
    // cargo test --release --lib function_call::test_function_call_time
    //    --features required  -- --exact --nocapture
    test_function_call(GasMetric::Time, VMKind::Wasmer0);
    test_function_call(GasMetric::Time, VMKind::Wasmer2);
    test_function_call(GasMetric::Time, VMKind::Wasmtime);
}

#[test]
fn test_function_call_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh \
    // cargo test --release --features no_cpu_compatibility_checks,required  \
    // --lib function_call::test_function_call_icount -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    test_function_call(GasMetric::ICount, VMKind::Wasmer0);
    test_function_call(GasMetric::ICount, VMKind::Wasmer2);
    test_function_call(GasMetric::ICount, VMKind::Wasmtime);
}

fn make_many_methods_contract(method_count: i32) -> ContractCode {
    let mut methods = String::new();
    for i in 0..method_count {
        write!(
            &mut methods,
            "
            (export \"hello{}\" (func {i}))
              (func (;{i};)
                i32.const {i}
                drop
                return
              )
            ",
            i = i
        )
        .unwrap();
    }

    let code = format!(
        "
        (module
            {}
            )",
        methods
    );
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

pub fn compute_function_call_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    repeats: u64,
    contract: &ContractCode,
) -> u64 {
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let vm_config = VMConfig::default();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];

    // Warmup.
    if repeats != 1 {
        let result = run_vm(
            &contract,
            "hello0",
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            vm_kind,
            ProtocolVersion::MAX,
            cache,
        );
        assert!(result.1.is_none());
    }
    // Run with gas metering.
    let start = start_count(gas_metric);
    for _ in 0..repeats {
        let result = run_vm(
            &contract,
            "hello0",
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            vm_kind,
            ProtocolVersion::MAX,
            cache,
        );
        assert!(result.1.is_none());
    }
    let total_raw = end_count(gas_metric, &start) as i128;

    println!("cost is {}", total_raw);

    total_raw as u64
}
