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
use near_vm_runner::internal::VMKind;
use near_vm_runner::prepare::{prepare_contract, ContractModule};
use near_vm_runner::{precompile_contract_vm, run_vm, MockCompiledContractCache};
use nearcore::get_store_path;
use num_rational::Ratio;
use std::fmt::Write;
use std::fs::File;
use std::io::{BufReader, Write as OtherWrite};
use std::sync::Arc;
use walrus::{FunctionBuilder, Module};

const REPEATS: u64 = 5;

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
    let reader = BufReader::new(
        File::open("/host/nearcore/codes.json").expect("Could not open genesis config file."),
    );
    let entries: Vec<(Vec<u8>, String)> = serde_json::from_reader(reader).unwrap();

    // for params in vec![(9990, 50), (20010, 10), (50010, 1), (100010, 1)].iter().cloned() {
    for params in vec![(9990, 110), (9990, 1)].iter().cloned() {
        // for (code, account_id) in entries.iter() {
        //     let params = account_id;
        let (method_count, body_repeat) = params;
        // let code = many_functions_contract(method_count);
        let code = many_functions_contract_with_repeats(method_count, body_repeat);
        let fname = format!("/home/Aleksandr1/nearcore/test_{}_{}.wasm", method_count, body_repeat);
        eprintln!("{}", fname);
        let mut file = File::create(fname).unwrap();
        // Write a slice of bytes to the file
        file.write_all(&code).unwrap();
        let contract = ContractCode::new(code.clone(), None);
        let store = RuntimeConfigStore::new(None);
        let config = store.get_config(ProtocolVersion::MAX);
        let vm_config = &config.wasm_config;

        for _ in 0..REPEATS {
            let cache_store = Arc::new(MockCompiledContractCache::default());
            let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());

            let start = start_count(metric);
            let _ = precompile_contract_vm(vm_kind, &contract, &vm_config, cache).unwrap();
            // if method_count < 10000 {
            //     assert!(result.is_ok());
            // } else {
            //     assert!(result.is_err());
            // }
            let total_raw = end_count(metric, &start) as i128;

            match metric {
                GasMetric::ICount => {
                    println!(
                        "total cost for contract with params {:?}, len = {} | {} ops, {} gas",
                        params,
                        code.len(),
                        total_raw,
                        ratio_to_gas_signed(metric, Ratio::new(total_raw as i128, 1 as i128)),
                    )
                }
                GasMetric::Time => {
                    println!(
                        "total cost for contract with params {:?}, len = {} | {} ms, {} gas",
                        params,
                        code.len(),
                        (total_raw as f64) / (1_000_000 as f64),
                        ratio_to_gas_signed(metric, Ratio::new(total_raw as i128, 1 as i128)),
                    )
                }
            };
        }
    }
}

fn blow_up_code(code: &[u8]) -> Vec<u8> {
    let m = &mut Module::from_buffer(code).unwrap();
    for i in 0..4000 {
        if i % 1000 == 0 {
            println!("{}", i);
        }
        let mut hello_func = FunctionBuilder::new(&mut m.types, &[], &[]);
        hello_func.func_body().i32_const(1).drop();
        let hello_func = hello_func.finish(vec![], &mut m.funcs);
        m.exports.add(&format!("hello{}", i), hello_func);
    }
    m.emit_wasm()
}

struct EstimatedCode {
    id: String,
    code: Vec<u8>,
}

#[derive(Debug)]
struct Estimation {
    id: String,
    fns: u64,
    len: u64,
    result: f64,
}

pub fn get_functions_number(original_code: &[u8], config: &VMConfig) -> usize {
    ContractModule::init(original_code, config).unwrap().module.functions_space()
}

#[allow(dead_code)]
fn test_function_call_all_codes(metric: GasMetric, vm_kind: VMKind) {
    let mut estimated_codes: Vec<EstimatedCode> = Vec::new();

    /// prepare mainnet contracts
    let reader = BufReader::new(
        File::open("/host/nearcore/codes.json").expect("Could not open genesis config file."),
    );
    let entries: Vec<(Vec<u8>, String)> = serde_json::from_reader(reader).unwrap();
    for (code, account_id) in entries.iter() {
        let code = blow_up_code(code);
        estimated_codes
            .push(EstimatedCode { id: format!("from_mainnet_with_noop.{}", account_id), code });
    }

    /// prepare params
    for (method_count, body_repeat) in
        vec![(2, 1), (5, 1), (10, 1), (100, 1), (1000, 1), (9990, 1)].iter().cloned()
    {
        let code = many_functions_contract_with_repeats(method_count, body_repeat);
        estimated_codes
            .push(EstimatedCode { id: format!("many_fns_{}_{}", method_count, body_repeat), code });
    }

    // estimate
    let mut estimations: Vec<Estimation> = Vec::new();
    for estimated_code in estimated_codes.iter() {
        let contract = ContractCode::new(estimated_code.code.clone(), None);
        let store = RuntimeConfigStore::new(None);
        let config = store.get_config(ProtocolVersion::MAX);
        let vm_config = &config.wasm_config;

        let raw_result = compute_function_call_cost(metric, vm_kind, REPEATS, &contract);
        let result = match metric {
            GasMetric::ICount => (raw_result as f64) / REPEATS / (10i64.pow(12) as f64), // teragas
            GasMetric::Time => (raw_result as f64) / REPEATS / (1_000_000 as f64),       // ms
        };
        estimations.push(Estimation {
            id: estimated_code.id.clone(),
            fns: get_functions_number(&estimated_code.code, vm_config) as u64,
            len: estimated_code.code.len() as u64,
            result,
        })
    }

    // show
    for estimation in estimations.iter() {
        println!("{:?}", estimation);
    }
}

#[test]
fn test_function_call_all_codes_icount() {
    test_function_call_all_codes(GasMetric::ICount, VMKind::Wasmer2);
}

#[test]
fn test_function_call_all_codes_time() {
    test_function_call_all_codes(GasMetric::Time, VMKind::Wasmer2);
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
fn test_prepare_contract_time() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh \
    // cargo test --release --features no_cpu_compatibility_checks,required  \
    // --lib function_call::test_prepare_contract_icount -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    test_prepare_contract(GasMetric::Time, VMKind::Wasmer2);
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
    let runtime = vm_kind.runtime().expect("runtime has not been enabled");
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let protocol_version = ProtocolVersion::MAX;
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(protocol_version).as_ref();
    let vm_config = runtime_config.wasm_config.clone();
    let fees = runtime_config.transaction_costs.clone();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let promise_results = vec![];

    // Warmup.
    if repeats != 1 {
        let result = runtime.run(
            &contract,
            "hello0",
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            protocol_version,
            cache,
        );
        assert!(result.1.is_none());
    }
    // Run with gas metering.
    let start = start_count(gas_metric);
    for _ in 0..repeats {
        let result = runtime.run(
            &contract,
            "hello0",
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            protocol_version,
            cache,
        );
        assert!(result.1.is_none());
    }
    let total_raw = end_count(gas_metric, &start) as i128;

    println!("cost is {}", total_raw);

    total_raw as u64
}
